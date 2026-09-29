# 21 — Benchmark results in states: dimensioning, independence, modes and intervals

**Status:** research input for how `mantle bench chunk` (`crates/mantle/src/bench.rs`) and calibration (`crates/disk/src/calibrate.rs`) run and report a measured point. This is not a decision record; decisions it supports belong in `docs/design/`.
**Compiled:** 2026-09-29.
**Scope:** Note 11 (§13.3, §16) runs each point in one-second rounds and stops when the 95% Student-t interval of the mean throughput is within ±5% or six rounds have run, after GBE07's JavaStats. On this machine reads spread ±7–107%: every thread's reads stall together for about 6 ms in bursts at random moments, and points measured after a large fill ran at half rate ([rounds measurement](../measurements/2026-09-29-chunk-store-rounds.md)). Note 11 §16.3 concluded, without a cited method, that bimodal results should be reported per state, and that step and round counts should come from a dimensioning run. This note collects cited methods for five things: (1) the dimensioning experiment; (2) checking that rounds are independent; (3) deciding, soundly at tens of rounds, whether a point's rounds form more than one state, and splitting them; (4) the interval for each state and for a unimodal point; (5) the rounds these need to have power. It also records what the storage literature says about device states, and closes with a method for mantle (§9).

---

## 0. How to read this note

**Citation tags.** `[KEY §section, p. N]` gives the printed page of the copy read. "PDF p. N" means the copy has no printed page numbers, or prints draft numbers; N is then the page index in the PDF. Earlier notes are cited as "note 11 §x".

**Quotes.** Quotes in "double quotes" are verbatim. Ligatures are normalized, hyphenation at line breaks is rejoined, and "..." marks an elision. **[image]** marks quotes and table cells read from page images: HH85, SIL81 and CHR65 are scans without a usable text layer, and the text layers of KJ13 (Eq. 1–4), HB15 (the rank formula) and PSJ13 (the BC formula) garble the equations, which were read from the rendered pages.

**Evidence labels.**
- *(no label)*: stated in the cited peer-reviewed source and checked against its text.
- **primary**: a standard, a government handbook, a textbook, or a vendor or software reference, read directly.
- **NON-PEER-REVIEWED**: preprints, posters, technical reports, software documentation.
- **DERIVED**: arithmetic or reasoning done in this note from stated facts. The sources do not state it.
- **UNVERIFIED**: not confirmed against a primary text.
- *abstract only*: only the authors' abstract was read.

**Method.**
1. All sources were fetched on 2026-09-29 (UTC) from the URLs in the table below. No contact identifier was sent with any request.
2. Text was extracted with `pdftotext`. Scans and equations were rendered with `pdftoppm` (110–220 dpi) and read from the images.
3. Every quote taken from a text layer was checked by machine against the source's text, with both sides reduced to lowercase letters and digits, and its page was located the same way. Quotes marked **[image]** were checked against the rendered page.
4. The dip test's simulated table (DIPTEST) was read from the package's serialized data file (`inst/extraData/qDiptab.rds`) with a minimal reader of R's serialization format; its values agree with HH85 Table 1 (§5.2).
5. Arithmetic in this note (binomial coverages, run-count distributions, interpolation of critical values) was done exactly or in double precision in a few lines of Python. No simulation was run: a timing-sensitive benchmark was running on this machine.
6. No secondary summary (blogs, lecture notes, other authors' descriptions) is used as evidence. Where a paper describes another that was not read, the description is attributed to the paper that makes it.

---

## Sources

| Key | Citation | Label | How obtained |
|---|---|---|---|
| **KJ13** | T. Kalibera, R. Jones. "Rigorous Benchmarking in Reasonable Time." ISMM '13, pp. 63–74. doi:10.1145/2464157.2464160. Same key as note 11. | peer-reviewed | Kent Academic Repository full text (record 33611, "Document Version Updated Version"); kar.kent.ac.uk returned 504, so the KAR file was read from a course mirror, http://petertsehsun.github.io/soen691/current/papers/reasonable_benchmarking.pdf, identified by its KAR cover. PDF p. 1 is the cover. |
| **KJ12** | T. Kalibera, R. Jones. "Quantifying Performance Changes with Effect Size Confidence Intervals." Technical Report 4-12, University of Kent, June 2012, 55 pp. | **NON-PEER-REVIEWED** (technical report) | https://www.cs.kent.ac.uk/pubs/2012/3233/content.pdf; record page https://www.cs.kent.ac.uk/pubs/2012/3233/. arXiv:2007.10899 (also 55 pp.) was fetched but not compared. |
| **BBKMT17** | E. Barrett, C. F. Bolz-Tereick, R. Killick, S. Mount, L. Tratt. "Virtual Machine Warmup Blows Hot and Cold." PACMPL 1(OOPSLA), 2017, pp. 1–27 (Crossref). doi:10.1145/3133876 | peer-reviewed venue; the text read is arXiv:1602.00602v6 (6 Oct 2017), 40 pp., labelled "Draft, Vol. 0, No. 0, Article 0" | arXiv PDF. The ACM PDF returned a bot-check page. The draft's page labels "0:N" equal PDF page N. |
| **KFE12** | R. Killick, P. Fearnhead, I. A. Eckley. "Optimal Detection of Changepoints With a Linear Computational Cost." *JASA* 107(500): 1590–1598, 2012. doi:10.1080/01621459.2012.737745 | peer-reviewed venue; the text read is arXiv:1101.1438v3 (9 Oct 2012) | arXiv PDF, 25 pp. |
| **HB15** | T. Hoefler, R. Belli. "Scientific Benchmarking of Parallel Computing Systems: Twelve ways to tell the masses when reporting performance results." SC '15, pp. 1–12. doi:10.1145/2807591.2807644 | peer-reviewed; the copy read is the authors' revision | https://htor.inf.ethz.ch/publications/img/hoefler-scientific-benchmarking.pdf, which carries the SC '15 identifiers, "© 2017 Copyright held by the owner/author(s)" and a footnote "Update 2017/01/25". Not compared with the ACM text. |
| **LB10** | J.-Y. Le Boudec. *Performance Evaluation of Computer and Communication Systems.* EPFL Press, 2010. | **primary** (textbook) | https://leboudec.github.io/perfeval/book/perf.pdf, "Version 2.3.2 of July 26, 2026 / Essentially identical to publisher's version, except for formatting / With bug fixes". Pages are that version's printed pages. |
| **MDJ18** | A. Maricq, D. Duplyakin, I. Jimenez, C. Maltzahn, R. Stutsman, R. Ricci. "Taming Performance Variability." OSDI '18, pp. 409–425. | peer-reviewed | https://www.usenix.org/system/files/osdi18-maricq.pdf |
| **HH85** | J. A. Hartigan, P. M. Hartigan. "The Dip Test of Unimodality." *Ann. Statist.* 13(1): 70–84, 1985. doi:10.1214/aos/1176346577 | peer-reviewed | Project Euclid download of the JSTOR scan, 15 pp., no text layer; read from page images **[image]**. |
| **AS217** | P. M. Hartigan. "Algorithm AS 217: Computation of the Dip Statistic to Test for Unimodality." *Applied Statistics* 34(3): 320–325, 1985. doi:10.2307/2347485 | **UNVERIFIED** | not read (paywalled). Known here only through DIPTEST's references. |
| **DIPTEST** | M. Maechler. R package `diptest` 0.77-2, 2025-08-19, "Hartigan's Dip Test Statistic for Unimodality - Corrected", GPL (≥ 2). | **NON-PEER-REVIEWED** (software and its simulated table) | CRAN manual https://cran.r-project.org/web/packages/diptest/diptest.pdf; source https://cran.r-project.org/src/contrib/diptest_0.77-2.tar.gz (`R/dipTest.R`, `inst/extraData/qDiptab.rds`). |
| **SIL81** | B. W. Silverman. "Using Kernel Density Estimates to investigate Multimodality." *JRSS B* 43(1): 97–99, 1981. | peer-reviewed | JSTOR scan on a course page, https://sites.stat.washington.edu/jaw/COURSES/580s/581/HO/Silverman.jrss.81.pdf; read from page images **[image]**. |
| **HY01** | P. Hall, M. York. "On the Calibration of Silverman's Test for Multimodality." *Statistica Sinica* 11: 515–536, 2001. | peer-reviewed | journal PDF, https://www3.stat.sinica.edu.tw/statistica/oldpdf/A11n28.pdf |
| **CH98** | M.-Y. Cheng, P. Hall. "Calibrating the excess mass and dip tests of modality." *JRSS B* 60(3): 579–589, 1998. doi:10.1111/1467-9868.00141 | peer-reviewed | journal-layout copy on the author's site, https://www.math.ntu.edu.tw/~cheng/edit_cheng/calibrating.pdf |
| **AAC19** | J. Ameijeiras-Alonso, R. M. Crujeiras, A. Rodríguez-Casal. "Mode testing, critical bandwidth and excess mass." *TEST* 28(3): 900–919, 2019. doi:10.1007/s11749-018-0611-5 | peer-reviewed venue; the text read is arXiv:1609.05188v3 (10 Apr 2019) | arXiv PDF; PDF pages. |
| **ABZ94** | K. M. Ashman, C. M. Bird, S. E. Zepf. "Detecting bimodality in astronomical datasets." *AJ* 108: 2348–2361, 1994. doi:10.1086/117248 | peer-reviewed | published version: ADS scan with text layer, https://articles.adsabs.harvard.edu/pdf/1994AJ....108.2348A (printed pages); wording checked against arXiv:astro-ph/9408030v1. |
| **PSJ13** | R. Pfister, K. A. Schwarz, M. Janczyk, R. Dale, J. B. Freeman. "Good things peak in pairs: a note on the bimodality coefficient." *Front. Psychol.* 4: 700, 2013. doi:10.3389/fpsyg.2013.00700 | peer-reviewed (general commentary, "Edited by: Holmes Finch") | published PDF, http://kaschwarz.net/publications/articles/Pfister_et_al_2013_Frontiers_Psychol.pdf |
| **FD13** | J. B. Freeman, R. Dale. "Assessing bimodality to detect the presence of a dual cognitive process." *Behav. Res. Methods* 45(1): 83–97, 2013. doi:10.3758/s13428-012-0225-x | peer-reviewed | publisher's online-first PDF ("Behav Res", © Psychonomic Society 2012, no journal page numbers) from the authors' lab site, https://scns-lab.squarespace.com/s/assessing-bimodality-to-detect-the-presence-of-a-dual-cognitive-process.pdf; PDF pages. |
| **FIS58** | W. D. Fisher. "On Grouping for Maximum Homogeneity." *JASA* 53(284): 789–798, 1958. doi:10.1080/01621459.1958.10501479 | **UNVERIFIED** | not accessible; Crossref and Semantic Scholar carry no abstract. |
| **EH69** | L. Engelman, J. A. Hartigan. "Percentage points of a test for clusters." *JASA* 64, 1969. | **UNVERIFIED** | not read; described only by HH85 p. 70. |
| **CR16** | J. Chen, J. Revels. "Robust benchmarking in noisy environments." arXiv:1608.04295v1, 15 Aug 2016, 7 pp. | **NON-PEER-REVIEWED** | arXiv PDF. The arXiv comment reads "Proceedings of the 20th Annual IEEE High Performance Extreme Computing Conference, 2016"; the HPEC 2016 program (https://ieee-hpec.org/2016/techprog2016/sept15.htm) lists it under "Lunch; View Posters and Demos"; Crossref has no DOI for it; the dblp record found is the CoRR one. |
| **CHR65** | Y. S. Chow, H. Robbins. "On the Asymptotic Theory of Fixed-Width Sequential Confidence Intervals for the Mean." *Ann. Math. Statist.* 36(2): 457–462, 1965. doi:10.1214/aoms/1177700156 | peer-reviewed | Project Euclid download of the JSTOR scan; page images **[image]**. |
| **NIST** | NIST/SEMATECH e-Handbook of Statistical Methods, §1.3.3.1 "Autocorrelation Plot", §1.3.3.15 "Lag Plot", §1.3.5.12 "Autocorrelation", §1.3.5.13 "Runs Test for Detecting Non-randomness". | **primary** (government handbook) | https://www.itl.nist.gov/div898/handbook/eda/section3/ (`autocopl.htm`, `lagplot.htm`, `eda35c.htm`, `eda35d.htm`) |
| **SNIA-PTS** | SNIA. *Solid State Storage (SSS) Performance Test Specification (PTS)*, Version 2.0.2. Same key as note 11. | **primary** (industry standard; NON-PEER-REVIEWED) | https://snia.org/sites/default/files/2025-02/SNIA-SSS-PTS-2.0.2.pdf |
| **CKZ09** | F. Chen, D. A. Koufaty, X. Zhang. "Understanding Intrinsic Characteristics and System Implications of Flash Memory based Solid State Drives." SIGMETRICS/Performance '09, pp. 181–192. doi:10.1145/1555349.1555371 | peer-reviewed | author PDF, https://homes.luddy.indiana.edu/fchen25/publications/pdf/sigmetrics09.pdf; PDF pages. |
| **KIM11** | Y. Kim, S. Oral, G. M. Shipman, J. Lee, D. A. Dillow, F. Wang. "Harmonia: A Globally Coordinated Garbage Collector for Arrays of Solid-state Drives." IEEE MSST 2011. | peer-reviewed | author copy, https://users.nccs.gov/~fwang2/papers/msst11.pdf; PDF pages. |
| **JK13** | M. Jung, M. Kandemir. "Revisiting widely held SSD expectations and rethinking system-level implications." SIGMETRICS '13, pp. 203–216. doi:10.1145/2465529.2465548 | peer-reviewed; *abstract only* | Penn State Pure record; the ACM PDF returned a bot-check page. |
| **DES12** | P. Desnoyers. "Analytic Modeling of SSD Write Performance." SYSTOR '12. Same key as note 11. | peer-reviewed | author PDF, https://www.ccs.neu.edu/~pjd/papers/pjd-systor12.pdf; PDF pages. |
| **GBE07** | as note 11 (JavaStats). | peer-reviewed | not re-read; cited through note 11. |

---

## 1. Findings that change decisions

1. **The round and pass counts come from KJ13's dimensioning formulas, and the interval comes from the top level only (§2).** KJ13 estimate each level's variance contribution `T_i²` from a dimensioning run and set `r_i = ⌈√((c_{i+1}/c_i)·(T_i²/T_{i+1}²))⌉` for every level but the top, which is repeated until its interval is narrow enough. The interval uses the top level's means only. A single process of `mantle bench chunk` cannot give an interval that covers process-to-process variation.
2. **Rounds must be shown independent before any interval or modality test applies, and six rounds cannot show it (§2.1, §4).** KJ13: "If measurements are not i.i.d., the variance and confidence interval estimates will be biased." At n = 6 the lag-1 autocorrelation bound is ±0.80, and an exact runs test has no 5% rejection region below 10 rounds (DERIVED).
3. **Mantle's data carry two different kinds of state (§9.1).** States in time (the drive's recent history) break independence and are found by changepoint segmentation, as BBKMT17 do. States in distribution (stall bursts caught or not by a round) can occur among independent rounds and are found by a test of unimodality.
4. **The dip test is the only test found that is distribution-free and has published null percentage points for n = 4–30 (§5).** KMM's likelihood-ratio test "does not provide a reliable method for detecting bimodality" below N = 50 [ABZ94]; the bimodality coefficient has no null distribution [PSJ13]; an AIC comparison of one- and two-component Gaussian fits judged 81% of unimodal simulations bimodal [FD13]; Silverman's test is conservative [HY01]. The dip's critical values at α = 0.05 and 0.01 for n = 4–100 are in §9.4.
5. **At tens of rounds the dip test can see only a large minority state (§5.3, DERIVED).** For two tight states with minority share p, the dip is about p/2, so rejection needs p > 2·D_crit(n): about 28% of the rounds at n = 10, 21% at n = 20, 18% at n = 30, 14% at n = 50.
6. **A separation index computed from a hard split is not a test (§5.8, DERIVED).** Splitting a single normal distribution at its mean gives two halves whose separation Δμ/σ is 2.65, above the "≥ 2" condition, which ABZ94 state for the parameters of an equal-weight two-Gaussian parent, not for a split sample.
7. **Report the median of each state with an exact order-statistic interval (§6).** Throughput rounds are rarely normal [MDJ18: 710 of 713 configurations; HB15 Rule 6]. LB10's binomial interval is exact for i.i.d. data and needs 6 rounds at 95%; HB15's and MDJ18's normal-approximation rank formula is wide or out of range at small n (DERIVED).
8. **The minimum (for throughput, the maximum) is not a safe estimator for storage (§7, §8).** CR16's argument needs every disturbance to add time; storage has transient elevated states ("a brief period of elevated performance" [SNIA-PTS p. 13]). CR16 is also not peer-reviewed.
9. **Note 11 §16.3's `n ≥ (t·CV/ε)²` is HB15's formula for normally distributed data (§6.4).** For data of unknown distribution HB15 recommend recomputing the nonparametric interval as measurements accrue.

---

## 2. Kalibera & Jones: levels, dimensioning and the interval (KJ13, KJ12)

### 2.1 Initialised and independent states

"We identify an initialised state and an independent state of benchmark execution. We call a state independent if the execution times of the benchmark iterations are (statistically) independent and identically distributed. A state is initialised — the lower bar — when iterations are no longer subject to obvious and significant initialisation overhead." [KJ13 §6, PDF p. 5]

"Note that it does not makes sense to repeat measurements unless the system has reached an independent state. If measurements are not i.i.d., the variance and confidence interval estimates will be biased. The first question to ask is, therefore, does a benchmark reach an independent state and, if so, after how many iterations?" [KJ13 §6, PDF p. 5]

Levels: "We refer to points of potential repetition as levels of the experiment" and "At the very least, repetition must be done at the highest level that has random variation to avoid bias, but sometimes repeating at lower levels can reduce experimentation time without sacrificing precision." [KJ13 §3, PDF p. 4]

### 2.2 How they check independence

"In the first step, we inspect run-sequence plots (of iteration duration against iteration number), looking for an iteration after which the data seem stable, that is with no regularities or patterns." Then "the script displays three plots for each benchmark execution: an autocorrelation function (ACF) plot, a lag plot, and a run-sequence plot with the consecutive measurements connected (details below). Each of these plots can reveal dependencies, and each is offered in two versions — one for the measured data and one for that data randomly reordered. The two versions make the interpretation easier: the experimenter simply looks for a systematic, significant difference between the real and the randomised plots." [KJ13 §6.1, PDF p. 5]

"We check lag plots for lags 1–4, using both iteration order and randomly reordered data". For the ACF, "independent data should have correlations mostly small in absolute value and in the range shown between the horizontal dotted lines (which bound the values expected from random noise) ... Any systematic structure in the correlations, even if small, is an additional indication of a dependency." [KJ13 §6.1, PDF p. 5]

Their study ran "three executions of each benchmark with 300 iterations per execution (note that we do not expect researchers to run this many iterations)" [KJ13 §6.1, PDF p. 5], and recommends: "Use this manual procedure just once to find how many iterations each benchmark, VM and platform combination requires to reach an independent state." [KJ13 §6.1, PDF p. 6]

On automation: "Automated on-line heuristics attempt to take a decision after a few iterations, as they are designed for real runs. This renders them less reliable than our once per benchmark/JVM/platform manual method where we look at 300 iterations." [KJ13 §6.3, PDF p. 7] "We believe that currently proposed or implemented heuristics have proved insufficient to detect independence accurately. ... Accurate and robust automation of this inspection is an open problem." [KJ13 §12, PDF p. 11]

### 2.3 Iterations that are not independent

"Many benchmarks do not reach an independent state in reasonable time (Table 2). So how should we run these benchmarks? Most have strong auto-dependencies: a gradual drift in times, trends (gradual increase and decrease), state changes (abrupt change in results after some number of iterations), systematic transitions between durations (e.g. odd-numbered iterations show one time and even-numbered ones another), and so on. By choosing which iterations to take, we influence the result significantly (by tens of percent)." [KJ13 §6.2, PDF p. 6]

"RECOMMENDATION: If a benchmark does not reach an independent state in a reasonable time, take the same iteration from each run." [KJ13 §6.2, PDF p. 6]

On averaging dependent iterations: "Aside: some researchers might repeat statistically dependent iterations and then include, say, their average in further summary [9]. This approach is not incorrect if the results are correctly interpreted, but the risk of misinterpretation is high. It redefines what is measured." ... "This approach always requires repetition at a higher level to avoid bias and to form the confidence interval." [KJ13 §6, PDF p. 5]

### 2.4 The dimensioning experiment

The initial (dimensioning) experiment: "Choose the repetition counts (exclusive of any warm-up iterations needed), r1, . . . , rn, at each of these levels to be some arbitrary yet sufficient value; 20 may be a good choice but use 30 if possible. If there are many levels for the initial experiment, reduce experimental time by using fewer repetitions (say, 10) at lower levels if you must. It makes sense only to include a level at the top of the hierarchy (n) where you know some repetition is needed." "Including other levels (n − 1 and below) is purely for optimisation of the experimental time, as repetition there is never needed for correctness." Partial experiments are allowed, "though always including the highest level", and "If the optimal repetition count at any level in these partial experiments ends up being 1, it is best not to repeat at that level in the real experiment." [KJ13 §9.1, PDF p. 9]

Costs: "gather the costs of repetition at each level, c1 , . . . , cn−1 , i.e. the time added exclusively by that level. The dimensioning process assumes that these costs do not change much between experiments, which follows our experience." [KJ13 §9.1, PDF p. 9]

The estimators and the optimum [KJ13 §9.2–9.3, Eq. 1–3, PDF pp. 9–10, **[image]**]. Levels run from 1 (lowest, the measured operation) to n (highest); `Y_{j_n…j_1}` are the measurements; a bullet marks an index averaged over.

```
(1)  S_i² = 1/(∏_{k=i+1..n} r_k) · 1/(r_i − 1) · Σ_{j_n=1..r_n} ··· Σ_{j_i=1..r_i} ( Ȳ_{j_n…j_i •…•} − Ȳ_{j_n…j_{i+1} •…•} )²
          (the first mean has i−1 bullets, the second i; S_i² is "the biased estimator of the variance at each level i, 1 ≤ i ≤ n")

(2)  T_1² = S_1²,      ∀i. 1 < i ≤ n:  T_i² = S_i² − S_{i−1}² / r_{i−1}

(3)  ∀i. 1 ≤ i < n:    r_i = ⌈ √( (c_{i+1}/c_i) · (T_i² / T_{i+1}²) ) ⌉
```

"If T_i² ≤ 0 (or at least very small — note T_i² denotes an estimator, not some value squared), then this level of the experiment induces little variation so repetitions at this level can be removed from the real experiment." [KJ13 §9.2, PDF p. 9] "Note that this formula does not give the optimum repetition count for the highest level. ... More repetitions can always be added at that level during the real experiment to improve the results' precision and the counts already found will remain optimal." [KJ13 §9.3, PDF p. 10]

**Check (DERIVED).** Eq. 3 applied to KJ13's Table 5 (`c1, c2` in seconds; `t1, t2` the normalized `√T_i²`) reproduces its column r1: bloat6 √((110.0/35.5)·(14.0²/2.7²)) = 9.13 → 10; lusearch9 0.30 → 1; xalan6 1.22 → 2; xalan9 14.32 → 15 [KJ13 Table 5, PDF p. 10]. For lusearch9, "it has a very high execution variation so experimenter time is much better spent repeating whole executions rather than iterations." [KJ13 §9.4, PDF p. 10]

### 2.5 The interval of the real experiment

After the real experiment, S_n² is recomputed from its data [KJ13 §9.3, Eq. 4, PDF p. 10, **[image]**]:

```
(4)  Ȳ ± t_{1−α/2, ν} · √(S_n² / r_n)  =  Ȳ ± t_{1−α/2, ν} · √( 1/(r_n(r_n − 1)) · Σ_{j_n=1..r_n} ( Ȳ_{j_n •…•} − Ȳ )² ),   ν = r_n − 1
```

"Observe that, for a single-level experiment, the interval is the standard asymptotic interval based on Student's t distribution ... Note also that the multi-level interval is the same as if we had used a single-level interval for the means of all data from all but the highest level (e.g. binary means)." [KJ13 §9.3, PDF p. 10] "The interval estimation, however, uses only estimates/data from the real experiments, so that the results are sound even if the variances change." [KJ13 §9, PDF p. 9]

"RECOMMENDATION: For each benchmark/VM/platform, conduct a dimensioning experiment to establish the optimal repetition counts (equation 3) for each but the top level of the real experiment. Re-dimension only if the benchmark/VM/platform changes." [KJ13 §9.3, PDF p. 10]

On the top level: "We do not show counts of fewer than 5 executions as they could hardly be used to get the variance estimate right (the confidence interval uses only the variance estimate at the highest level, so it is fine to have smaller repetition counts at the other levels)." "The number of executions (highest-level repetitions) can be established on-line, by adding repetitions until the confidence interval is sufficiently narrow." [KJ13 §11, PDF p. 11]

On normality: "Although the theorem does not fully justify this assumption, parametric methods have been found to be robust under various sets of conditions [2, 24]." [KJ13 §4, PDF p. 4]

### 2.6 Modes and bootstrap intervals (KJ12)

KJ13 does not use the words "mode", "modal" or "bimodal" (text search, DERIVED); its "state changes" are changes over iterations (§2.3). The technical report does: "Computer performance measurements cannot be assumed to be normally distributed. Often they are multi-modal, with long-tails to the right. Deviations from normality may not be fatal for the t-test/confidence interval though ... there has been no study of how the t-test/confidence interval is affected by violations from normality common in computer performance data." [KJ12 §3.2, p. 11]

Bootstrap: "The bootstrap method is intuitively simple and works for additional metrics, such as the median, as well as the mean." [KJ12 §7.2, p. 24] Their one-system interval resamples with replacement at every level, 1000 or more times, and takes quantiles of the resampled means: "For the construction of the confidence interval given the simulated means, we use the percentile method." [KJ12 §7.2.1, p. 25]

Evaluation of coverage: "The asymptotic method seems better than bootstrap for small numbers of binaries, say 2 to 20, but there is no practical difference for larger numbers of binaries" [KJ12 §8.3.1, p. 43]; with the asymptotic method coverages "are about 99% for 3 binaries, below 98% for 10 binaries, and below 97% for 20 binaries" [KJ12 §8.4.1, p. 47]; "As in practice a too-high coverage is often worse than too-low coverage, it makes sense to use the asymptotic method (t-distribution) even in cases when the normality assumptions cannot be made." [KJ12 §8.4.1, p. 49] Summary: "For the bootstrap, resampling with replacement at all levels (RRR) is a safe choice. For the asymptotic method, using the t-distribution even when normality assumptions cannot be made seems a safe choice." [KJ12 §8.6, p. 52]

---

## 3. Barrett et al.: steady states found by changepoint analysis (BBKMT17, KFE12)

### 3.1 Segmentation

Design: each benchmark "is run with 2000 in-process iterations and repeated using 30 process executions" [BBKMT17 §3, PDF p. 5]. Outliers first, "conservatively defining an outlier as one that, within a sliding window of 200 in-process iterations, lies outside the median ±3 × (90%ile − 10%ile)" [§4.1, PDF p. 10].

"Formally, a changepoint is a point in time where the statistical properties of prior data are different to the statistical properties of subsequent data; the data between two changepoints is a changepoint segment." "We utilise the PELT algorithm [Killick et al. 2012] which reduces the complexity to O(n) by noting that once an ‘obvious’ changepoint has been discovered, it is not worth including data before that changepoint in further searches." [§4.2, PDF p. 10]

Cost and penalty: "There are various ways of defining when a changepoint has occurred, but the best fit for our data is to consider changes in both the mean and variance of in-process iterations. To automate this, we use the cpt.meanvar function in the R changepoint package [Killick and Eckley 2014], passing 15 log n (where n is the time series length minus the number of outliers) to the penalty argument, and receiving back changepoint locations along with the mean and variance of each changepoint segment. Whilst dependence is observed in some of our experiment's data, the large penalty we use allows us to make an assumption of independence [Antoch et al. 1997]" [§4.2, PDF p. 10]. In the threats section: "As this suggests, penalties are as much an art as a science. A typical penalty for our setup is 4 log n whereas we used 15 log n." [§7, PDF p. 20]

### 3.2 Equivalent segments and classification

Changepoint analysis "has no way of knowing what constitutes the ‘noise floor’ in our problem domain; nor can it guarantee to find identical means for very similar segments separated from one another by a segment with a clearly distinct mean or variance." [§4.3, PDF p. 10] They "formally define that a segment si is equivalent to the final segment s f if mean(si ) is within mean(s f ) ± max(variance(s f ), 0.001s)". The variance was "a good heuristic for this cumulative effect" of external noise, and they "simply interpreted it as having units in seconds rather than seconds squared" [§4.3, PDF p. 11].

Classes: "we (somewhat arbitrarily) define that a process execution reaches a steady-state if all segments which cover the last 500 in-process iterations are considered equivalent to the final segment. If not, we classify the process execution as no steady state". Otherwise, "all segments are considered equivalent, leading to a classification of flat"; "at least one segment is faster than the final segment leading to a classification of slowdown"; "If a steady state benchmark is not flat or a slowdown, then by definition the final segment must be faster than at least one preceding segment, leading to a classification of warmup" [§4.3, PDF p. 11]. "We consider benchmarks whose behaviour is either flat or warmup as ‘good’ (flat benchmarks may be unobservably fast warmup), while benchmarks which are either slowdown or no steady state as ‘bad’." [§4.3, PDF p. 12]

### 3.3 Inconsistent benchmarks

Across process executions: "if its process executions all share the same classification (e.g. warmup) then we classify the pair the same way (in this example, warmup); otherwise we classify the pair as inconsistent." "Good inconsistency can occur because some benchmarks are on the edge of our ability to differentiate warmup from flat behaviour, and we prefer to assume that we are at fault rather than the VM. Bad inconsistency is always more troubling: it means that, at least sometimes, users will experience poor performance." [§4.3, PDF p. 12]

### 3.4 Reporting

Time to steady state: distributions "which makes reporting standard confidence intervals misleading. We therefore use Inter-Quartile Ranges (IQRs) to give an indication of the spread of values, reporting the median and 5% and 95% percentiles (using linear interpolation when the percentile boundaries lie between two data points)." [§4.4, PDF p. 12]

Steady-state performance: "We report means and 99% confidence intervals calculated via bootstrapping (with 100,000 iterations). Although we assume that values within segments are independent (see Section 7), the values across different segments are clearly not independent. When bootstrapping, we therefore sample values within, but never across, segments" [§4.4, PDF p. 12].

Dependence within segments: "Across all three of our benchmarking machines, 11.8% of process executions showed some form of dependence in one or more changepoint segments." A simulation with the smallest (−0.968), largest (0.668) and mean (−0.215) lag-1 dependence they observed gave 99% interval coverages of "100%, 92%, and 99.9% respectively"; independent data gave 98.3% [§7, PDF p. 20].

On a CoV steady-state heuristic (GBE07's, threshold 0.01): "it also finds steady states for 78.1% of the process executions we classify as no steady state" [§8, PDF p. 21].

### 3.5 The method underneath (KFE12)

The segmentation minimizes a penalized cost, `Σ_{i=1..m+1} C(y_(τ_{i−1}+1):τ_i) + βf(m)` (Eq. 1): "Here C is a cost function for a segment and βf (m) is a penalty to guard against over fitting. Twice the negative log likelihood is a commonly used cost function in the changepoint literature" [KFE12 §2, PDF p. 4]. "Examples of such penalties include Akaike's Information Criterion (AIC, Akaike (1974)) (β = 2p) and Schwarz Information Criterion (SIC, also known as BIC; Schwarz, 1978) (β = p log n), where p is the number of additional parameters introduced by adding a changepoint." "A larger minimum segment length is easily implemented when appropriate" [§2, PDF p. 5]. PELT is exact: "the exactness of our approach can lead to substantial improvements in the accuracy of the inferred segmentation of the data" compared with binary segmentation [abstract, PDF p. 2]. Its linear cost has conditions: "Unless otherwise stated, we used the SIC penalty. In this case the penalty constant increases with the amount of data, and as such the application of PELT lies outside the conditions of Theorem 3.2." [§4, PDF p. 14] "Note that for a change in variance (with unknown mean), the minimum segment length is two observations." [§4.1, PDF p. 14]

---

## 4. Independence checks at tens of rounds (LB10, NIST, MDJ18)

LB10 states the assumption and its failure mode. "The assumption that the random variables are iid is capital; if it does not hold, the confidence intervals are wrong." [LB10 §2.2.2, p. 32] "Iid-ness is a property of a stochastic model, not of the data." [§2.3.1, p. 40] "If we compute a confidence interval (using a method that assumes iid data) whereas the iid assumption does not hold, then we introduce some bias. Data arising from high resolution measurements are frequently positively correlated. In such cases, the confidence interval is too small" [§2.3.3, p. 42]. Sub-sampling can restore independence, but "it does not work if the data set is small, nor for some large data sets, which remain correlated after repeated sub-sampling (such data sets are called long range dependent)." [§2.3.3, p. 43]

LB10's three checks [§2.3.2, pp. 41–42]:
1. Autocorrelation: "If the data is iid, then ρk = 0 for k ≥ 1, and the sample autocorrelation coefficients fall within the values ±1.96/√n (where n is the sample size) with 95% probability." [p. 41]
2. Lag plots of the value at time t against time t + h [p. 42].
3. The turning point test: "A test provides an automated answer, but is sometimes less sure than a visual inspection." [p. 42] A turning point is an index where the sequence is not monotonic; "Under H0 , the probability of a turning point at i is 2/3" [§4.5.2, p. 123], and for large n the count T is approximately normal with mean (2n − 4)/3 and variance (16n − 29)/90, so `p = 2(1 − N_{0,1}(|T − (2n−4)/3| / √((16n−29)/90)))` [Eq. 4.44, p. 124, **[image]**].

NIST gives the same autocorrelation bound, `±z_{1−α/2}/√N`, as "recommended" when the plot "is being used to test for randomness (i.e., there is no time dependence in the data)" [NIST §1.3.3.1], and a runs test above and below the median: "We will code values above the median as positive and values below the median as negative. A run is defined as a series of consecutive positive (or negative) values." Its statistic is `Z = (R − R̄)/s_R` with `R̄ = 2n₁n₂/(n₁ + n₂) + 1` and `s_R² = 2n₁n₂(2n₁n₂ − n₁ − n₂)/((n₁ + n₂)²(n₁ + n₂ − 1))`; the normal approximation is for n₁, n₂ > 10, and "For a small-sample runs test, there are tables to determine critical values that depend on values of n1 and n2 (Mendenhall, 1982)." [NIST §1.3.5.13] "When the autocorrelation is used to detect non-randomness, it is usually only the first (lag 1) autocorrelation that is of interest." [NIST §1.3.5.12]

MDJ18 test stationarity with the augmented Dickey–Fuller test: "Most statistical tests—including confidence intervals— assume stationarity" [MDJ18 §4.4, p. 415].

**What these checks can see at tens of rounds (DERIVED).**

The lag-1 bound `±1.96/√n`: ±0.80 at n = 6, ±0.62 at 10, ±0.44 at 20, ±0.36 at 30, ±0.28 at 50, ±0.20 at 100.

The runs test has an exact small-sample distribution. Given n₁ values above and n₂ below the median, every arrangement is equally likely for i.i.d. continuous data, so the number of runs R has

```
P(R = 2k)     = 2·C(n₁−1, k−1)·C(n₂−1, k−1) / C(n₁+n₂, n₁)
P(R = 2k + 1) = [C(n₁−1, k)·C(n₂−1, k−1) + C(n₁−1, k−1)·C(n₂−1, k)] / C(n₁+n₂, n₁)
```

(the classical run-count distribution, derived here by counting compositions; the tables NIST cites were not read). With the median value itself dropped when n is odd, so n₁ = n₂ = ⌊n/2⌋, the two-sided 5% test rejects when:

| n (rounds) | reject if R ≤ | or R ≥ | E[R] | P(R ≤ lower) |
|---|---|---|---|---|
| ≤ 9 | no rejection region | | | |
| 10–11 | 2 | 10 | 6 | 0.0079 |
| 12–13 | 3 | 11 | 7 | 0.0130 |
| 14–15 | 3 | 13 | 8 | 0.0041 |
| 16–17 | 4 | 14 | 9 | 0.0089 |
| 18–19 | 5 | 15 | 10 | 0.0122 |
| 20–21 | 6 | 16 | 11 | 0.0185 |
| 24–25 | 7 | 19 | 13 | 0.0095 |
| 30–31 | 10 | 22 | 16 | 0.0199 |
| 40–41 | 14 | 28 | 21 | 0.0182 |
| 50–51 | 18 | 34 | 26 | 0.0156 |

Few runs means persistent states (a state in time); many runs means alternation, the "odd-numbered iterations show one time and even-numbered ones another" pattern KJ13 name. At n = 10 only one block of low rounds followed by one block of high rounds (R = 2), or perfect alternation (R = 10), is detectable.

---

## 5. Deciding whether rounds form more than one state

### 5.1 Hartigan & Hartigan's dip test (HH85, DIPTEST, CH98, AAC19)

Abstract: "The dip test measures multimodality in a sample by the maximum difference, over all sample points, between the empirical distribution function, and the unimodal distribution function that minimizes that maximum difference. The uniform distribution is the asymptotically least favorable unimodal distribution, and the distribution of the test statistic is determined asymptotically and empirically when sampling from the uniform." [HH85, p. 70, **[image]**]

Definitions: "A distribution function F is unimodal with mode m if F is convex in (−∞, m] and concave in [m, ∞)." With `ρ(F, G) = sup_x |F(x) − G(x)|` and 𝒰 the class of unimodal distribution functions, "The dip of a distribution function F is defined by D(F) = ρ(F, 𝒰)"; "thus the dip measures departure from unimodality." [HH85 §2, p. 71, **[image]**]

The argument for the uniform null: "We propose the dip statistic as the maximum difference between the empirical distribution function, and the unimodal distribution function that minimizes that maximum difference. The statistic may be computed in order n operations, for n observations, and it is consistent for testing any unimodal against any multimodal distribution. We argue that the appropriate null distribution is uniform, by showing that the dip is asymptotically larger for the uniform than for any distribution in a wide class of unimodal distributions, those with exponentially decreasing tails. (We speculate that the result holds for the class of all unimodal distributions.)" [HH85 §1, p. 71, **[image]**] Theorem 5: for F unimodal with a nonzero kth derivative at the mode (k ≥ 2) and exponentially decreasing density, "Then √nD(F_n) → 0 in probability." [HH85 Theorem 5, p. 75, **[image]**] By contrast, "From Theorem 3, √nD(F_n) converges in distribution to the dip computed for a Brownian bridge" when sampling from the uniform [HH85 §5, p. 80, **[image]**].

It needs no kernel width: "A modal interval is produced as an outcome of the dip calculation; it is not known how this competes with the various estimates of a mode ... It does have the benefit of not requiring a kernel width." [HH85 §1, p. 71, **[image]**] The algorithm: "An order n algorithm exists." It is described as a taut string between the empirical distribution function shifted up and down by d, with steps (i)–(vii) [HH85 p. 79, **[image]**]; in that description "2D(F) is the minimum value of d_ij", so implementations must watch the factor of two. CH98 note the same scale difference: the excess mass and dip tests "may be shown to be equivalent in the one-dimensional case, in that the excess mass statistic equals exactly twice the dip statistic" [CH98 §1.2, p. 580].

DIPTEST fixes the scale and small n: "For n ≤ 3 where n <- length(x), the dip statistic Dn is always the same minimum value, 1/(2n), i.e., there's no possible dip test." It also records that the original Fortran "was not giving symmetric results for mirrored data", traced to a misplaced parenthesis, and that "This bug has been corrected for diptest version 0.25-0 (Feb 13, 2004)"; it calls the Statlib code of AS217 "(buggy!)" [DIPTEST manual, `dip`].

HH85 on other tests (p. 70, **[image]**). Wolfe's likelihood ratio for a two-component normal mixture "may be expected to be quite sensitive to the normality assumption, and may, for example, decide with high probability that a long-tailed unimodal distribution has more than one mode." Engelman and Hartigan's test "divides the sample into two subsets to maximize the likelihood ratio that the two subsets are sampled from normals with different means, against the null hypothesis that the means are equal. The test statistic is the maximum likelihood ratio over all divisions. The distribution is asymptotically normal (Hartigan, 1978), the statistic is easy to compute, but again the test will not work well when the bimodal alternative is not a normal mixture."

### 5.2 Critical values

"In Table 1 appear the percentage points .01 .05 .10 .50 .90 .95 .99 .995 .999 of the DIP, for sample sizes n = 4–10, 15, 20, 30, 50, 100, 200, based on 9999 repetitions from the uniform. ... the table shows that √nD(F_n) has very nearly the same percentage points for n = 100 as n = 200. It is suggested that interpolation be based on √nDIP." [HH85 §5, p. 80, **[image]**] Table 1's notes: "(1) Dip is the maximum distance between the empirical distribution and the best fitting unimodal distribution. (2) Based on 9999 dips. Maximum standard error is .001." and "(4) Interpolate on √n dip." [HH85 Table 1, p. 80, **[image]**]

DIPTEST recomputed the table: "Whereas Hartigan(1985) published a table of empirical percentage points of the dip statistic (see dip) based on N=9999 samples of size n from U [0, 1], our table of empirical quantiles is currently based on N=1’000’001 samples for each n." [DIPTEST manual, `qDiptab`] Its `dip.test` interpolates: "the p-value is computed via linear interpolation (of √n D_n) in the qDiptab table" [DIPTEST manual, `dip.test`]. The code interpolates √n·D linearly in n between the tabulated sizes (`R/dipTest.R`).

The upper percentage points are the critical values: reject unimodality at level α when D_n exceeds the (1 − α) point.

| n | HH85 .90 | HH85 .95 (α = 0.05) | HH85 .99 (α = 0.01) | DIPTEST .95 | DIPTEST .99 |
|---|---|---|---|---|---|
| 4 | .1863 | .2056 | .2325 | .2073 | .2318 |
| 5 | .1773 | .1872 | .1966 | .1864 | .1965 |
| 6 | .1586 | .1645 | .1904 | .1648 | .1919 |
| 7 | .1445 | .1597 | .1832 | .1599 | .1841 |
| 8 | .1428 | .1552 | .1744 | .1540 | .1730 |
| 9 | .1362 | .1458 | .1623 | .1466 | .1642 |
| 10 | .1302 | .1394 | .1623 (footnote "Repeated computations") | .1396 | .1597 |
| 15 | .1097 | .1179 | .1365 | .1188 | .1360 |
| 20 | .0970 | .1047 | .1209 | .1051 | .1206 |
| 30 | .0815 | .0884 | .1012 | .0882 | .1015 |
| 50 | .0645 | .0702 | .0804 | .0703 | .0812 |
| 100 | .0471 | .0510 | .0586 | .0511 | .0590 |
| 200 | .0341 | .0370 | .0429 | .0368 | .0427 |

HH85 columns **[image]**; DIPTEST columns rounded to four places. Cross-check (DERIVED): the two tables differ by at most 0.0017 in the .95 column and 0.0019 in the .99 column, except n = 10 at .99 (0.0026). For small n, the lower percentage points equal 1/(2n) (for example .1250 at n = 4, .0833 at n = 6), the atom DIPTEST describes. The interpolated values for every n from 4 to 50 are in §9.4.

### 5.3 Power and conservatism

HH85 computed power for one alternative only: F₁, "a mixture, in the proportions 3:2:3, of a uniform on (0, ¼), a uniform on (¼, ¾), and a uniform on (¾, 1)" [p. 80]. Table 2, "In sampling from F1, the probability that the statistic exceeds the 95% point computed in sampling from F0 (based on 1000 repetitions)": dip .795 at n = 50 and .973 at n = 100 (depth .749/.961, likelihood ratio .540/.905) [HH85 Table 2, p. 81, **[image]**].

The uniform null makes the test conservative for other unimodal shapes. HH85: "There may be evidence in the data that, if the true distribution is unimodal, it is far from uniform. Following Silverman (1981), it would be possible to evaluate the significance of a computed dip, against the null distribution of dips obtained by sampling from the best fitting unimodal distribution as specified in Theorem 6. This procedure should have better power than the present test for discovering two relatively close modes with pronounced tails" [pp. 81–82, **[image]**]. CH98: "Suggestions by Hartigan and Hartigan (1985) and Müller and Sawitzki (1991a) that the tests be based on comparisons with properties of samples of uniform random variables are attractive on the grounds of simplicity, but they lead to considerable conservatism. It may be shown that the asymptotic levels of such tests are zero, for each non-zero value of nominal level." [CH98 §1.2, p. 580] CH98's calibrated test was simulated at "The sample size n was 50, 100 or 200." [§3, p. 583]. AAC19 simulated n = 50, 200 and 1000 and found "the results obtained with HH are quite conservative. For instance, for n = 1000, even taking α = 0.10, the percentage of rejections is always below 0.002." [AAC19 §3, PDF p. 21]

**No source read gives the dip test's power below n = 50** (HH85, CH98, AAC19 start at 50; FD13 at 250, §5.7).

**A necessary condition at small n (DERIVED).** Take two states with minority share p, each narrow compared with the gap between them. A unimodal distribution function can jump only at its mode, so a fit that is continuous across the minority state must miss its jump of p by at least p/2 on one side. A fit that places the majority at the mode and spreads the missing mass across the gap achieves p/2. So the sample dip is about min(p, 1 − p)/2, and never below 1/(2n). A wider state with width w and gap g still has a dip of at least p/(2(1 + w/g)). The dip test at level α can therefore flag two states only if p > 2·D_crit(n, α): about 0.28 of the rounds at n = 10, 0.21 at 20, 0.18 at 30, 0.14 at 50, 0.10 at 100 (α = 0.05). This is a necessary condition, not a power estimate; power for real state shapes has to be measured by resampling from them (§9.2).

### 5.4 Silverman's critical bandwidth (SIL81, HY01, CH98, AAC19)

With a normal-kernel density estimate `f̂(t; h)`, "Define the k-critical window width h_crit by h_crit = inf{h; f̂(., h) has at most k modes}." "Large values of h_crit will reject the null hypothesis." [SIL81 §2, Eq. 2, p. 97, **[image]**] Significance comes from simulation: "To provide a conservative assessment of the significance of h0, an appealing choice of the representative g0 from which to simulate is obtained by rescaling f̂(., h0), as constructed from the data, to have variance equal to the sample variance." [§3, p. 98, **[image]**] The worked example used "100 replications of 22 observations" and reads the p-values for 1, 2, 3, 4 modes as "a hierarchical set of significance tests" [§4, p. 99, **[image]**].

HY01: "It is known that Silverman's bootstrap test for multimodality tends towards conservatism, even in large samples, in the sense that the actual level tends to be less than the nominal one." [HY01 abstract, p. 515] CH98 add that it "is itself quite conservative, however, even in the asymptotic limit" [§1.2, p. 580], and AAC19 found SI "quite conservative: even for high sample sizes, the percentage of rejections is below the significance level, and quite close to 0 even for α = 0.10" [AAC19 §3, PDF p. 20]. The test needs a bootstrap per point and gives no tabulated critical values.

### 5.5 The mixture likelihood ratio (KMM) and Ashman's separation (ABZ94)

KMM fits one- and g-component Gaussian mixtures. The likelihood ratio statistic "is an estimate of the improvement in going from a 1-mode to a g-mode fit. The significance of the LRTS may be estimated by comparing –2lnλ to a χ2 distribution ... However, this provides only an approximation of the statistical significance. For the homoscedastic, univariate case, this approximation has been shown to be adequate ... However, for more complicated situations, the only way to reliably assess the statistical significance is a bootstrap estimation" [ABZ94 §2.1, p. 2350].

Sample size: "Our experiments revealed that for N < 50 the likelihood ratio test used in the KMM algorithm does not provide a reliable method for detecting bimodality." [ABZ94 §3, p. 2352] False positives on single Gaussians with N = 50–500: "even in low-N datasets, the false-positive frequency is never greater than 10%" [§3.3, p. 2356].

The separation: "We define a dimensionless separation of the means", Δμ = (μ2 − μ1)/σ (Eq. 3.1), with σ² the common variance of a homoscedastic two-Gaussian mixture of equal proportions; "such a distribution only shows two peaks if" Δμ ≥ 2.0 (Eq. 3.2, "cf. Everitt & Hand 1981") [ABZ94 §3, p. 2351]. It is a property of the parent mixture's parameters. The paper does not name a statistic "D" or give a "D > 2" rule for unequal variances; that form is later usage. The R package `modes` documents an "Ashman's D" taking `mu1, mu2, sd1, sd2`, with "A good rule of thumb is that if the statistic is above ~2, there is good separation" (https://rdrr.io/cran/modes/man/Ashmans_D.html, NON-PEER-REVIEWED); where the unequal-variance form first appeared is **UNVERIFIED**.

### 5.6 The bimodality coefficient (PSJ13)

`BC = (m₃² + 1) / (m₄ + 3·(n − 1)²/((n − 2)(n − 3)))`, with m₃ the skewness and m₄ the excess kurtosis, both bias-corrected; "The BC of a given empirical distribution is then compared to a benchmark value of BCcrit = 5/9 ≈ 0.555 that would be expected for a uniform distribution" [PSJ13, p. 1, formula **[image]**]. "A probability density function for the BC, however, cannot be derived (Knapp, 2007). This is a major drawback because it precludes a thorough null-hypothesis significance test." Skew misleads it: "Distribution C, however, is clearly unimodal when inspected by eye but its heavy skew also leads to a large BC." [PSJ13, p. 2] Throughput rounds are skewed (§6.3), so the BC is unsuitable here.

### 5.7 Freeman & Dale's comparison (FD13)

Simulations used "250, 500, 1,000, or 2,000 simulated observations" [FD13, PDF p. 5]. "Among the simulations with unimodality (proportion = 0 %), BC was beyond the .555 threshold 21 % of the time; HDS had a significant p value 0 % of the time; and AICdiff judged the two-component Gaussian model to be a more economical fit 81 % of the time." For bimodal simulations, "BC hit threshold in 65 % of the cases; HDS in 58 % of the cases; and AICdiff in 94 % of the cases." [FD13, PDF p. 6] "Finally, sample size had a relatively weak influence on all three measures." [PDF p. 10] On small samples: "In general, the sampling error of skewness and kurtosis are high at smaller sample sizes (10 or fewer), suggesting that the BC, which is computed from these parameters, may be unstable at smaller sample sizes." [PDF p. 5] FD13 contain no comparison at tens of observations. PSJ13's summary of it: "both measures have merit for assessing bimodality but neither statistic is perfectly sensitive and specific at the same time" [PSJ13, p. 2].

### 5.8 A separation index from a hard split is not a test (DERIVED)

Split a standard normal distribution at its mean, which is where a minimum-within-sum-of-squares split of a large normal sample falls. Each half has mean ±√(2/π) = ±0.798 and standard deviation √(1 − 2/π) = 0.603, so

```
Δμ/σ = 2·√(2/π) / √(1 − 2/π) = 1.596 / 0.603 = 2.647
```

The same split of a uniform distribution gives 0.5/(0.5/√12) = 3.46. Any separation index (ABZ94's Δμ, or the later unequal-variance "D") computed from a hard split of a unimodal sample exceeds 2. The sources agree that the maximized split statistic needs its own null distribution: Engelman and Hartigan's "maximum likelihood ratio over all divisions" has one ("asymptotically normal") [HH85 p. 70, **[image]**], and ABZ94's significance comes from the likelihood ratio test, not from Δμ. Δμ ≥ 2 describes a fitted parent mixture, not a sample split. The pitfall in the task statement is confirmed: the value is 2.65.

### 5.9 Splitting (FIS58, EH69: UNVERIFIED)

FIS58 is the usual citation for optimal grouping of ordered one-dimensional data; it was not accessible, so nothing is quoted from it. The two-group case needs nothing beyond sorting (DERIVED): for sorted `x_(1) ≤ … ≤ x_(n)`, a split into two groups minimizing the within-group sum of squares puts every value below the cut in one group, so only the n − 1 cuts between consecutive order statistics need comparing, in O(n) with prefix sums. HH85's description of EH69 (§5.1) is the maximum-likelihood version of the same division for two normal groups with a common variance.

---

## 6. Intervals (HB15, LB10, MDJ18, KJ12, CHR65)

### 6.1 Normality and the mean (HB15)

"Most of the statistics described in this section can only be used if measurements are independent samples of a normal distribution." [HB15 §3.1.2, p. 4] The interval of the mean is `[x̄ − t(n−1, α/2)s/√n, x̄ + t(n−1, α/2)s/√n]` [p. 4]. "Rule 5: Report if the measurement values are deterministic. For nondeterministic data, report confidence intervals of the measurement." with the example "We collected measurements until the 99% confidence interval was within 5% of our reported means." [p. 4]

Checking normality: "Razali and Wah [48] showed empirically that the Shapiro-Wilk test [51] is most powerful, yet, it may be misleading for large sample sizes. We thus suggest to check the test result with a Q-Q plot or an analysis specific to the used statistics." [p. 4] The central limit theorem is not a licence: "Our experiments (Figure 2) and other authors [12] show that the 30-40 samples as indicated in some textbooks [38] are not sufficient. We recommend attempting to normalize the samples until a test of the resulting distribution indicates normality or use the nonparametric techniques described in the next section." "Rule 6: Do not assume normality of collected data (e.g., based on the number of samples) without diagnostic checking." [p. 5]

Two modes: "The top of Figure 3 demonstrates how assuming normality can lead to wrong conclusions: the CI around the mean is tiny while the mean does not represent the distribution well." [p. 5] Figure 3 shows a bimodal latency density for one system with the mean between the modes, a narrow 99% interval of the mean and a wide 99% interval of the median [p. 6].

Summarizing rates: "In general, if the denominator has the primary semantic meaning, the harmonic mean provides correct results ... If the absolute counts (e.g., flops and seconds) are available we recommend using the arithmetic mean for both before computing the rate." "Rule 3: Use the arithmetic mean only for summarizing costs. Use the harmonic mean for summarizing rates." [§3.1.1, p. 3, the first quote checked on the page image] For rounds of equal duration, Σops/Σtime equals the arithmetic mean of the per-round rates (DERIVED).

### 6.2 The median and other quantiles (HB15, LB10)

"However, normal distributions are only rarely observed when measuring computer performance, where most system effects lead to increased execution times. Sources of error are scheduling, congestion, cache misses etc., typically leading to multi-modal distributions that are heavily skewed to the right." "Nonparametric metrics, such as the median or other percentiles, do not assume a specific distribution and are most robust. However, these measures also require independent and identically distributed (iid) measurements." [HB15 §3.1.3, p. 5] Rank formula: "Le Boudec [9] shows that the 1−α CI ranges from the measurement at rank ⌊(n − z(α/2)√n)/2⌋ to rank ⌈1 + (n + z(α/2)√n)/2⌉ ... For example, for a 95% CI, z(0.025) = 1.96. We note that one cannot compute exact CIs because it only considers measured values as ranks and the resulting bounds can be slightly wider than necessary" [p. 5, formula **[image]**].

LB10 Theorem 2.1 is exact for i.i.d. data from any distribution with a density (with a half-open interval otherwise): for i.i.d. X₁…Xₙ with order statistics X₍₁₎ ≤ … ≤ X₍ₙ₎ and `B_{n,p}` the binomial CDF, "A confidence interval for mp at level γ is [X(j) , X(k) ] where j and k satisfy Bn,p (k − 1) − Bn,p (j − 1) ≥ γ" [LB10 §2.2.2, p. 33]. For large n, `j ≈ ⌊np − η√(np(1 − p))⌋` and `k ≈ ⌈np + η√(np(1 − p))⌉ + 1` [p. 33]. "For n = 10, the theorem and the table in Section A say that a 95%-confidence interval for the median ... is [X(2), X(9)]." "Note that, for small values of n, no confidence interval is possible at the levels 0.95 or 0.99. This is due to the probability that the true quantile is outside any of the observed data still being large." [p. 33]

LB10 Table A.1, median (q = 50%) [p. 315; transcribed from the page, every row below recomputed from the binomial (DERIVED check, no mismatch)]:

| n | 95%: j, k | actual level | 99%: j, k | actual level |
|---|---|---|---|---|
| ≤ 5 | none possible | | none possible (n ≤ 7) | |
| 6 | 1, 6 | 0.969 | — | |
| 7 | 1, 7 | 0.984 | — | |
| 8 | 1, 7 | 0.961 | 1, 8 | 0.992 |
| 9 | 2, 8 | 0.961 | 1, 9 | 0.996 |
| 10 | 2, 9 | 0.979 | 1, 10 | 0.998 |
| 11 | 2, 10 | 0.988 | 1, 11 | 0.999 |
| 12 | 3, 10 | 0.961 | 2, 11 | 0.994 |
| 13 | 3, 11 | 0.978 | 2, 12 | 0.997 |
| 14 | 3, 11 | 0.965 | 2, 12 | 0.993 |
| 15 | 4, 12 | 0.965 | 3, 13 | 0.993 |
| 16 | 4, 12 | 0.951 | 3, 14 | 0.996 |
| 17 | 5, 13 | 0.951 | 3, 15 | 0.998 |
| 18 | 5, 14 | 0.969 | 4, 15 | 0.992 |
| 19 | 5, 15 | 0.981 | 4, 16 | 0.996 |
| 20 | 6, 15 | 0.959 | 4, 16 | 0.993 |
| 21 | 6, 16 | 0.973 | 5, 17 | 0.993 |
| 22 | 6, 16 | 0.965 | 5, 18 | 0.996 |
| 23 | 7, 17 | 0.965 | 5, 19 | 0.997 |
| 24 | 7, 17 | 0.957 | 6, 19 | 0.993 |
| 25 | 8, 18 | 0.957 | 6, 20 | 0.996 |
| 26 | 8, 19 | 0.971 | 7, 20 | 0.991 |
| 27 | 8, 20 | 0.981 | 7, 21 | 0.994 |
| 28 | 9, 20 | 0.964 | 7, 21 | 0.992 |
| 29 | 9, 21 | 0.976 | 8, 22 | 0.992 |
| 30 | 10, 21 | 0.957 | 8, 23 | 0.995 |
| 40 | 14, 27 | 0.962 | 12, 29 | 0.994 |
| 50 | 18, 32 | 0.951 | 16, 35 | 0.993 |
| 60 | 23, 39 | 0.960 | | |
| 70 | 27, 44 | 0.959 | | |
| ≥ 71 | ≈ ⌊0.50n − 0.980√n⌋, ⌈0.50n + 1 + 0.980√n⌉ | 0.950 | (n ≥ 73) ≈ ⌊0.50n − 1.288√n⌋, ⌈0.50n + 1 + 1.288√n⌉ | 0.990 |

The rank approximation at small n (DERIVED): at n = 6 it gives ranks 0 and 7, outside the sample; at n = 20 it gives [x₍₅₎, x₍₁₆₎] with coverage 0.988, where the exact interval is [x₍₆₎, x₍₁₅₎] at 0.959; at n = 50 it gives [x₍₁₈₎, x₍₃₃₎] at 0.967. Below 71 the exact table is narrower and never out of range.

### 6.3 Evidence from performance data (MDJ18)

"Many studies suggest that the normality assumption does not hold for the data obtained in computer systems experiments ... Thus, we adopt nonparametric statistics for the remainder of this paper, and recommend that, for performance experiments, these methods be used unless normality can be demonstrated." MDJ18 compute median intervals with the same rank formula, citing LB10 [MDJ18 §2, p. 410]. With Shapiro–Wilk, "we should reject the null hypothesis for over 99% of the configurations (710 out of 713)", and for bandwidth "there is a practical maximum that cannot be exceeded except by measurement error, and most measurements lie near this maximum ... leading to a skewed distribution with a compressed range above the median and a much larger range below it. The situation is reversed for latency tests." [§4.3, p. 415] On one server, "roughly half of the points (26,695) can be considered to be coming from a normal distribution" [p. 415].

Two modes: "The SSDs that we tested, on the other hand, exhibit a bimodal pattern; the exact underlying cause is difficult to ascertain because of the opaque nature of the vendor's FTL, but the effect on experiments is clear and dramatic." This was random reads at iodepth 1 across servers [§4.2, p. 414]. "For extreme multi-modal distributions, such as the one seen in Figure 2, the mean and standard deviation have no problem computing values “in the middle” where no points actually lie, but the median and nonparametric CIs can only pick from points actually in the dataset, making it take much longer for them to converge—or preventing them from converging at all." [§5, p. 417]

### 6.4 How many measurements (HB15, MDJ18)

For normal data HB15 give `n = (s·t(n−1, α/2) / (e·x̄))²` [§4.2.2, p. 8, **[image]**], which is note 11 §16.3's formula. "Non-normally distributed data If the distribution is unknown, one cannot easily compute the required number of measurements analytically. However, it is possible to check if a given set of measurements satisfies the required accuracy. Thus, we recommend recomputing the 1−α CI (cf. Section 3.1.3) after each ni = i · k, i ∈ N measurements and stop the measurement once the required interval is reached. ... Furthermore. we note that n > 5 measurements are needed to assess confidence intervals nonparametrically." [HB15 §4.2.2, p. 8]

MDJ18's CONFIRM estimates the count by resampling subsets of past measurements without replacement: "we start at s = 10, assuming that smaller subsets are insufficient to estimate nonparametric CIs reliably and should not be considered", with "c = 200" trials per size [§5, p. 416]. Examples: CoV 0.3% needs about 10 runs and CoV 9.0% about 240 for median intervals within 1% [§4.1, p. 414]; "most configurations up to about 4% CoV require only tens of repetitions to reach the target of r = 1% for CIs" [§5, p. 417]; and "The relationship between variance and the number of repetitions required is complex; good estimates of the latter require significant prior data." [p. 417]

### 6.5 Stopping when the interval is narrow enough (CHR65)

CHR65 want "a confidence interval of prescribed width 2d and prescribed coverage probability α" when σ² is unknown. They stop at the first N with `v_N ≤ d²N/a_N²`, where `v_n = n⁻¹Σ(x_i − x̄_n)² + n⁻¹` and a_n → a, the normal quantile. "Under the sole assumption that 0 < σ² < ∞", the coverage tends to the nominal one as d → 0, "(asymptotic “consistency”)" [CHR65 §1, pp. 457–458, **[image]**]. The start can be delayed: "N in (3) could be defined as the smallest (or the smallest odd, etc.) integer ≥ n0 such that the indicated inequality holds, where n0 is any fixed positive integer." [p. 458, **[image]**] The guarantee is asymptotic in narrow intervals. A rule that may stop after three rounds has no coverage guarantee (DERIVED).

---

## 7. The minimum as an estimator (CR16, NON-PEER-REVIEWED)

CR16 model a timing as the true time plus positive delays. "In the limit where the delay factor time scales are greater than τacc , the total error terms will always be positive, such that choosing the smallest timing measurement will choose the sample with the smallest magnitude of error." [CR16 §IV.B, PDF p. 5] On modes: "While estimators like the median and trimmed mean are known to be robust to outliers [27], Fig. 3 demonstrates that they still capture bimodality of the distributions plotted in Fig. 4. Thus, these estimators are undesirable for choosing n, since the result could vary drastically between different executions of Alg. 1, depending on which of the estimator's modes was captured in the sample ... In contrast, the distribution of the minimum across all experimental trials is unimodal in all cases we have observed." [§IV.B, PDF p. 5] Their benchmarks showed "bimodality in the second and fourth benchmarks" [§I.A, PDF p. 2].

For throughput the analogue is the maximum round (DERIVED). The argument needs every disturbance to lower throughput. Storage devices also have transient states that raise it: SNIA-PTS describes "a brief period of elevated performance" in a fresh device [p. 13], and a write cache absorbs bursts. The maximum can therefore report a transient state. A median of the upper state, with its interval (§9.3), estimates the same quantity without that failure. CR16 is a poster preprint; nothing in mantle should rest on it alone.

---

## 8. Storage results with more than one state

**SNIA-PTS** (primary, NON-PEER-REVIEWED). "The performance of an SSS device is highly dependent on its prior usage, the pre-test state of the device and test parameters." [§3, p. 20] "A typical SSS device, taken Fresh-Out-of-the-Box (FOB), and exposed to a workload, typically experiences a brief period of elevated performance, followed by a transition to Steady State performance." [p. 13] "Transition Zone: A performance state where the device's performance is changing as it goes from one state to another (such as from FOB to Steady State)." [§2.1.26, p. 17] Steady state is required for reporting, "To ensure that a device's initial performance (FOB or Purged) will not be reported as “typical”", and "oscillations around an average are “steady” in a sense, but might be a cause for concern." [§3.1, p. 20] The steady-state test is range ≤ 20% of the average and best-fit-line excursion ≤ 10% over a five-round window [§2.1.24, p. 17; note 11 §13.2]. When it is not met: "If Steady State was not reached for any of the SS Variables, then the Test Operator may report results per above, picking the average of the last five Rounds run as Round x. In the case where Steady State was not reached, the Test Operator must state this fact in the final report." [§7.3, p. 28]

**CKZ09.** "we found that fragmentation could seriously impact performance – by a factor of over 14 times on a recently announced SSD" [abstract, PDF p. 1]. "After fragmentation, the bandwidth of sequential write on SSD-M collapses from 42.7MB/sec to 3.02MB/sec (14 times lower)." [§5.8, PDF p. 9] The state persists: "However, it seems that a long idle period would not automatically cure a fragmented SSD." [§5.8, PDF p. 10]

**KIM11.** "SSDs can exhibit significant performance degradations when garbage collection (GC) conflicts with an ongoing I/O request stream." [abstract, PDF p. 1] "during an ongoing GC process incoming requests targeted for the same Flash device that is busy with the ongoing GC process are stalled and placed in a queue and scheduled for service following the completion of the GC process [22]. This stalling can significantly degrade performance when incoming requests are bursty." [§II.D, PDF p. 3] In their time series, "the bandwidth fluctuates more widely due to GC activity with respect to increasing mix of write requests in workloads." [§III.C, PDF p. 4]

**JK13** (*abstract only*): "the performance of sequential reads gets significantly worse over time", and the paper studies "the worst-case performance characteristics of our SSDs" and enhancements such as "TRIM commands and background-tasks".

**DES12.** Simulations were run "to “warm up” the FTL algorithm, ensuring some degree of fragmentation; statistics collection began after the end of the warmup period." [§6, PDF pp. 8–9]

**MDJ18.** SSD random reads at iodepth 1 were bimodal (§6.3).

What these recommend reporting: SNIA-PTS reports only the steady state (or states that it was not reached) and plots convergence; the peer-reviewed papers show time series and before/after comparisons. None gives a method for reporting a point whose results occupy two states (DERIVED from the texts read).

---

## 9. Consequences for mantle

### 9.1 Two kinds of state in mantle's data (DERIVED)

- **States in time.** "The first put points after the fill pass ran at half their usual rate (4 KiB puts with one writer: 102/s) and at the usual rate when run later in the same process" (2026-09-28 chunk-store benchmark, finding 7). That is a device state that persists across rounds, like the fragmented or GC-bound states of CKZ09 and KIM11 and SNIA-PTS's transition zone, and it is variation at the pass level of §9.2: the same point, revisited later in the same process, measured differently. In a point's rounds it shows as dependence: runs of slow rounds and a changepoint (KJ13's "state changes"). `mantle bench chunk` also creates dependence by design: each round's puts write for a step "or until they have written a third of the volume" before that round's reads (`bench.rs` module documentation), so every round changes the state the next round starts from.
- **States in distribution.** "every thread's reads stall together for about 6 ms at a time, in bursts, at random moments. A one-second round catches none or several." (2026-09-29 rounds measurement, finding 1). If bursts are independent of round boundaries, rounds are independent draws from a mixture indexed by the number of bursts caught, and the distribution can have one mode per count. KIM11's mechanism (requests stalled behind garbage collection) is one candidate cause; it is **UNVERIFIED** for this machine.

The two need different treatment. Every interval and modality test in §4–§6 assumes i.i.d. rounds (LB10 p. 32; HB15 p. 5; KJ13 §6). So states in time are looked for first, by the independence check, and handled by segmentation or by repeating at a higher level. States in distribution are looked for only among independent rounds, by the dip test.

### 9.2 The dimensioning experiment

**Levels** (KJ13 §3 applied to mantle, DERIVED):

| Level | One repetition is | Cost `c_i`: time exclusively added (measured) | Variation it can carry |
|---|---|---|---|
| 1 round | one round of one point: put step, get step, file-layer step, delete | the round's wall time | stall bursts caught; the round's own writes |
| 2 pass | the same point revisited later in the same process, after the other points | the settling rounds a revisit discards | drive history built by the other points |
| 3 process | a new process: calibration, a new scratch volume and its fill | process start + calibration + volume creation + fill | file placement, allocator state, the fill's effect on the drive |

Level 3 is the top. Only a run that repeats processes can state an interval covering process-to-process variation (KJ13 §3: "At the very least, repetition must be done at the highest level that has random variation to avoid bias").

**Counts** (KJ13 §9.1): r₃ = 20 processes (30 if affordable), r₂ = 10 passes, r₁ = 30 rounds, rounds kept in time order. The full design is 6,000 rounds per point, about 5 h per point at 3 s per round, too long for 16 points. KJ13 allow partial experiments that keep the top level (DERIVED arithmetic):
- **A** (levels 3 and 1): 20 processes × 30 rounds × 16 points × 3 s ≈ 8 h, plus 20 process setups. One overnight run per device.
- **B** (levels 3, 2, 1), only for points whose rounds fail the independence check in A.

**Formulas.** Eq. 1–3 of §2.4. For experiment A (two levels, j₂ = process, j₁ = round):

```
S₁² = (1/r₂) Σ_{j₂} [ 1/(r₁ − 1) Σ_{j₁} (Y_{j₂j₁} − Ȳ_{j₂•})² ]       mean within-process variance
S₂² = 1/(r₂ − 1) Σ_{j₂} (Ȳ_{j₂•} − Ȳ_{••})²                           variance of process means
T₁² = S₁²,   T₂² = S₂² − S₁²/r₁
r₁* = ⌈ √( (c₂/c₁) · (T₁²/T₂²) ) ⌉                                     rounds per process in real runs
interval of a real run with r₂ processes: Ȳ ± t_{0.975, r₂−1} · √(S₂²/r₂)      (Eq. 4)
```

If T₂² ≤ 0, process-level variation is negligible and a single process's rounds suffice. If r₁* = 1, processes, not rounds, should be repeated, as for KJ13's lusearch9.

**Also measured in the dimensioning run (DERIVED):**
1. **Independence at level 1**, by KJ13's manual inspection (§2.2: run-sequence, lag plots 1–4 and ACF against randomly reordered copies) over the 30-round passes. The automated checks of §9.3 guard real runs, but this inspection is the authority.
2. **The step length.** Record each round's throughput in sub-round slices (for example tenths of the step). Aggregating slices gives, from the same data, the variance and lag-1 autocorrelation at shorter and longer steps. Choose the shortest step whose rounds pass the independence check and that carries the transfers the reported quantiles need (note 11 §13.3). A longer step averages bursts inside a round, which, in KJ13's words, "redefines what is measured".
3. **The changepoint penalty β for §9.3 step 2.** Permute each pass's rounds, which gives KJ13's randomly reordered reference, and take the smallest β for which at most 5% of permutations produce a changepoint. KFE12's SIC value (p log n) is the comparison point; BBKMT17 needed 15 log n against a typical 4 log n.
4. **The dip test's behaviour on this device.** Estimate the false-rejection rate on points judged unimodal, and the power for the two-state shapes found, by resampling from the measured states. No published power exists below n = 50 (§5.3).
5. **Re-dimension** when the device, OS or benchmark changes (KJ13 §9.3).

### 9.3 Per point: the decision procedure

Inputs: a point's rounds in time order, `x_1 … x_n` (throughput per operation: put, get, file layer), each with its operation count and elapsed time. Each step cites its basis; the assembly is DERIVED.

1. **Independence** (§4). Two checks:
   - the lag-1 autocorrelation r₁ (NIST §1.3.5.12) lies outside ±1.96/√n (LB10 p. 41; NIST §1.3.3.1); or
   - the number of runs above and below the median lies in the exact two-sided 5% region of §4's table.

   Either marks the rounds dependent. Two checks at 5% each flag independent rounds up to about 10% of the time (DERIVED); the dimensioning run measures the actual rate on permuted rounds. Below 10 rounds the runs test has no rejection region; the report then says `independence not testable at n`, and the dimensioning run's verdict for the point stands in.
2. **Dependent: states in time** (§3).
   - Segment with PELT (KFE12). Use twice the negative normal log-likelihood with changes in mean and variance as the cost (BBKMT17 §4.2), a minimum segment length of 6 rounds so that every segment can carry a 95% median interval (LB10 Table A.1), and β from the dimensioning run. For n ≤ 100, exhaustive optimal partitioning is as cheap and gives the same minimum.
   - Treat segments whose 95% median intervals overlap as equivalent. This replaces BBKMT17's tolerance, the larger of 0.001 s and the final segment's variance, which they chose as "a good heuristic for this cumulative effect" of external events [BBKMT17 §4.3, PDF p. 11]. HB15 §3.2 says non-overlapping intervals imply a difference.
   - Classify as BBKMT17 do, with higher throughput in place of shorter time. No steady state: the final segment begins within the last quarter of the rounds and the segment before it is not equivalent (BBKMT17's proportion, the last 500 of 2000 iterations, which they chose "somewhat arbitrarily"). Otherwise flat if all segments are equivalent; slowdown if some earlier segment, not equivalent to the final one, is faster; warmup if some earlier segment is slower and none is faster.
   - Report every segment: its first and last round, median and interval. Report the final segment as the point's result only if the class is flat or warmup. Otherwise report `no steady state within n rounds`, as SNIA-PTS §7.3 requires, and follow KJ13 §6.2: repeat the point at a higher level (passes or processes) and take the same round index from each repetition.
3. **Independent: states in distribution** (§5).
   - Compute the dip `D_n` with HH85's order-n algorithm (§5.1). The scale must be the table's: a correct implementation returns 1/(2n) for n ≤ 3 (DIPTEST). Implement from the paper: AS217's Fortran is described as buggy, and DIPTEST's C is GPL, which `cargo deny check licenses` would have to admit.
   - If `D_n > D_crit(n, 0.05)` (§9.4), the rounds form more than one state. Otherwise report the point as unimodal, together with the smallest minority share the test could have seen (§9.4, last column).
   - Split: sort, and cut between the consecutive order statistics that minimize the within-group sum of squares (§5.9). Stop at two states: testing a state of tens of rounds for a third mode fails the power bound of §5.3.
   - Never offer a separation index of the split (Δμ, "D") as evidence (§5.8).
4. **Intervals** (§6).
   - A unimodal point, and every state or segment with at least 6 rounds: the median, with the order-statistic interval `[x_(j), x_(k)]` of LB10 Theorem 2.1. Use the exact binomial j and k (Table A.1 for 6–70; the approximation from 71), at 95%, and at 99% from 8 rounds. Do not use the rank approximation of HB15 and MDJ18 at these sizes (§6.2).
   - A state with fewer than 6 rounds: its count, share, minimum and maximum, and no interval (LB10 p. 33).
   - Shares: k of n rounds, with LB10 Theorem 2.4's exact interval (p. 39). This ignores that the split came from the same data, so the interval is optimistic (DERIVED).
   - The point's aggregate rate, Σops/Σelapsed over its rounds (HB15 Rule 3 on counts). It describes the long-run mixture only in the proportions this environment produced. Its interval comes from the top level (Eq. 4 over process means). Within one process it has no interval that covers process-level variation.
5. **Stopping.** Rounds continue until n ≥ n_min (§9.5) and each reported interval's relative half-width, `max(m − x_(j), x_(k) − m)/m` for median m, is at most ε, or until n = n_max from the dimensioning budget. The report says which ended the point, as GBE07 and note 11 §13.4 do. Stopping on width is justified only asymptotically (CHR65), which is one more reason for n_min.

### 9.4 The dip test's critical values, n = 4–100

Rows marked T are DIPTEST's tabulated quantiles (N = 1,000,001 per n; HH85's values are in §5.2). Rows marked I are interpolated linearly in n on √n·D between the tabulated sizes, as HH85 Table 1 note (4) and DIPTEST's `dip.test` do (DERIVED). "Minority rounds" is the smallest count k with k/n > 2·D_crit(n, 0.05), the necessary condition of §5.3 (DERIVED, tight states).

| n | D_crit α = 0.05 | D_crit α = 0.01 | | minority rounds |
|---|---|---|---|---|
| 4 | 0.2073 | 0.2318 | T | 2 |
| 5 | 0.1864 | 0.1965 | T | 2 |
| 6 | 0.1648 | 0.1919 | T | 2 |
| 7 | 0.1599 | 0.1841 | T | 3 |
| 8 | 0.1540 | 0.1730 | T | 3 |
| 9 | 0.1466 | 0.1642 | T | 3 |
| 10 | 0.1396 | 0.1597 | T | 3 |
| 11 | 0.1342 | 0.1536 | I | 3 |
| 12 | 0.1296 | 0.1483 | I | 4 |
| 13 | 0.1255 | 0.1437 | I | 4 |
| 14 | 0.1219 | 0.1396 | I | 4 |
| 15 | 0.1188 | 0.1360 | T | 4 |
| 16 | 0.1155 | 0.1323 | I | 4 |
| 17 | 0.1125 | 0.1290 | I | 4 |
| 18 | 0.1099 | 0.1259 | I | 4 |
| 19 | 0.1074 | 0.1232 | I | 5 |
| 20 | 0.1051 | 0.1206 | T | 5 |
| 21 | 0.1029 | 0.1181 | I | 5 |
| 22 | 0.1008 | 0.1157 | I | 5 |
| 23 | 0.0988 | 0.1135 | I | 5 |
| 24 | 0.0970 | 0.1115 | I | 5 |
| 25 | 0.0953 | 0.1095 | I | 5 |
| 26 | 0.0937 | 0.1077 | I | 5 |
| 27 | 0.0922 | 0.1060 | I | 5 |
| 28 | 0.0908 | 0.1044 | I | 6 |
| 29 | 0.0894 | 0.1029 | I | 6 |
| 30 | 0.0882 | 0.1015 | T | 6 |
| 31 | 0.0869 | 0.1000 | I | 6 |
| 32 | 0.0856 | 0.0986 | I | 6 |
| 33 | 0.0844 | 0.0972 | I | 6 |
| 34 | 0.0833 | 0.0960 | I | 6 |
| 35 | 0.0822 | 0.0947 | I | 6 |
| 36 | 0.0812 | 0.0936 | I | 6 |
| 37 | 0.0802 | 0.0924 | I | 6 |
| 38 | 0.0792 | 0.0914 | I | 7 |
| 39 | 0.0783 | 0.0903 | I | 7 |
| 40 | 0.0775 | 0.0893 | I | 7 |
| 41 | 0.0766 | 0.0884 | I | 7 |
| 42 | 0.0758 | 0.0875 | I | 7 |
| 43 | 0.0750 | 0.0866 | I | 7 |
| 44 | 0.0743 | 0.0857 | I | 7 |
| 45 | 0.0736 | 0.0849 | I | 7 |
| 46 | 0.0729 | 0.0841 | I | 7 |
| 47 | 0.0722 | 0.0833 | I | 7 |
| 48 | 0.0715 | 0.0826 | I | 7 |
| 49 | 0.0709 | 0.0819 | I | 7 |
| 50 | 0.0703 | 0.0812 | T | 8 |
| 60 | 0.0645 | 0.0745 | I | 8 |
| 70 | 0.0601 | 0.0694 | I | 9 |
| 80 | 0.0565 | 0.0653 | I | 10 |
| 90 | 0.0536 | 0.0619 | I | 10 |
| 100 | 0.0511 | 0.0590 | T | 11 |

For n outside the table, or to use a different null than the uniform (HH85 pp. 81–82), a critical value is the (1 − α) quantile of the dips of many samples of size n drawn from the null distribution, as HH85 (9,999 samples) and DIPTEST (1,000,001) computed theirs. The dimensioning run can compute such values once per n it uses.

### 9.5 Rounds needed

| Purpose | Rounds | Basis |
|---|---|---|
| a 95% median interval exists (99%) | 6 (8) | LB10 Table A.1; HB15 §4.2.2 "n > 5" |
| the runs test has a two-sided 5% rejection region | 10 | §4 table (DERIVED) |
| a sample lag-1 autocorrelation of 0.5 exceeds the 95% bound | 16 (1.96/√16 = 0.49) | LB10 p. 41 bound (DERIVED) |
| the dip test can flag a minority state of share p, tight states, α = 0.05 (α = 0.01) | p = 0.5: 4 (4); 1/3: 6 (9); 0.3: 9 (12); 0.25: 14 (19); 0.2: 23 (31); 0.15: 44 (60); 0.1: 105 (143) | §5.3, §9.4 (DERIVED) |
| each of two states carries its own 95% interval | about 6/p for minority share p: p = 0.3 → 20, p = 0.2 → 30 | LB10 (DERIVED) |
| the mixture likelihood ratio (not recommended) | ≥ 50 | ABZ94 §3 |
| CONFIRM's smallest resample | 10 | MDJ18 §5 |
| dimensioning run | 20–30 at the top level, 10 or more below; ≥ 5 top-level repetitions for its variance | KJ13 §9.1, §11 |

So a point needs about 20–30 rounds to test independence, look for a minority state of 20–30% of the rounds, and give each state an interval. Six rounds, `Rounds::STANDARD`'s maximum, support a 95% median interval, `[min, max]` at 0.969 (LB10 Table A.1), and none of the checks. At 3 s per round, 16 points × 30 rounds take about 24 min, against about 5 min at six rounds (DERIVED). The dimensioning run replaces these defaults with measured n_min and n_max.

### 9.6 What a point's report carries

For each operation of each point: n rounds; the independence verdict (r₁ against its bound, R against its region, or `not testable`); the class (unimodal, two states, or segmented as flat, warmup, slowdown or no steady state); `D_n` against `D_crit(n)`; for each state or segment its share (with interval), median `[x_(j), x_(k)]` or range, and round span for segments; the aggregate rate; the smallest minority share the dip test could have seen; and whether the top level (processes) was repeated. A single mean with a coefficient of variation is not reported for a point with two states (HB15 Fig. 3; MDJ18 §5; note 11 §16.3).

### 9.7 What changes in `calibrate.rs` and `bench.rs` (DERIVED)

- `Rounds::STANDARD` (min 3, max 6, ±5%) cannot run the independence or modality checks. n_min comes from §9.5 and n_max from the dimensioning budget.
- `relative_interval`, the Student-t half-width of the mean, assumes normal rounds (HB15 Rule 6); throughput rounds generally are not (MDJ18). Use the order-statistic interval of the median, and keep the t table (`T95`) for the top-level interval of Eq. 4.
- Existing six-round results can already be restated as medians with `[min, max]` as their 95% interval.
- The round loop interleaves put, get and file-layer steps, and each round's puts change the device state. The independence check will show whether this makes rounds dependent. If it does, the dimensioning run decides between a settle phase before measuring and treating states in time as §9.3 step 2 does.

### 9.8 Uncertain

- The uniform is least favourable only asymptotically, and for the class of Theorem 5; HH85 "speculate" beyond it. The dip test's level at n ≤ 30 for skewed unimodal throughput has not been measured here. The dimensioning run should check it by resampling (§9.2 item 4).
- The minority-share bound of §5.3 is exact only for tight states; it is a necessary condition, not power.
- A PELT segmentation of tens of rounds with a permutation-calibrated penalty is this note's construction. No source validates it at this size.
- Share intervals ignore that the split was chosen from the same rounds.
- Whether the stall bursts are independent of round boundaries is not measured. If they cluster over seconds, bursts create states in time, not in distribution.

---

## 10. Unverified items and corrections to other documents

**Not obtained or not read.**
- AS217: not read; only DIPTEST's account of its bug.
- FIS58: not accessible; no abstract available. Nothing is quoted from it.
- EH69: not read; described by HH85 p. 70.
- Antoch et al. 1997 (cited by BBKMT17 for the penalty's effect under dependence), Knapp 2007 (cited by PSJ13), Everitt & Hand 1981 (cited by ABZ94), York 1998 (cited by CH98), Mendenhall 1982 (cited by NIST): not read.
- The exact run-count distribution in §4 is derived here; the classical tables (Swed–Eisenhart, Wald–Wolfowitz) were not read.
- JK13: abstract only.
- ACM versions of KJ13, BBKMT17, HB15 and CKZ09 were not compared with the copies read. KJ13 was read from a mirror of the Kent Academic Repository file because the repository returned 504. BBKMT17 and KFE12 were read as arXiv versions, AAC19 as its arXiv version.
- GBE07: not re-read; cited through note 11.
- The "Ashman's D" with unequal variances and "D > 2": origin not found; not in ABZ94.

**Corrections and additions to repository documents** (INFERENCE, for their owners):
- Note 11 §16.3's instruction to report bimodal results per state now has sources: HB15 §3.1.3 and Fig. 3, MDJ18 §5, BBKMT17 §4.4, and CR16 §IV.B (non-peer-reviewed). The method is §9 here.
- Note 11 §16.3's `n ≥ (t·CV/ε)²` is HB15 §4.2.2's formula and holds for normally distributed data. For unknown distributions HB15 recommend recomputing a nonparametric interval as measurements accrue.
- The rounds measurement (2026-09-29), finding 2, "A spread of 0.3 needs about 140 rounds for ±5%", applies that normal-data formula to reads the same note describes as bursty. Per-state intervals may need fewer rounds; untested.
- `calibrate.rs`'s `Rounds::STANDARD` comment ("six are enough for their minimum and maximum to bracket the median 95% of the time") agrees with LB10 Table A.1 (n = 6: `[x(1), x(6)]`, 0.969).
- `bench.rs`'s module documentation states the stop rule as "within ±5% at 95% confidence", which is the t-interval of the mean. §9.3 replaces it with the median's order-statistic interval, after the independence and modality checks.
