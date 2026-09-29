# 04 — Erasure coding, placement, repair, and tail-tolerant reads

| | |
|---|---|
| **Status** | Research input for design (not a decision record) |
| **Lookup date** | 2026-09-28 (all crate versions, repo activity, and web documents are "as of" this date) |
| **Scope** | Part A: literature on RS/LRC coding, repair, availability, placement, load balancing, tail latency. Part B: Rust crates for RS, CRC32C/CRC32/CRC64-NVME, MD5, SHA-256. Final section: recommendations for mantle. |

## How to read this document

**Method.** Every paper claim was checked against the primary PDF. PDFs came from USENIX, VLDB, arXiv, the CACM author copy, or the authors' own pages. Text was extracted with `pdftotext` and searched for exact wording. Section numbers, table numbers, and figure numbers come from the PDFs themselves. Volume, issue, page, and DOI metadata comes from the Crossref API or from the page footers of the USENIX PDFs. Crate facts come from the crates.io API, the published `.crate` sources (cited as `file:line` against the named version), and the GitHub API. Nothing was taken from blogs.

**Labels.**

- **[V]** means verified from the primary source cited next to it. Quotes are verbatim.
- **UNVERIFIED** means I could not confirm the item from a primary source. The reason is given each time.
- **[Analysis]** means my own reasoning or arithmetic. It is not a claim made by the cited paper.

**Notation.** Papers disagree on notation, so this document normalizes all codes to **RS(k, m)**: *k* data chunks, *m* parity chunks, and *n = k + m* chunks in total. The papers' own notations map onto it as follows:

- Tectonic "RS(r, k)" = r data + k parity, so Tectonic RS(9,6) is 9+6 = 15 chunks.
- Ford et al. "RS(r, s−r)" = r data + (s−r) parity.
- f4 "Reed-Solomon(n, k)" = n data + k parity.
- Azure LRC "(k, l, r)" = k data, l local parities, r global parities.

---

## 0. Executive summary

| Question | Recommendation (details in §R) | Strongest evidence |
|---|---|---|
| Which codes? | Replicate open blocks with R=3 and quorum appends, then re-encode on seal. Which profiles are allowed depends on the number of failure domains (one chunk per domain): **RS(6,3)** for 9–11 domains; **RS(8,4)** as the default for ≥ 12; **RS(10,4)** as an opt-in capacity profile for ≥ 14, and only with copyset placement; **RS(9,6)** as an opt-in durable profile for ≥ 15. R3 only below 9 domains. LRC(12,2,2) in v2. | WAS §1 and Tectonic §5.2 (replicate, then encode on seal). Ford Table 3 under correlated failures: RS(8,4) 5.11E7 d > RS(6,3) 1.03E7 d > R=3 6.82E6 d > RS(9,4) 2.39E6 d. f4 and Tectonic run RS(10,4); Tectonic runs RS(9,6) for long-lived data. |
| Chunk layout | Use contiguous RS chunks, not striped. Most reads are then served directly from one chunk. Cap reconstruction reads with a budget. | Tectonic §6.4: reads "usually direct", reconstruction reads "10× more IOs", reconstructed reads capped "to 10% of all reads". |
| Placement | Record placement explicitly in metadata, as Tectonic does, rather than computing it (CRUSH). Put at most one chunk per failure domain (rack). Keep upgrade domains separate. Constrain stripes to copysets built from about 10–100 consistent shuffles. Pick among candidates with power-of-two-choices. | Pan §3.3/§3.5; Ford §5.2 (rack-aware ≈3× stripe MTTF); Cidon 2013 (99.99% → 0.15%); Mitzenmacher 2001/2000. |
| Repair | Use a priority queue ordered by remaining redundancy. Delay repair for transient failures, but not for stripes with little margin left. Detect failures fast. Rate-limit repair against client I/O. Verify every repair with CRCs. | Ford §5.1 ("prioritize reconstruction of stripes which have lost the most chunks"), §8.2, §10; Huang §3.1.1, §4.4. |
| Hedged reads | Hedge at the p95 latency of the request class. Replicated data is hedged to a replica, or with a tied request. RS data is hedged with a budgeted reconstruction read. Multi-chunk reads fetch k+1 chunks and use the first k, except when bandwidth-bound. | Dean & Barroso ("95th-percentile", 1,800 ms → 74 ms at +2% requests; tied requests −16% median / −38% p99.9); Huang §5.1–5.2; EC-Cache; Tectonic 10% cap. |
| Crates | RS: `reed-solomon-simd` 3.1.0. CRC32C, CRC32 and CRC64-NVME: `crc-fast` 1.10.0. MD5: `md-5` 0.11.0. SHA-256: `sha2` 0.11.0, or `aws-lc-rs` if it is already in the dependency tree. | See Part B; justification in §R5. |

---

# Part A — Literature

## A1. Reed–Solomon coding for storage: foundations

### A1.1 Plank, "A Tutorial on Reed-Solomon Coding for Fault-Tolerance in RAID-like Systems" (1997)

**Citation [V].** J. S. Plank. *Software—Practice & Experience* 27(9):995–1012, September 1997. DOI 10.1002/(SICI)1097-024X(199709)27:9<995::AID-SPE111>3.0.CO;2-6 (Crossref). I read the text from the technical-report version, UT CS-96-332 (`web.eecs.utk.edu/~jplank/plank/papers/CS-96-332.pdf`). Its math glyphs do not extract, so only the prose is quoted below.

**Key content [V]**

- The TR now opens with this banner: *"IMPORTANT The information dispersal matrix A given in this paper does not have the desired properties. Please see Technical Report CS-03-504 for a correction to this problem."* (p. 1)
- The algorithm has three parts: *"using the Vandermonde matrix to calculate and maintain checksum words, using Gaussian Elimination to recover from failures, and using Galois Fields to perform arithmetic."* (p. 5, "Overview of the RS-Raid Algorithm")
- **The flawed claim** (p. 6): *"Because matrix [A] is defined to be a Vandermonde matrix, every subset of rows of matrix is guaranteed to be linearly independent."* This does not hold for the identity-on-top matrix the tutorial builds. See A1.2.
- Arithmetic uses two logarithm tables, `gflog` and `gfilog`, when w is "small (16 or less)" (p. 7). The paper warns against doing arithmetic "over the integers modulo 2^w" (p. 6).
- Updates are applied as deltas: when data word d_j changes, every checksum word is adjusted. The paper cites Gibson's "update penalty" and states that RS-Raid's penalty is m disks, "the minimum value for tolerating m failures" (p. 11).
- **UNVERIFIED:** the exact bound the tutorial states on n + m relative to w, because the glyphs do not extract. The peer-reviewed restatement in A1.4 is: *"w must be large enough that n ≤ 2^w + 1"*, and *"Most implementations choose w = 8, since their systems contain fewer than 256 disks"* (Plank et al. FAST'09 §2.1).

### A1.2 Plank & Ding, "Note: Correction to the 1997 Tutorial on Reed-Solomon Coding" (2003/2005)

**Citation [V].** J. S. Plank and Y. Ding. Technical Report UT-CS-03-504, April 24, 2003. Journal version: *Software—Practice & Experience* 35(2):189–194, February 2005, DOI 10.1002/spe.631 (Crossref). Text read from `web.eecs.utk.edu/~jplank/plank/papers/CS-03-504.pdf`.

**Key content [V]**

- §1: *"The tutorial as published presented an information dispersal matrix, which does not have the properties claimed – that the deletion of any m rows results in an invertable n×n matrix."*
- §3, "A Correct Information Dispersal Matrix", lists the required properties: the matrix is (n+m)×n; *"The n×n matrix in the first n rows are the identity matrix"*; and *"Any submatrix formed by deleting m rows of the matrix is invertible."*
- **The fix.** Start from an (n+m)×n Vandermonde matrix and apply elementary column operations: swap two columns, scale a column by a nonzero element, or add a multiple of one column to another. Repeat until the top n rows form the identity. The justification: *"elementary matrix operations do not change the rank of a matrix."* The note works a full example over GF(2^4) with n = m = 3.
- §2 points readers to Cauchy RS (Blömer et al.) and Tornado codes as alternatives.

**Implications for mantle [Analysis]**

1. Correctness of the code depends on how the generator matrix is constructed. It is not a property of "RS" in the abstract. Any codec mantle adopts has to be proven MDS for every (k, m) mantle ships. **CI should decode every erasure pattern of up to m losses** for each shipped profile. That is only C(14,4) = 1001 patterns for RS(10,4) and C(15,6) = 5005 for RS(9,6), so it is cheap.
2. Parity bytes depend on the generator matrix, the field, and the basis. Two "RS(10,4)" libraries almost never produce the same parity. The **codec identity must therefore be recorded in block metadata**: library, field, construction, and version. The stored parity is only meaningful relative to that codec.

### A1.3 Blömer, Kalfane, Karp, Karpinski, Luby, Zuckerman, "An XOR-Based Erasure-Resilient Coding Scheme" (1995)

**Citation [V, from a reference list].** ICSI Technical Report TR-95-048, International Computer Science Institute, Berkeley, August 1995. This is exactly as cited in Plank & Ding ref. [1].

**UNVERIFIED (primary text).** The PDF could not be retrieved. The ICSI FTP and web mirrors return errors, and no author copy was found. The construction is therefore described here from a peer-reviewed secondary source, Plank et al. FAST'09 §2.2 [V]:

- *"CRS codes [6] modify RS codes in two ways. First, they employ a different construction of the Generator matrix using Cauchy matrices instead of Vandermonde matrices. Second, they eliminate the expensive multiplications of RS codes by converting them to extra XOR operations."*
- Each GF(2^w) element becomes a w×w bit matrix, so *"G^T [goes] from a n × k matrix of w-bit words to a wn × wk matrix of bits"*.
- Strips are split into w packets, and *"the performance of CRS coding is directly related to the number of ones in G^T"*.

**Implication [Analysis].** Every square submatrix of a Cauchy matrix is invertible, which makes Cauchy constructions MDS by design and sidesteps the A1.2 pitfall. The bit-matrix/XOR form is what the Azure paper means by *"a transformation that enables the use of XOR operations exclusively"* (Huang §4.4).

### A1.4 Plank, Luo, Schuman, Xu, Wilcox-O'Hearn, "A Performance Evaluation and Examination of Open-Source Erasure Coding Libraries for Storage" (FAST 2009)

**Citation [V].** *7th USENIX Conference on File and Storage Technologies (FAST '09)*, pp. 253–265. PDF: `usenix.org/legacy/event/fast09/tech/full_papers/plank/plank.pdf`.

**Key content [V]**

- **Scope** (§1): five open-source implementations of five codes — classic RS, Cauchy RS, EVENODD, RDP, and Minimal-Density RAID-6.
- **Main results**, verbatim from p. 253:
  - *"The special-purpose RAID-6 codes vastly outperform their general-purpose counterparts. RDP performs the best of these by a narrow margin."*
  - *"Cauchy Reed-Solomon coding outperforms classic Reed-Solomon coding significantly, as long as attention is paid to generating good encoding matrices."*
  - *"An optimization called Code-Specific Hybrid Reconstruction [14] is necessary to achieve good decoding speeds in many of the codes."*
  - *"Parameter selection can have a huge impact on how well an implementation performs. Not only must the number of computational operations be considered, but also how the code interacts with the memory hierarchy, especially the caches."*
  - *"Of the five libraries tested, Zfec [33] implemented the fastest classic Reed-Solomon coding, and Jerasure [26] implemented the fastest versions of the others."*
- **Machines** (§4.1, p. 257): a MacBook with a 2 GHz Core Duo (memcpy 6.13 GB/s, XOR 2.43 GB/s) and a Dell Pentium 4 at 1.5 GHz (memcpy 2.92 GB/s, XOR 1.32 GB/s). Both were single-core runs.
- **Packet size** (§4.4): *"lower packet sizes have less tight XOR loops, but better cache behavior. Higher packet sizes perform XORs over larger regions, but cause more cache misses."*
- **Conclusions** (§7, p. 263):
  - *"Note that w ∈ {8, 16, 32} are all bad for CRS coding."*
  - *"For non-RAID-6 applications, CRS coding performs much better than RS coding, but now w should be chosen to be as small as possible"*
  - Finding a good packet size *"takes more effort than executing a simple binary search"*
  - *"Part of Zfec's better performance comes from its smaller memory footprint"*
  - *"The place where future research will have the biggest impact is for larger values of m"*
  - Exploiting multicore is called out as a challenge.

### A1.5 Plank, Greenan, Miller, "Screaming Fast Galois Field Arithmetic Using Intel SIMD Instructions" (FAST 2013)

**Citation [V].** *11th USENIX Conference on File and Storage Technologies (FAST '13)*, pp. 299–306. Author copy: eScholarship `qt1vr1629w` (the USENIX file URL returned HTML). The library is GF-Complete.

**Key content [V]**

- p. 299: historically, *"the performance of multiplication is at least four times slower than XOR"*. The paper's implementations are *"2.7 to 12 times faster than other implementations of Galois Field arithmetic."*
- §5 (p. 301): *"mm_shuffle_epi8(a, b) is the real enabling SIMD instruction for Galois Fields … it returns a 128-bit vector composed of 16 simultaneous table lookups"*. For w = 4, *"six instructions suffice for the 32 multiplications."*
- §6, GF(2^8): split each byte into two 4-bit halves and do two 16-entry lookups (the "left-right table"), using the same six instructions. There are only 256 possible multipliers y, so the tables can be precomputed. The paper notes the technique *"was documented in assembly code by Anvin … Linux kernel."*
- §7, GF(2^16): "Altmap" splits words across two registers. *"The down sides to Altmap are that memory regions are constrained to be multiples of 32 bytes"*. The eight tables total 128 bytes and take "under 200 instructions" to build.
- Evaluation (p. 303, Fig. 6): Intel Core i7-3770 at 3.4 GHz, regions from 1 KB to 1 GB. *"Memcpy() and XOR are limited by the L1 cache; while the SIMD techniques and Anvin's optimization are limited by the L2 cache."*
- p. 304: *"with the SIMD instructions, the cache becomes the limiting factor of multiplication, and I/O becomes the dominant concern with erasure coding."* The techniques *"have been leveraged to sustain throughputs of over 4 GB/s in recent tests of Reed-Solomon coding."* The paper also says AVX2 permutation instructions *"may be leveraged similarly."*

**Implications of A1.x for mantle [Analysis]**

- For mantle's small codes (n ≤ 16), GF(2^8) with split-table shuffle multiply (PSHUFB on x86, TBL on NEON) runs at memory or cache speed. Coding CPU cost will not bottleneck HDD- or NIC-bound paths. The FAST'13 conclusion that I/O dominates is what should drive mantle's design effort: placement, repair traffic, and read amplification.
- A codec's quality is decided by its matrix construction (A1.2) and its memory behaviour (A1.4), not by its asymptotic complexity. Mantle should benchmark at its own chunk sizes, for example 64 KiB–8 MiB per call, and not trust library headline numbers.

---

## A2. Lin, Chung, Han — FFT-based RS (FOCS 2014) and Leopard-RS

### Citation

**[V]** S.-J. Lin, W.-H. Chung, Y. S. Han, "Novel Polynomial Basis and Its Application to Reed-Solomon Erasure Codes," *2014 IEEE 55th Annual Symposium on Foundations of Computer Science (FOCS)*, pp. 316–325, October 2014. DOI 10.1109/FOCS.2014.41 (Crossref). Text from arXiv:1404.3458v2.

Extended journal version [V, Crossref]: S.-J. Lin, T. Y. Al-Naffouri, Y. S. Han, W.-H. Chung, "Novel Polynomial Basis With Fast Fourier Transform and Its Application to Reed–Solomon Erasure Codes," *IEEE Trans. Inf. Theory* 62(11):6284–6299, November 2016. DOI 10.1109/TIT.2016.2608892.

### Key content [V]

- **Abstract.** *"h-point polynomial evaluation can be computed in O(h log2(h)) finite field operations with small leading constant"*. It builds *"encoding and erasure decoding algorithms for the (n = 2^r, k) Reed-Solomon codes … the encoding can be completed in O(n log2(k)) finite field operations, and the erasure decoding in O(n log2(n))"*. The authors say it is *"the first approach supporting Reed-Solomon erasure codes over characteristic-2 finite fields while achieving a complexity of O(n log2(n)), in both additive and multiplicative complexities."*
- **Constraints as stated.**
  - The transform in §III is defined *"for h a power of two."*
  - Encoding (§V, Alg. 1) is systematic and computes parity in n/k − 1 blocks of size k.
  - Footnote 2 reads: *"Since k and n are both powers of 2, n is divisible by k."*
  - Erasure decoding (Alg. 2) uses a formal derivative. It is erasure-only.
- **Measurements** (§VI.B), on an Intel i7-950 with n = 2^16 and k/n = 1/2: *"about 1.12 seconds to generate a codeword, and 3.06 seconds to decode"*. This is *"around 17 times faster"* than Didier's O(n lg² n) method.
- §VIII: *"all the transforms … can be easily implemented in parallel processing."*

### Leopard-RS: the practical instantiation

Source: `github.com/catid/leopard`, BSD-3-Clause, pushed 2026-09-17, not archived.

The README was substantially rewritten in 2026 around "Leopard2". I read the README, `leopard.h`, and `Benchmarks.md` from `master` [V]:

- **Legacy API constraints.** `leopard.h` lines 161–164: *"The sum of original_count + recovery_count must not exceed 65536. The recovery_count <= original_count. The buffer_bytes must be a positive multiple of 64"*. Errors come back as `LeopardResult` codes such as `Leopard_InvalidSize` and `Leopard_InvalidCounts`.
- **The Leopard2 README** says:
  - *"Leopard1's public API accepted only the high-rate shape R <= K."* Leopard2 *"adds a low-rate profile for R > K"*.
  - *"Both profiles accept positive, non-power-of-two K and R through shortening and puncturing; the maximum transmitted code length is unchanged at K + R <= 65536."*
  - *"Field AUTO uses GF8 for parents through 256 coordinates and GF(2^16) for larger parents, up to 65,536 coordinates."*
  - *"Native GF16 requires complete two-byte symbols"*.
  - Dispatch is at runtime across SSSE3, AVX2, AVX-512VL and GFNI, with no `-march=native`.
- **Where O(n log n) pays off** (author benchmark, `Benchmarks.md`). The author labels it informal: *"I'm not being very rigorous here"*. On an AVX2 laptop with k = m = 128 and 64 KB chunks:
  - Leopard encodes at 1964.98 MB/s and decodes at 600.542 MB/s.
  - CM256, an O(N²) Cauchy codec, runs at about 100 MB/s. The author's note on it: *"This type of software gets slower as O(K*M) … It is practical for either small data or small recovery set up to 255 pieces."*
  - I found no primary source that gives a crossover point for small n.

### Implications for mantle [Analysis]

- The FFT approach helps when n is in the hundreds or more, for example wide archival codes or network-coding-style uses. Mantle's profiles have n ≤ 16. At that size the FFT's asymptotic advantage mostly disappears. The reed-solomon-simd README confirms this: that crate is slower at decoding *"with about 42 or fewer recovery shards"* (B1.1).
- The FFT codes are still MDS and systematic. Leopard-style libraries also bring well-engineered runtime SIMD dispatch, and that can matter more than the algorithm at small n.
- Leopard-family codecs use a Cantor/"novel" basis. Their parity is **not** byte-compatible with Vandermonde or Cauchy codecs such as ISA-L, Jerasure or klauspost. This reinforces the rule from A1.2: record the codec ID in block metadata.
- Leopard's legacy constraint R ≤ K is harmless for mantle, because every profile has m ≤ k.

---

## A3. Locally repairable codes: Windows Azure Storage LRC and "XORing Elephants"

### A3.1 Huang et al., "Erasure Coding in Windows Azure Storage" (USENIX ATC 2012)

**Citation [V].** C. Huang, H. Simitci, Y. Xu, A. Ogus, B. Calder, P. Gopalan, J. Li, S. Yekhanin. *2012 USENIX Annual Technical Conference (ATC '12)*. PDF: `usenix.org/system/files/conference/atc12/atc12-final181_0.pdf`. **UNVERIFIED:** proceedings page range, because this PDF has no page footers.

**Construction and properties [V]**

- **Definition** (§2): *"A (k, l, r) LRC divides k data fragments into l groups, with k/l data fragments in each group. It computes one local parity within each group. In addition, it computes r global parities from all the data fragments … n = k + l + r … the normalized storage overhead is n/k = 1 + (l + r)/k."*
- **Maximally Recoverable (§2.2).** Coefficients are chosen so the code *"can decode any failure pattern which is information-theoretically decodable."* The paper also states: *"LRC is not Maximum Distance Separable"*. §2.2.1 constructs the coding equations.
- **§2.2.2, "Putting Things Together"** (for the running (6,2,2) example): *"the (6, 2, 2) LRC is capable of decoding arbitrary three failures. It can also decode all the information-theoretically decodable four failure patterns, which accounts for 86% of all the four failures."* **Note:** the 86% figure is for (6,2,2). The paper gives no four-failure fraction for (12,2,2) (UNVERIFIED for (12,2,2)).
- **§2.2.3** gives a decodability check. For each local group whose parity survived, swap that parity for one erased data fragment. Then the pattern is decodable if the remaining erasures number no more than the global parities.
- **§2.4, Summary:** *"Any single data fragment failure can be decoded from k/l fragments within its local group … It tolerates up to r + 1 arbitrary fragment failures. It also tolerates failures more than r+1 (up to l+r), provided those are information-theoretically decodable … Among all the codes that can decode single data fragment failure from k/l fragments and tolerate r + 1 failures, LRC requires the minimum number of parities."*

**Reliability model [V]** (§3.1, Fig. 3)

- It is a Markov chain that *"focus[es] on independent failures"*.
- The single-failure repair rate is ρ₉ = (M−1)B/(SC). The paper notes that *"the repair rates beyond single failure are dominated by the time taken to detect failure and trigger repair"*, so it sets ρ₈ = ρ₇ = ρ₆ = 1/T.
- For (6,2,2) the average repair cost is C = 3.6.
- Parameters: M = 400 nodes, S = 16 TB, B = 1 Gbps, ε = 0.1, T = 30 min.
- **Table 1, MTTF in years:** 3-replication 3.5×10⁹; RS(6,3) 6.1×10¹¹; LRC(6,2,2) 2.6×10¹².

**Parameter choice [V]** (§3.2–3.3, Figs. 4–5)

- Stripe width is capped by the number of fault domains: *"Since each fragment has to place on a different fault domain, the number of fault domains in a cluster limits the total number of fragments in the code. We use 20 as the limit here, since our storage stamps (clusters) have up to 20 fault domains."*
- Only parameter sets with MTTF at or above 3-replication are kept.
- **Against RS(6,3):**
  - LRC(12,4,2) keeps the 1.5× overhead and cuts single-fragment reconstruction reads from 6 to 3, *"a reduction of 50%"*.
  - LRC(12,2,2) keeps the cost at 6 reads and lowers overhead *"from 1.5x to 1.33x"*.
- §1 explains why RS(12,4) was rejected at 1.33×: reconstruction would need 12 reads, which *"greatly increases the chance of hitting a hot storage node"*.

**Implementation in WAS [V]**

- **§1, lifecycle.** Extents are *"replicated three times … Once reaching a certain size (e.g., 1 GB), extents are sealed … WAS then erasure codes a sealed extent lazily in the background, and once the extent is erasure-coded the original 3 full copies … are deleted."* Fragments can be offline *"for seconds to a few minutes due to an upgrade."*
- **§4.3, placement.** Placement *"takes into account two factors: i) load, which favors less occupied and less loaded extent nodes; ii) reliability, which avoids placing two fragments (belonging to the same erasure coding group) into the same correlated domain … fault domain and upgrade domain … Upgrade domains are typically orthogonal to fault domains."*
  - *"A WAS stamp consists of 20 racks. For maximum reliability, each of the total 16 fragments for an extent is placed in a different rack."*
  - With 10 upgrade domains ("at most 10% … offline"), (12,2,2) uses 9 upgrade domains. The data fragments x_i and y_i share upgrade domain i, the two local parities share one domain, and each global parity gets a domain to itself. Result: *"when one upgrade domain is taken offline, every single data fragment can still be accessed efficiently."*
- **§4.4, designing for EC.**
  - All I/O types are throttled and scheduled: *"Every EN keeps track of its load at the network ports and on individual disks to decide to accept, reject, or delay I/O requests"*.
  - Erasure coding must keep up with ingest.
  - Reconstruction reads ahead in units of up to 5 MB and caches up to 256 MB.
  - **Consistency.** *"each append block contains a header with CRC of the data block, which is checked when the data is written and every time data is read. When a particular data read or reconstruction operation fails due to CRC checks, the operation is retried using other combinations of erasure-coded fragments."*
  - After encoding, several decoding combinations are exercised in memory: one local reconstruction per group, then reconstructions using one global parity and two, three and four data fragments. Finally, *"the coordinator EN performs a CRC of all of the final data fragments and checks that CRC against the original CRC of the full extent … This last step ensures we have not used data that might become corrupted in memory during coding operations."*
  - For arithmetic, WAS uses precomputed tables plus the XOR-only transformation with XOR scheduling.
- **§5.1, small I/O (4–64 KB) on a heavily loaded cluster.**
  - Direct read: 91 ms.
  - RS(12,4) reconstruction from a random 12 of the 15 surviving fragments: 305 ms, *"because it is determined by the slowest fragment among the entire selection."*
  - The fix: *"selecting more (k') fragments and decoding from the first k arrivals … very effective in weeding out slow fragments"*. RS reading 13 fragments took 151 ms.
  - LRC reading 6 fragments took 166 ms.
- **§5.2, large I/O (4 MB).**
  - Direct read: 99 ms. RS reconstruction: 893 ms ("9 times slower"). LRC: 418 ms.
  - The bottleneck was the 1 Gbps NIC. Also: *"aggressive reads with Reed-Solomon using more fragments does not help, but rather hurts latency."*
- **§5.3.** Decode time is 13.2 µs for RS and 7.12 µs for LRC, which is *"orders of magnitude smaller than the transfer time."*
- **§6** cites Ford et al.: *"transient errors in which no data are lost account for more than 90% of data center failures."*
- **§7:** WAS *"chose LRC (12, 2, 2) since it achieves our 1.33x storage overhead target"*. The paper says it gives *"better durability than the traditional approach of keeping 3 copies"* and is laid out *"across the 20 fault domains and 10 upgrade domains."*

### A3.2 Sathiamoorthy et al., "XORing Elephants: Novel Erasure Codes for Big Data" (PVLDB 2013)

**Citation [V].** M. Sathiamoorthy, M. Asteris, D. Papailiopoulos, A. G. Dimakis, R. Vadali, S. Chen, D. Borthakur. *Proc. VLDB Endowment* 6(5):325–336, March 2013. DOI 10.14778/2535573.2488339 (Crossref). PDF: `vldb.org/pvldb/vol6/p325-sathiamoorthy.pdf`.

**Key content [V]**

- **Setting.**
  - A Facebook cluster of *"more than 3000 nodes, 30 PB of logical data"*.
  - HDFS-RAID uses RS(10,4): *"can tolerate any 4 block failures and has a storage overhead of only 40%"*.
- **The repair problem** (§1):
  - *"It is quite typical to have 20 or more node failures per day that trigger repair jobs, even when most repairs are delayed to avoid transient failures."*
  - *"A typical data node will be storing approximately 15 TB and the repair traffic with the current configuration is estimated around 10–20% of the total average of 2 PB/day cluster network traffic."*
  - *"(10,4) RS encoded blocks require approximately 10× more network repair overhead per bit compared to replicated blocks."*
  - *"if 50% of the cluster was RS encoded, the repair network traffic would completely saturate the cluster network links."*
- **Construction** (Fig. 2): LRC(10,6,5) on top of RS(10,4).
  - It adds local parities S₁ over X₁..X₅, S₂ over X₆..X₁₀, and S₃ over P₁..P₄, which would cost 17/10.
  - The coefficients are chosen so that *"S1 + S2 + S3 = 0. We can therefore not store the local parity S3 and instead consider it an implied parity"*. That brings the cost to **16/10 = 1.6×**.
  - Any single block, parity included, is repaired from 5 blocks. For example, *"if P2 is lost, it can be recovered by reading 5 blocks P1, P3, P4, S1, S2"*.
- **Theory.**
  - Lemma 1: MDS codes have locality k.
  - Theorem 2 is the distance–locality bound, d ≤ n − k − ⌈k/r⌉ + 2, extended from linear codes to all codes.
  - Theorem 3: the length-16 code *"has locality 5 for all coded blocks and optimal distance d = 5"*. That is the same distance as RS(10,4), so any 4 failures are tolerated.
- **Results.**
  - *"a reduction of approximately 2× on the repair disk I/O and repair network traffic"*, at the cost of *"14% more storage compared to Reed-Solomon codes"*.
  - Repair duration: *"Xorbas finishes 25% to 45% faster than HDFS-RS"* (§5, EC2 plus a 35-node, 370 TB Facebook test cluster).
- **Table 1 (§4).** It assumes N = 3000, 30 PB, node MTTF of 4 years, 256 MB blocks, and a 1 Gbps cross-rack repair limit, with *"all coded blocks of a stripe … placed in different racks"*. Results:

  | Scheme | Storage overhead | Repair traffic | MTTDL (days) |
  |---|---|---|---|
  | 3-replication | 2× | 1× | 2.3079E+10 |
  | RS(10,4) | 0.4× | 10× | 3.3118E+13 |
  | LRC(10,6,5) | 0.6× | 5× | 1.2180E+15 |

  The paper notes: *"MTTDL assumes independent node failures."*

### Implications for mantle (A3) [Analysis]

- **What LRC buys.** It cuts single-failure repair reads and degraded-read fan-out roughly in half at equal durability, and that is the dominant repair case (98% of repairs are single-block, see A4.3). It does not help multi-failure repair.
- **What LRC costs.** It is not MDS. It needs more failure domains: 16 for (12,2,2). It also needs its own coefficient construction and decodability checker (Azure §2.2.1–2.2.3), which general-purpose RS crates do not provide.
- **Placement requirements.** LRC doubles as a placement tool. Group-aware assignment of upgrade domains keeps every data fragment cheaply readable during rolling upgrades (Azure §4.3). If mantle adds LRC, placement must know which local group each chunk belongs to.
- **Encode-time verification.** Azure's verify-after-encode (§4.4) is cheap in CPU and catches in-memory corruption during coding. Mantle should adopt it for every seal/re-encode, independent of the code chosen.
- **Model limits.** Both papers' reliability numbers assume independent failures. Section A5 shows why that assumption badly overstates availability.

---

## A4. Repair-bandwidth theory and piggybacked RS

### A4.1 Dimakis et al., "Network Coding for Distributed Storage Systems" (IEEE Trans. IT 2010)

**Citation [V].** A. G. Dimakis, P. B. Godfrey, Y. Wu, M. J. Wainwright, K. Ramchandran. *IEEE Trans. Inf. Theory* 56(9):4539–4551, September 2010. DOI 10.1109/TIT.2010.2054295 (Crossref). Text read from arXiv:0803.0632v1. Conference version: INFOCOM 2007, DOI 10.1109/INFCOM.2007.232.

**Key content [V]**

- **The problem with conventional repair** (abstract): the common practice is for a new node to *"download subsets of data stored at a number of surviving nodes, reconstruct a lost coded block … We show that this procedure is sub-optimal."* The paper instead proposes regenerating codes, in which helpers send functions of their data. It proves a *"fundamental tradeoff between storage and repair bandwidth … using flow arguments on an appropriately constructed graph"* (Theorem 1).
- **The two extreme points** (§III-C, arXiv numbering). M is the file size and d is the number of helpers.
  - Eq. (5), MSR: (α, γ) = (M/k, M·d / (k(d − k + 1))). With d = k, *"the total network bandwidth for repair is M, the size of the original file"*.
  - Eq. (6), MBR: α = γ = 2Md / (2kd − k² + k). *"MBR codes incur no bandwidth expansion at all, just like a replication system does."*
- **[Analysis] A worked example for RS(10,4)**, with M = 10 chunks' worth of data:
  - Naive repair downloads M.
  - MSR with d = 13 helpers needs γ = 13M/(10·4) = 0.325M, which is about 3.1× less repair traffic.

### A4.2 Rashmi et al., "A 'Hitchhiker's' Guide to Fast and Efficient Data Reconstruction in Erasure-coded Data Centers" (SIGCOMM 2014)

**Citation [V].** K. V. Rashmi, N. B. Shah, D. Gu, H. Kuang, D. Borthakur, K. Ramchandran. *Proc. ACM SIGCOMM 2014*, pp. 331–342. DOI 10.1145/2619239.2626325 (Crossref). Author copy: `cs.cmu.edu/~nihars/publications/Hitchhiker_SIGCOMM14.pdf`.

**Key content [V]**

- **Abstract:** Hitchhiker *"reduces both network traffic and disk IO by around 25% to 45% during reconstruction … with no additional storage, the same fault tolerance, and arbitrary flexibility in the choice of parameters."* It *"rides" on top of RS codes*.
- **Production measurements on Facebook's warehouse cluster:** *"36% reduction in the computation time and a 32% reduction in the data read time, in addition to the 35% reduction in network traffic and disk IO."*
- **§3.1, Hitchhiker-XOR+ for (k=10, r=4):** *"requires 35% lesser data for reconstruction of any of the data units"*. It uses two substripes. Encoding needs *"only XOR operations in addition to the underlying RS encoding."*
- **Hop-and-couple** couples bytes a fixed "hop-length" apart so that reconstruction reads stay contiguous on disk.
- **§6.5.** Encoding costs more than plain RS.

### A4.3 Facebook warehouse repair statistics (Hitchhiker §1, §6.6; Rashmi et al., HotStorage 2013)

**[V]** The measurements from the production cluster:

- *"a median of more than 50 machine-unavailability events occur per day, and a median of 95,500 blocks of RS-encoded data are recovered each day (the typical size of a block is 256 Megabytes (MB)) … a median of more than 180 Terabytes (TB) of data is transferred through the top-of-rack switches every day"* for this purpose.
- Over six months, *"among all the stripes that had at least one block to be reconstructed, 98.08% of them had exactly one such block missing, 1.87% had two blocks missing, and the number of stripes with three or more such blocks was 0.05%"* (Hitchhiker §6.6).
- HotStorage 2013 (`usenix.org/system/files/conference/hotstorage13/hotstorage13-rashmi.pdf`) reports the same 98.08 / 1.87 / 0.05 split. It adds that RS(10,4) blocks are placed on *"14 different (randomly chosen) machines … chosen from different racks. To recover a missing block, any 10 of the remaining 13 blocks … are downloaded"* through the top-of-rack switches.

**Implications (A4) [Analysis]**

- Repair traffic is not a second-order cost. At Facebook scale, RS repair moved about 180 TB/day across racks. Mantle's repair scheduler must be bandwidth-aware and rate-limited (see §R3).
- Single-chunk repair is about 98% of the workload. Codes that optimize it (LRC, Hitchhiker) cut real traffic. MSR/regenerating codes are theoretically optimal but less mature in deployment. No production system in this literature set uses them, so I would not choose them for v1.
- Hitchhiker keeps the RS storage cost and MDS property. That makes it a later drop-in option ("v3"), if repair traffic turns out to matter and LRC's wider stripe cannot be placed.

---

## A5. Ford et al., "Availability in Globally Distributed Storage Systems" (OSDI 2010)

**Citation [V].** D. Ford, F. Labelle, F. I. Popovici, M. Stokely, V.-A. Truong, L. Barroso, C. Grimes, S. Quinlan. *9th USENIX Symposium on Operating Systems Design and Implementation (OSDI '10)*. PDF: `usenix.org/legacy/event/osdi10/tech/full_papers/Ford.pdf`, 14 pages. **UNVERIFIED:** proceedings page range, because the PDF has no footers.

### Data, definitions, component behavior [V]

- **Data set** (§1): *"tens of Google storage cells, each with 1000 to 7000 nodes, over a one year period"*.
- **The 15-minute threshold** (§1, Fig. 1): *"less than 10% of events last longer than 15 minutes."* The paper focuses on events of 15 minutes or longer. *"initiating recovery after transient failures is inefficient … GFS typically waits 15 minutes before commencing recovery of data on unavailable nodes."*
- **Headline finding** (§1): *"the critical element in models of availability is their ability to account for the frequency and magnitude of correlated failures."*
- **Causes** (§3, Table 1): node restarts, planned reboots, unplanned reboots, and unknown. The example cell shows *"the majority of unavailability is generated by planned reboots"* (Fig. 4).
- **Component MTTF** (§3.1, Table 2): disk 10–50 years, node 4.3 months, rack 10.2 years. The node figure drives availability: *"significantly greater frequency of node failures makes them a much more important factor"*.
- **Scrubbing** (§3.1): *"Background scrubbing in GFS finds between 1 in 10^6 to 10^7 of older data blocks do not match the checksums recorded when the data was originally written."* Integrity is also verified on client reads.

### Correlated failures [V]

- **§4.1, burst definition:** *"a maximal sequence of node failures, each one occurring within a time window w of the next … We choose w = 120 s"*. The window is longer than the polling interval, *"less than a tenth of the average time it takes our system to recover a chunk"*, and Fig. 6 is flat beyond it.
- **How many failures are correlated:** *"37% of failures are part of a burst of at least 2 nodes … close to 37% of failures are truly correlated."* The false-clustering rates are 8.0% into any burst and 0.068% into bursts of 10 or more nodes.
- **§4.2–4.3.** A "rack affinity" score runs from 0 to 1, with 0.5 meaning random. The intro summarizes: *"most large bursts of failures are associated with rack- or multirack level events."*

### Coping with failure [V]

- **§5.1, repair prioritization and rate limits:** *"Distributed filesystems will necessarily employ queues for recovery operations following node failure. These queues prioritize reconstruction of stripes which have lost the most chunks. The rate at which missing chunks may be recovered is limited by the bandwidth of individual disks, nodes, and racks. Furthermore, there is an explicit design tradeoff in the use of bandwidth for recovery operations versus serving client read/write requests."* Fig. 9 shows recovery after a 20-node burst affecting millions of stripes, and notes *"Operators may adjust the rate-limiting."*
- **§5.2, rack-aware placement:** *"A rack-aware policy is one that ensures that no two chunks in a stripe are placed on nodes in the same rack … for small and medium bursts sizes, and large encodings, using a rack-aware placement policy increases the stripe MTTF by a factor of 3 typically. This is a significant gain considering that in uniform random placement, most stripes end up with their chunks on different racks due to chance."*

### Markov model findings (§7–§8) [V]

- **§8.1, validation.** The model is meant for *"relative comparison of competing storage solutions"*. Example validation: predicted 5E+6 days against observed 1.76E+6 days.
- **§8.2, recovery rate.**
  - Without correlated failures, *"reducing recovery times by a factor of µ will increase stripe MTTF by a factor of µ² for R = 3 and by µ⁴ for RS(9, 4)"*.
  - *"For RS(6, 3) with no correlated failures, a 10% reduction in recovery time results in a 19% reduction in unavailability. However, when correlated failures are taken into account, even a 90% reduction in recovery time results in only a 6% reduction in unavailability."*
- **§8.3, Table 3.** Stripe MTTF in days:

  | Policy (% overhead) | With correlated failures | Without correlated failures |
  |---|---|---|
  | R=2 (100) | 1.47E+5 | 4.99E+05 |
  | R=3 (200) | 6.82E+6 | 1.35E+09 |
  | R=4 (300) | 1.40E+8 | 2.75E+12 |
  | R=5 (400) | 2.41E+9 | 8.98E+15 |
  | RS(4,2) (50) | 1.80E+6 | 1.35E+09 |
  | RS(6,3) (50) | 1.03E+7 | 4.95E+12 |
  | RS(9,4) (44) | 2.39E+6 | 9.01E+15 |
  | RS(8,4) (50) | 5.11E+7 | 1.80E+16 |

  The accompanying text: *"failing to account for correlation of node failures typically results in overestimating availability by at least two orders of magnitude, and eight in the case of RS(8,4). Correlation also reduces the benefit of increasing data redundancy."*
- **§8.4, component rates:** *"Assuming R = 3 … a 10% reduction in the latent disk error rate has a negligible effect on stripe availability … a 10% reduction in the disk failure rate increases stripe availability by less than 1.5% … cutting node failure rates by 10% can increase data availability by 18%."*
- **§8.5, multi-cell replication** (Table 4):
  - R=2×2 with a 1-day inter-cell recovery reaches 1.08E+10 days, *"two orders of magnitude longer MTTF than R = 4"*, at about 6.8 MB/day of inter-cell bandwidth per user PB. The paper gives that figure as R=2's inverse MTTF, which for 1 PB is 6.8 GB/day, so the unit is off by a thousand (note 15 §7 item 5).
  - RS(6,3)×2 reaches 5.32E+13 days (1-day recovery) and 1.22E+15 days (1-hour recovery).

### Recommendations the authors made [V] (§10)

- *"correlation among node failures dwarfs all other contributions to unavailability"*.
- The concrete recommendations:
  - determine acceptable battery-transfer rates;
  - *"Focusing on reducing reboot times, because planned kernel upgrades are a major source of correlated failures"*;
  - *"Moving towards a dynamic delay before initiating recoveries, based on failure classification and recent history of failures in the cell."*

### Implications for mantle (A5) [Analysis]

**Correction (2026-09-29, note 15 §1.1).** Ford's chain counts chunks that are *unavailable*
for 15 minutes or more ("We call a chunk available if the node it is stored on is
available"; "we focus only on events that are 15 minutes or longer"), so every stripe MTTF
in Tables 3 and 4 is a mean time to unavailability, not to data loss. The relative lessons
below hold. The magnitudes are not durability, and mantle's durability model takes permanent
losses instead (docs/design/durability.md).

1. **Model correlated failures.** Any durability claim mantle makes, or any profile it picks, has to be evaluated under a correlated-failure model: bursts at the rack, power and upgrade-domain level. Independent-failure MTTDLs, like those in A3, are optimistic by 2–8 orders of magnitude (Table 3).
2. **Stripe width.**
   - In Table 3, RS(9,4) at 44% overhead is *less* available than R=3 under correlated failures (2.39E6 vs 6.82E6 days). RS(8,4) at 50% is 7.5× better than R=3.
   - Adding data chunks at fixed m increases exposure to bursts. That is my inference from Table 3, not a stated conclusion.
   - Ford does not evaluate RS(10,4) (UNVERIFIED how it would fare). One would expect it to sit near RS(9,4). RS(10,4) (offered only as an opt-in profile in §R1.2) is therefore acceptable **only with** strict rack-aware and copyset-constrained placement (A6) and fast repair of low-margin stripes. RS(8,4) (the recommended large-cluster default) or RS(9,6) are the safer choices.
3. **Repair delay.** Delay repair of transient failures (15 min is the GFS baseline). Make the delay dynamic, based on failure classification. Never delay stripes whose remaining margin is 0–1.
4. **Recovery speed is not a cure.** Faster recovery helps little once failures are correlated, since a 90% faster recovery gives only 6% less unavailability. Investment should go first to placement across failure domains, then to recovery bandwidth.
5. **Planned maintenance.** Planned reboots and upgrades dominate unavailability. Mantle's upgrade orchestration must be placement-aware, taking at most m−1 chunks of any stripe offline per batch, and preferably one (see WAS upgrade domains, A3.1).
6. **Integrity checking.** Checksum every read and scrub data at rest. GFS found checksum mismatches at a rate of 10⁻⁶ to 10⁻⁷ of older blocks.

---

## A6. Copysets and Tiered Replication

### A6.1 Cidon et al., "Copysets: Reducing the Frequency of Data Loss in Cloud Storage" (USENIX ATC 2013)

**Citation [V].** A. Cidon, S. M. Rumble, R. Stutsman, S. Katti, J. Ousterhout, M. Rosenblum. *2013 USENIX Annual Technical Conference (ATC '13)*, pp. 37–48. PDF: `usenix.org/system/files/conference/atc13/atc13-cidon.pdf`.

**Key content [V]**

- **Abstract:** *"random replication is almost guaranteed to lose data in the common scenario of simultaneous node failures due to cluster-wide power outages … in a 5000-node RAMCloud cluster under a power outage, Copyset Replication reduces the probability of data loss from 99.99% to 0.15%. For Facebook's HDFS cluster, it reduces the probability from 22.8% to 0.78%."*
- **Fig. 1 scenario.** R = 3, and 1% of nodes do not come back after a power outage; the paper cites 0.5–1% from its refs [7, 25].
- *"Copyset Replication with 3 replicas achieves a lower data loss probability than the random replication scheme does with 5 replicas."*
- **The frequency-versus-magnitude trade-off** (§1). For the 5000-node RAMCloud cluster with one outage a year, Copyset Replication loses data about once in *"625 years"*, and then *"an average of 64 GB"*. Random replication loses about 344 MB in *every* outage. Operators prefer fewer, larger losses, because each incident has a fixed cost. The paper quotes Facebook's HBase lead and Google's Barroso on this.
- **Definitions** (§3). A *copyset* is *"a distinct set of nodes that contain all copies of a given chunk. Each copyset is a single unit of failure."* The *scatter width* S is *"the number of nodes that store copies for each node's data. Using a low scatter width may slow recovery time from independent node failures, while using a high scatter width increases the frequency of data loss from correlated failures."*
- **Why random replication is fragile** (§3.2). With N = 5000, R = 3 and S = 10, a minimal scheme has about 8,300 copysets, while random replication has about 275,000. *"in a minimal copyset scheme, the number of copysets grows linearly with S, while random replication creates O(S^(R−1)) copysets."*
- **The algorithm** (§4):
  - Permutation phase: generate P = S/(R−1) random permutations of the nodes. Cut each into consecutive groups of R; each group is a copyset. A permutation is regenerated if it violates constraints such as racks.
  - Replication phase: the primary goes anywhere. HDFS places it locally; RAMCloud uses *"Mitzenmacher's randomized load balancing"*. Secondaries go to a random copyset that contains the primary.
  - Node join: create new copysets that contain the node.
  - Node failure: replace the node in each of its copysets and re-replicate.
- **Overhead** (§5.4.2). RAMCloud backup recovery took 51% longer: 1256 MB in 0.73 s for random versus 3648 MB in 1.10 s for copysets. The paper says this is *"not inherent"* and comes from a choice to keep scatter width strictly minimal.
- **§6.1, "Copysets and Coding":** coding *"techniques generally do not impact the probability of data loss due to simultaneous failures … If the coded data is distributed on a very large number of copysets, multiple simultaneous failures will still cause data loss."* HDFS-RAID reduced copysets only by a factor of 5, while *"Copyset Replication still creates two orders of magnitude fewer copysets."*

### A6.2 Cidon et al., "Tiered Replication: A Cost-effective Alternative to Full Cluster Geo-replication" (USENIX ATC 2015)

**Citation [V].** A. Cidon, R. Escriva, S. Katti, M. Rosenblum, E. G. Sirer. *2015 USENIX Annual Technical Conference (ATC '15)*, pp. 31–43. PDF: `usenix.org/system/files/conference/atc15/atc15-paper-cidon.pdf`.

**Key content [V]**

- **Abstract:**
  - *"using two replicas is sufficient for protecting against independent node failures, while using three random replicas is inadequate for protecting against correlated node failures."*
  - The design splits the cluster into a primary tier holding two replicas and a backup tier holding the third. The third replica is rarely read, so the backup tier can sit on separate infrastructure or in a separate AZ.
  - The algorithm *"can be executed incrementally for each cluster change, which allows it to supports dynamic environments in which nodes join and leave the cluster, and it facilitates additional data placement constraints … such as network and rack awareness."*
  - It *"improves the cluster-wide MTTF by a factor of 20,000 compared to random replication and by a factor of 20 compared to previous non-random replication schemes, without increasing the amount of storage."*
- **Algorithm 1 (greedy).** While some node's scatter width is below S, start a copyset from that node. Add candidates in increasing order of scatter width if they pass the constraint checks. Those checks cover the tier rule, other designer constraints, and "didNotAppear", which minimizes pairwise overlap between copysets. Commit when the copyset has R members. A node that joins is simply covered by rerunning the algorithm. A departing node's copysets are treated as down, then the algorithm reruns and the data is re-replicated.
- **Model** (§2, Table 1): S = 10, R = 3, τ = 60 min, node MTTF 10 years. *"Typical values for Sτ are between 1-30 minutes"*. Example: 1 TB, S = 10 and 500 MB/s per node give about 3 minutes to recover a node.
- **Coding** (§6): *"fully compatible with any coding or de-duplication schemes"*. The paper gives no EC-specific analysis.

### A6.3 [Analysis] Extending the copyset argument to erasure-coded stripes

For RS(k, m), a *stripe set* is the set of n = k + m nodes (or disks) that hold one stripe. Data is lost if at least m+1 members of some stripe set fail at the same time. I used the combinatorial model from Copysets §3: F of N nodes fail at random, so the number of failed members in a given set is hypergeometric, and P(any loss) = 1 − (1 − p_set)^(#distinct sets). With N = 5000 and F = 50 (1%), the model **reproduces the paper's two R=3 numbers**:

- 99.99% for random placement with 10⁷ distinct sets;
- 0.157% for the minimal single-permutation scheme, which the paper reports as 0.15%.

Applying the same model to EC stripes. "Shuffle copysets" follow Tectonic (A7.2): P permutations, each cut into ⌊N/n⌋ groups, which gives scatter width S ≈ P(n−1).

| Code | p_set (≥ m+1 of n failed) | P=1 (S≈n−1) | P=10 | P=100 | Random placement, 10⁷ stripes | Random, 10⁸ stripes |
|---|---|---|---|---|---|---|
| R=3 | 9.41e-7 | 0.157% | 1.56% | 14.5% | 99.99% | 100% |
| RS(6,3) | 1.08e-6 | 0.060% | 0.60% | 5.8% | ≈100% | 100% |
| RS(4,2) | 1.84e-5 | 1.52% | 14.2% | 78.5% | 100% | 100% |
| RS(8,4) | 6.13e-8 | 0.0026% | 0.026% | 0.25% | 45.8% | 99.8% |
| RS(10,4) | 1.53e-7 | 0.0054% | 0.054% | 0.54% | 78.2% | 100% |
| RS(9,6) | 3.92e-11 | 1.3e-6 % | 1.3e-5 % | 1.3e-4 % | 0.039% | 0.39% |

The columns P=1 through P=100 are copyset-constrained placement. For sensitivity, RS(10,4) with P=100 gives 0.014% at F=25 and 16.4% at F=100.

What the table shows:

- With random per-stripe placement, any mid-sized cluster holds 10⁷ to 10¹⁰ stripes; Tectonic's cluster held 15 B blocks. At that size, a simultaneous 1% node loss almost certainly destroys *some* stripe unless m is large, as RS(9,6) shows. Rack-aware placement alone does not help against this kind of failure: a power event that kills a random 1% of hosts spreads across racks.
- Constraining stripes to a bounded family of stripe sets cuts that probability by orders of magnitude. The same trade-off Cidon describes applies: larger P (scatter width) means faster, more parallel rebuilds but more distinct stripe sets.
- For EC the constant is kinder than for replication, because loss needs m+1 coincident failures rather than R.

---

## A7. CRUSH versus explicit placement recorded in metadata (Tectonic)

### A7.1 Weil, Brandt, Miller, Maltzahn, "CRUSH: Controlled, Scalable, Decentralized Placement of Replicated Data" (SC 2006)

**Citation [V].** *Proc. ACM/IEEE SC 2006*, DOI 10.1109/SC.2006.19. Crossref lists the pages as "31". PDF: `ceph.io/assets/pdfs/weil-crush-sc06.pdf`.

**Key content [V]**

- **Abstract.** CRUSH is a *"scalable pseudorandom data distribution function … that efficiently maps data objects to storage devices without relying on a central directory … designed to facilitate the addition and removal of storage while minimizing unnecessary data movement … distributes data in terms of user-defined policies that enforce separation of replicas across failure domains."*
- **§1.** *"any party in a large system can independently calculate the location of any object"*. The metadata is *"mostly static, changing only when devices are added or removed"*. Replication, RAID parity, and erasure coding are all supported.
- **§3.1–3.2, cluster map and rules.** The weighted hierarchical cluster map produces a *"declustered"* distribution. Rules are sequences of `take(a)` / `select(n,t)` / `emit`. Table 1's example is `take(root) → select(1,row) → select(3,cabinet) → select(1,disk) → emit`.
- **§3.2.1, reject and reselect.** Selection retries for *"collision"*, *"failed"*, or *"overloaded"* devices. *"Failed or overloaded devices are marked as such in the cluster map, but left in the hierarchy to avoid unnecessary shifting of data"*. An overloaded device sheds load by rejecting placements pseudo-randomly, with a probability set in the map.
- **§3.2.2, "Replica Ranks", the EC-specific point:** *"With parity and erasure coding schemes … the rank or position of a storage device in the CRUSH output is critical because each target stores different bits of the data object. In particular, if a storage device fails, it should be replaced in CRUSH's output list R in place, such that other devices in the list retain the same rank"*. Reselection uses r′ = r + f_r·n for EC, instead of "first n" (r′ = r + f) for replication.
- **§3.3–§4.2, data movement.** A failed device remaps only w_failed/W of the data. Changes to the hierarchy can cause *"additional data movement beyond the theoretical optimum"*. Table 2 summarizes the bucket types:

  | Bucket | Lookup speed | Data movement on additions | Data movement on removals |
  |---|---|---|---|
  | Uniform | O(1) | poor | poor |
  | List | O(n) | optimal | poor |
  | Tree | O(log n) | good | good |
  | Straw | O(n) | optimal | optimal |

  Fig. 5 evaluates the movement factor relative to the optimum Δw/W on a 4-level hierarchy of 7290 devices.
- **§4.3.** Mapping is *"O(log n) for a cluster with n OSDs."*
- **§4.4.** *"a single event like a power failure … will affect multiple devices, and the larger peer groups associated with declustered replication greatly increase the risk of data loss."* This is the same problem Copysets addresses. The paper admits *"it is difficult to quantify the magnitude of the improvement in overall system reliability in the absence of a specific storage cluster configuration and associated historical failure data."*
- **§6.** CRUSH *"eliminat[es] the conventional need for allocation metadata"*.

### A7.2 Tectonic's explicit placement: Pan et al., "Facebook's Tectonic Filesystem: Efficiency from Exascale" (FAST 2021)

**Citation [V].** S. Pan, T. Stavrinos, Y. Zhang, A. Sikaria, P. Zakharov, A. Sharma, S. Shankar P, M. Shuey, R. Wareing, M. Gangapuram, G. Cao, C. Preseau, P. Singh, K. Patiejunas, JR Tipton, E. Katz-Bassett, W. Lloyd. *19th USENIX Conference on File and Storage Technologies (FAST '21)*, pp. 217–231. PDF: `usenix.org/system/files/fast21-pan.pdf`.

**Key content [V]**

- **§3.2, Chunk Store:** *"Blocks are either Reed-Solomon encoded or replicated for durability. For RS(r, k) encoding, the block data is split into r equal chunks (potentially by padding the data), and k parity chunks are generated … Chunks in a block are stored in different fault domains (e.g., different racks) for fault tolerance. Background services repair damaged or lost chunks"*. Tectonic also *"provides per-block durability to allow tenants to tune the tradeoff"*.
- **§3.3, Metadata Store (Table 1).** There are three layers: Name (dir → subdirs and files), File (file → blocks), and Block (block → `list<disk_id>`, i.e. the chunks). The Block layer also holds the **reverse index disk → blocks**, which maintenance uses. Each layer is hash-partitioned, and the store is ZippyDB.
- **§3.5, background services.** These are garbage collectors, a rebalancer, a stat service, disk inventory, block repair/scan, and a storage-node health checker. *"The rebalancer identifies chunks that need to be moved in response to events like hardware failure, added storage capacity, and rack drains. The repair service handles the actual data movement by reconciling the chunk list to the disk-to-block map for every disk"*. This runs per Block-layer shard and per disk.
- **§3.5, "Copysets at scale":** *"a copyset for an RS(10,4)-encoded block consists of 14 disks … Having too many copysets risks data unavailability if there is an unexpected spike in disk failures … too few copysets results in high reconstruction load to peer disks … The Block Layer and the rebalancer service together attempt to maintain a fixed copyset count … keep in memory about one hundred consistent shuffles of all the disks in the cluster. The Block Layer forms copysets from contiguous disks in a shuffle. On a write, the Block Layer gives the Client Library a copyset from the shuffle corresponding to that block ID … Copysets are best-effort, since disk membership in the cluster changes constantly."*
- **§5.1, data warehouse.**
  - *"Long-lived data is typically RS(9,6) encoded; short-lived data, e.g., map-reduce shuffles, is typically RS(3,3)-encoded."*
  - **Hedged quorum writes:** the client sends *"a reservation request to 19 storage nodes in different failure domains, four more than required for the write … writes … to the first 15 storage nodes that respond … acknowledges … as soon as a quorum of 14 out of 15 nodes return success. If the 15th write fails, the corresponding chunk is repaired offline."*
  - Result: *"~20% improvement in 99th percentile latency for RS(9,6) encoded, 72 MB full-block writes, in a test cluster with 80% throughput utilization."*
  - The reservation step is described as *"similar to hedging [22]"*, where [22] is Dean & Barroso.
- **§5.2, blob storage.** New blobs are written as replicated partial-block quorum appends. The client commits *"the post-append block size and checksum to the block metadata before acknowledging"*. When a block is sealed, *"the Client Library reencodes the block from replicated form to RS(10,4) … requiring only a single large IO on each of the 14 target storage nodes"*. RS-encoding each small append directly would take *"14 IOs with RS(10, 4) instead of 3"*.
- **§6.4, managing reconstruction load.** *"Because Tectonic uses contiguous RS encoding and the majority of reads are smaller than a chunk size, reads are usually direct … Reconstruction reads require 10× more IOs than direct reads (for RS(10,4) encoding)."* Overload causes cascades, which the paper calls a *"reconstruction storm"*. The fix: *"We instead prevent reconstruction storms by restricting reconstructed reads to 10% of all reads. This fraction of reconstructed reads is typically enough to handle disk, host, and rack failures in our production clusters."*
- **§6.1, Table 2** (one production cluster): 1590 PB capacity, 1250 PB used, 10.7 B files, 15 B blocks, 4208 storage nodes.

### Implications for mantle (A7) [Analysis]

**Choose explicit, metadata-recorded placement (as Tectonic does) over CRUSH-style computed placement.** The reasons:

1. **Metadata is consulted anyway.** In a Tectonic-style design the metadata store must be read for name → file → block regardless. The extra block → disks lookup costs little. The main CRUSH advantage is locating data "without relying on a central directory", and mantle does not need it.
2. **Constraints compose.** Explicit placement can combine arbitrary constraints: fault domains, upgrade domains (A3.1), copyset families (A6, Tectonic §3.5), load and free space (A8), tenant isolation. It can also respect all of them during churn. In CRUSH, constraints must be expressible as rules over a static hierarchy, and a hierarchy change moves data *"beyond the theoretical optimum"* (§3.3).
3. **Repair is simpler.** The reverse index (disk → blocks) turns "what does disk D hold?" into a scan of one shard (Tectonic §3.5). With CRUSH the same question means recomputing mappings for every object.
4. **What to keep from CRUSH.** Keep the **replica-rank rule** (§3.2.2): when an EC chunk is repaired onto a new disk it keeps its chunk index. Keep **marking failed devices without moving data immediately** (§3.2.1): combined with the transient-failure delay from A5, this avoids unneeded data movement.
5. **Costs of explicit placement.** It needs a rebalancer, a repair service, and stored placement for every block: 15 B entries for Tectonic's cluster. Mantle's metadata-store design must budget for this, roughly 14 disk IDs × 4–8 bytes per block, plus the reverse index.

---

## A8. Mitzenmacher: the power of two choices, and stale information

### A8.1 "The Power of Two Choices in Randomized Load Balancing" (IEEE TPDS 2001)

**Citation [V].** M. Mitzenmacher. *IEEE Trans. Parallel Distrib. Syst.* 12(10):1094–1104, October 2001. DOI 10.1109/71.963420 (Crossref). PDF: `eecs.harvard.edu/~michaelm/postscripts/tpds2001.pdf`.

**Key content [V]**

- **Model.** The "supermarket model": arrivals are Poisson with rate λn (λ < 1) across n FIFO servers with exponential service times. Each customer samples d servers and joins the shortest queue.
- **Abstract:** *"Having d = 2 choices leads to exponential improvements in the expected time a customer spends in the system over d = 1, whereas having d = 3 choices is only a constant factor better than d = 2."*
- **Expected time in the system.** With d = 1 it is 1/(1−λ). With d ≥ 2 it is bounded by Σ_{i≥1} λ^{(d^i − d)/(d − 1)} + o(1) (Theorems 1 and 5).
- **Theorem 6.** For d ≥ 2 the longest queue is *"log log n / log d + O(1) with high probability"*.
- **The rule of thumb** (§1): *"Systems where items have two (or a small number of) choices can perform almost as well as a perfect load balancing system with global load knowledge. Indeed, because a system based on two choices can have significantly lower overhead, it is possible it may perform better than apparently better but more complicated load balancing algorithms."*
- **The static case.** Karp et al. and Azar et al. [1] *"demonstrated an exponential improvement in the maximum load"* (§1.1). I did not re-derive the exact static bound for one choice versus two here. For the static version the paper points to Azar et al.
- **§4, simulations** with n = 100 and 500 queues. The model's predictions fall *"within a few percent"* of simulation up to λ = 0.95.

### A8.2 "How Useful Is Old Information?" (IEEE TPDS 2000)

**Citation [V].** M. Mitzenmacher. *IEEE Trans. Parallel Distrib. Syst.* 11(1):6–20, January 2000. DOI 10.1109/71.824633 (Crossref). The author's PDF is a scan; I read page 6 visually.

**Key content [V].** From the abstract:

- *"only small amounts of queue length information can be extremely useful … having incoming tasks choose the least loaded of two randomly chosen processors is extremely effective over a large range of possible system parameters. In contrast, using global information can actually degrade performance unless used carefully; for example, unlike most settings where the load information is current, having tasks go to the apparently least loaded server can significantly hurt performance."*

From §1:

- *"the strategy of going to the shortest queue can lead to extremely bad behavior when load information is out of date; however, the strategy of going to the shortest of two randomly chosen queues performs well"*.

Dean & Barroso (A9) make the same practical point: probing and then picking the least-loaded server *"can create temporary hot spots by all clients picking the same (least-loaded) server at the same time."*

### Implications for mantle (A8) [Analysis]

- **Selecting the disks for a new stripe.** First filter candidates by hard constraints: failure and upgrade domains, copyset family (A6), health, and not-draining. Then take **two random feasible candidates** and choose the better one by a score such as free-space fraction and recent queue depth or latency. Do not choose "the global emptiest disks". Placement decisions run on stale stats pushed from storage nodes, which is exactly the setting where least-loaded herds (A8.2).
- **Choosing a copyset.** Hash(block_id) gives two candidate shuffle-groups (A7.2). Pick the less loaded. This keeps Tectonic's bounded copyset count while adding the d = 2 benefit.
- **Read-path selection.** When several replicas or subsets can serve a read, use d = 2 for choosing among them too.

---

## A9. "The Tail at Scale" and k-of-n erasure-coded reads

### A9.1 Dean & Barroso, "The Tail at Scale" (CACM 2013)

**Citation [V].** J. Dean and L. A. Barroso. *Communications of the ACM* 56(2):74–80, February 2013. DOI 10.1145/2408776.2408794 (Crossref). Author copy: `barroso.org/publications/TheTailAtScale.pdf`.

**Fan-out amplifies the tail [V].** A server that *"typically responds in 10ms but with a 99th-percentile latency of one second"*, fanned out over *"100 such servers in parallel, then 63% of user requests will take more than one second"*. At 1-in-10,000 slow requests across 2,000 servers, *"almost one in five"* user requests are slow. Table 1 gives leaf-finish times: the 99th percentile for one random leaf is 10 ms, but 140 ms for all leaves.

**Hedged requests [V].**

- *"issue the same request to multiple replicas and use the results from whichever replica responds first … sends one request to the replica believed to be the most appropriate, but then falls back on sending a secondary request after some brief delay. The client cancels remaining outstanding requests once the first result is received."*
- *"One such approach is to defer sending a secondary request until the first request has been outstanding for more than the 95th-percentile expected latency for this class of requests. This approach limits the additional load to approximately 5% while substantially shortening the latency tail."*
- *"in a Google benchmark that reads the values for 1,000 keys stored in a BigTable table distributed across 100 different servers, sending a hedging request after a 10ms delay reduces the 99.9th-percentile latency for retrieving all 1,000 values from 1,800ms to 74ms while sending just 2% more requests."*
- *"The overhead of hedged requests can be further reduced by tagging them as lower priority than the primary requests."*

**Tied requests [V].**

- Hedging has a *"window of vulnerability in which multiple servers can execute the same request unnecessarily. That extra work can be capped by waiting for the 95th-percentile expected latency before issuing the hedged request, but this approach limits the benefits to only a small fraction of requests. Permitting more aggressive use of hedged requests with moderate resource consumption requires faster cancellation of requests."*
- The authors also note: *"Mitzenmacher said allowing a client to choose between two servers based on queue lengths at enqueue time exponentially improves load-balancing performance over a uniform random scheme. We advocate not choosing but rather enqueuing copies of a request in multiple servers simultaneously and allowing the servers to communicate updates on the status of these copies to each other"*. They call these "tied requests".
- The client should wait *"a small delay of two times the average network message delay (1ms or less in modern data-center networks)"* before sending the second copy.
- Table 2: a BigTable read of uncached data from the cluster filesystem, where each chunk has 3 replicas, with and without a tied request after 1 ms:

  | Percentile | Idle cluster: no hedge → tied | With concurrent terasort: no hedge → tied |
  |---|---|---|
  | p50 | 19 → 16 ms (−16%) | 24 → 19 ms (−21%) |
  | p90 | 38 → 29 ms (−24%) | 56 → 38 ms (−32%) |
  | p99 | 67 → 42 ms (−37%) | 108 → 67 ms (−38%) |
  | p99.9 | 98 → 61 ms (−38%) | 159 → 108 ms (−32%) |

- The alternative of probing remote queues first is *"less effective than submitting work to two queues simultaneously"*. Three reasons: load changes between the probe and the request; service times are hard to estimate; and clients herd onto the same least-loaded server.

**Cross-request (long-term) techniques [V].**

- **Micro-partitions.** With about 20 partitions per machine, load can be shed *"in roughly 5% increments and in 1/20th the time"*.
- **Selective replication** of hot items.
- **Latency-induced probation.** A slow machine is excluded but keeps receiving *"shadow requests"*, and *"removal of serving capacity from a live system during periods of high load actually improves latency."*
- **"Good enough" results** and **canary requests**, both aimed at information-retrieval systems.
- **Mutations.** The techniques *"are most applicable for operations that do not perform critical mutations"*. Writes are more tolerant because they are small, can move off the critical path, and consistent updates already use quorum protocols.

### A9.2 Applicability to reading k-of-n erasure-coded chunks (primary literature)

- **Azure LRC §5.1 [V].** Reading k' > k fragments and decoding from the first k arrivals is *"very effective in weeding out slow fragments"*. RS read with 12 fragments took 305 ms; with 13 it took 151 ms. For 4 MB bandwidth-bound reads, extra fragments *"hurts latency"* (§5.2).
- **EC-Cache [V].** K. V. Rashmi, M. Chowdhury, J. Kosaian, I. Stoica, K. Ramchandran, "EC-Cache: Load-Balanced, Low-Latency Cluster Caching with Online Erasure Coding," *OSDI '16*, pp. 401–417.
  - "Late binding": *"instead of reading exactly k splits, we read (k + ∆) splits (where ∆ ≤ r) and wait for the reading of any k splits to complete."*
  - *"using k = 10 and ∆ = 1 suffices … a bandwidth overhead of at most 10% can lead to more than 50% reduction in the median and tail latencies"*.
  - Caveats: this is an in-memory cache, not disk. It *"offers advantages only for objects greater than 1 MB due to the overhead of creating (k + ∆) parallel TCP connections."*
- **Huang, Pawar, Zhang, Ramchandran [V].** "Codes Can Reduce Queueing Delay in Data Centers," *ISIT 2012*, pp. 2766–2770, DOI 10.1109/ISIT.2012.6284026. From the arXiv:1202.1359 abstract: a simple linear code plus "Blocking-one Scheduling" can *"reduce data retrieval delay by up to 17% over currently popular replication-based strategies"*. The setting is a single piece of content.
- **Joshi, Liu, Soljanin [V].** "On the Delay-Storage Trade-Off in Content Download from Coded Distributed Storage Systems," *IEEE JSAC* 32(5):989–997, May 2014, DOI 10.1109/JSAC.2014.140518. From the arXiv:1305.3945 abstract: *"reading only a subset of the disks is sufficient to reconstruct the content. For the same total storage used, coding exploits the diversity in storage better than simple replication"*. The analysis uses a fork-join queueing model and derives a download-time/storage trade-off.
- **Tectonic §6.4 [V].** Reconstructed reads are capped at 10% of reads to prevent reconstruction storms (A7.2).

### Implications for mantle (A9) [Analysis]

Hedging a read of RS-encoded data is not the same as hedging a replicated read. With a contiguous layout, the normal read of a byte range touches **one** data chunk. The only way to hedge it without a replica is a **reconstruction read**, which costs k disk IOs and k× the network bytes for that range.

This is safe only if all of the following hold:

1. The hedge fires late, at the p95 of a latency histogram for the same request class. This bounds how often it fires to about 5%.
2. The hedge is sent at lower priority.
3. It draws from a global reconstruction budget, e.g. ≤ 10% of reads, as in Tectonic. This guards against reconstruction storms.
4. It is disabled when the node or cluster is overloaded.

For reads that need k chunks anyway (degraded reads, full-stripe reads, repair), read k+1 and use the first k (Azure §5.1, EC-Cache). The exception is bandwidth-bound transfers (Azure §5.2); an example rule is to disable Δ when the per-read payload exceeds a threshold or NIC utilization is high.

Detailed policy is in §R4.

---

## A10. f4 (OSDI 2014): EC and replication-factor numbers

**Citation [V].** S. Muralidhar, W. Lloyd, S. Roy, C. Hill, E. Lin, W. Liu, S. Pan, S. Shankar, V. Sivakumar, L. Tang, S. Kumar, "f4: Facebook's Warm BLOB Storage System," *11th USENIX Symposium on Operating Systems Design and Implementation (OSDI '14)*, pp. 383–398. PDF: `usenix.org/system/files/conference/osdi14/osdi14-paper-muralidhar.pdf`. The author list was checked against the PDF title page.

**Key content [V]**

- **Abstract.** f4 *"stores over 65PBs of logical BLOBs and reduces their effective-replication-factor from 3.6 to either 2.8 or 2.1"*.
- **Haystack's 3.6×** is 3 replicas × 1.2 for RAID-6 on 12-disk nodes.
- **§1.** f4 uses *"Reed-Solomon(10,4) coding and lays blocks out on different racks … XOR coding in the wide-area"*, and it *"saves over 53PB"*.
- **Cells.** Each cell has *"14 racks of 15 hosts with 30 4TB drives per host"*. That is exactly one rack per chunk of RS(10,4).
- **Block size.** Blocks are *"typically 1 GB"*. Larger blocks mean fewer BLOBs spanning blocks and less metadata. Blocks are not made larger still because rebuilding them would cost more.
- **Replication factors.** The 2.8× figure is RS(10,4) (1.4×) stored in two regions. The 2.1× figure uses a buddy volume in another region plus an XOR in a third: *"The 2.1 replication factor comes from the 1.4X for the primary single cell replication for each of two volumes and another 1.4X for the geo-replicated XOR of the two volumes."*
- **Failure handling.**
  - *Backoff nodes* reconstruct just the requested BLOB online ("40KB instead of 1GB").
  - *Rebuilder nodes* rebuild full blocks offline. They detect failures by probing and report to a coordinator.
- **Tectonic's later view** (§2.1). Tectonic describes f4 as having *"an effective replication factor of 2.8×"*, with Haystack's effective factor having grown *"to 5.3×"* because of IOPS-driven over-provisioning.

**Implications for mantle [Analysis]**

- **Two independent 1.4× deployments.** f4 and Tectonic's blob re-encoding both use RS(10,4). For an S3-style blob workload, 1.4× within a cluster is the established production point.
- **Geo-level savings are separate.** f4's XOR-across-three-regions trick targets mantle's future multi-site mode, not the in-cluster code.
- **Serve degraded reads at object granularity.** Reconstruct only the requested range, as f4's backoff nodes do, and never the whole chunk. This matches Tectonic's direct-read design.

---

# Part B — Rust crate evaluation

**Method.**

- Versions, dates, licenses, MSRVs and download counts come from the crates.io API. They were looked up on 2026-09-28.
- Source code is from the published `.crate` tarball (`static.crates.io`) for the named version. `file:line` references point into that tarball.
- Maintenance signals come from the GitHub API: last commit, last push, open issues including PRs, and archived status.
- The panic audit greps each crate's non-test code for `panic!`, `.unwrap()`, `.expect(`, `assert!`, `assert_eq!`, `unreachable!`, `unimplemented!` and `todo!`. Each hit is then read to decide whether caller-controlled input can reach it through the public API.
- I wrote no code and ran no crate benchmarks. The one local measurement (B4) uses OpenSSL's built-in `speed` tool.

## B1. Reed–Solomon crates

### Summary table

| Crate (latest) | Released | License | Algorithm and field | SIMD and dispatch | Shard constraints | Public API returns | Maintenance |
|---|---|---|---|---|---|---|---|
| **reed-solomon-simd 3.1.0** | 2025-10-14 | MIT AND BSD-3-Clause | Leopard / Lin-Chung-Han FFT, GF(2^16), O(n log n), systematic | SSSE3, AVX2 (x86/x86_64), NEON (aarch64), selected at **runtime** via `cpufeatures`; scalar fallback | size even and non-zero; 1–32768 original + 1–32768 recovery, up to 65535 per the README table | `Result<_, Error>` throughout | last commit 2025-10-14; repo pushed 2026-09-05; 20 open |
| reed-solomon-erasure 6.0.0 | 2022-09-23 | MIT | Vandermonde→systematic matrix (Backblaze/klauspost port); GF(2^8) (n ≤ 256) or GF(2^16) (n ≤ 65536) | optional `simd-accel` compiles **C** (`simd_c/reedsolomon.c`) with **`-march=haswell`** by default on x86_64 (compile-time; no runtime detection) | equal shard sizes | `Result`, but some `pub` helpers panic | last commit 2022-11-11; 20 open |
| reed-solomon-novelpoly 2.0.0 | 2024-01-25 | Apache-2.0 AND MIT | LCH novel basis, GF(2^16) | AVX only if built with `target_feature="avx"` **and** feature `avx` (compile-time) | n rounded **up** and k rounded **down** to powers of two | `Result` | last commit 2024-01-25 |
| leopard-codec 0.2.0 | 2025-09-18 | Apache-2.0 | Leopard, GF(2^8) only | none | shard size % 64 == 0; parity ≤ data; ≤ 256 shards | `Result` | celestiaorg; last commit 2025-09-18 |
| reed-solomon-16 0.1.0 | 2022-01-04 | MIT AND BSD-3-Clause | Leopard GF(2^16) (predecessor of -simd) | none | — | — | last commit 2022-01-06 |
| rustfs-erasure-codec 9.0.0 | 2026-09-16 | MIT | classic RS GF(2^8)/GF(2^16) + Leopard GF8/GF16 | runtime-dispatched (NEON/SSSE3/AVX2/AVX-512/GFNI/VSX) per README | — | — | created 2026-06-17; 8 releases (majors 7→8→9) in 3 months; 1 GitHub star; MSRV 1.96 |
| erasure-isa-l 0.3.0 / -sys 1.1.0; isal-rs 0.5.3 | 2026-03 / 2024-10 | MIT / BSD-3 | bindings to Intel ISA-L (C/asm) | ISA-L's own dispatch | — | — | single maintainers; `-sys` vendors ISA-L and builds it from source if no system `libisal` is found |

### B1.1 `reed-solomon-simd` 3.1.0 (details)

**Identity [V].**

- MSRV 1.82. 2.10 M total downloads, 400 k in the last 90 days.
- Repo `AndersTrier/reed-solomon-simd`.
- The README says it is a fork of Markus Laire's `reed-solomon-16`, which is based on Leopard-RS.

**Algorithm and SIMD [V].**

- README: *"Reed-Solomon erasure coding based on Leopard-RS … O(n log n) complexity. Entirely written in Rust. Runtime selection of best SIMD implementation on both AArch64 (Neon) and x86(-64) (SSSE3 and AVX2) with fallback to plain Rust."*
- Dispatch is in `src/engine/engine_default.rs:28-50`: `cpufeatures::new!(has_avx2, "avx2")` selects `Avx2`, otherwise `has_ssse3` selects `Ssse3`; on aarch64, `has_neon` selects `Neon`; otherwise `NoSimd`.
- `cpufeatures 0.2.17` is a dependency only on x86, x86_64 and aarch64. The default feature is `std`, and the crate works in `no_std`.
- README "Safety": *"The only use of unsafe in this crate is to allow for target specific optimizations in Ssse3, Avx2 and Neon."*

**Constraints [V].**

- *"Any combination of 1 - 32768 original shards with 1 - 32768 recovery shards"*. Up to 65535 are allowed on either side, subject to a table: original ≤ 2^16 − 2^n when recovery ≤ 2^n.
- *"Shard size must be even (shard.len() % 2 == 0)"*. The error variant is `Error::InvalidShardSize`: *"Size must be non-zero and even."*
- Compatibility: *"Starting from version 3.0.0, shard sizes that are not multiples of 64 are supported. However, if your shard size is a multiple of 64, it remains compatible across all versions."*

**API and allocation [V]** (`src/reed_solomon.rs`).

- `ReedSolomonEncoder::new(original_count, recovery_count, shard_bytes) -> Result<Self, Error>` *"allocates required working space"*.
- `add_original_shard(&mut self, shard) -> Result<(), Error>`.
- `encode(&mut self) -> Result<EncoderResult<'_>, Error>`. Recovery shards are borrowed from internal buffers. *"When returned EncoderResult is dropped the encoder is automatically reset"*.
- `reset(...)`: *"Existing working space is re-used if it's large enough or re-allocated otherwise."*
- `supports(o, r) -> bool`.
- The decoder mirrors this: `add_original_shard(index, shard)`, `add_recovery_shard(index, shard)`, and `decode() -> Result<DecoderResult>`. `DecoderResult::restored_original(index) -> Option<&[u8]>` is bounds-checked (`rate/decoder_work.rs:189-196`).
- One-shot `encode()` and `decode()` functions also exist.
- Error variants: `DifferentShardSize`, `DuplicateOriginalShardIndex`, `DuplicateRecoveryShardIndex`, `InvalidOriginalShardIndex`, `InvalidRecoveryShardIndex`, `InvalidShardSize`, `NotEnoughShards`, `TooFewOriginalShards`, `TooManyOriginalShards`, `UnsupportedShardCount`.

**Panic audit [V].** I found 14 potential panic sites in non-test code. None is reachable through the high-level public API with bad input:

- `rate/encoder_work.rs:105` and `rate/decoder_work.rs:157`: `assert!(shard_bytes % 2 == 0)`. Both are preceded by `Self::validate(original_count, recovery_count, shard_bytes)?` (`rate/rate_high.rs:125`, `:292`, and the same in the low and default rates), which returns `InvalidShardSize` first.
- `rate/rate_default.rs:113-346`: `unreachable!()` on the `InnerEncoder::None` / `InnerDecoder::None` state. This is an internal state invariant.
- `engine/shards.rs:182`: an `assert!` on buffer sizing. Internal.
- `engine/tables.rs:250,281`: `try_into().unwrap()` converting fixed-size tables. Cannot fail.
- The crate also makes `engine` and `rate` public (`src/lib.rs:40-41`). I did not audit those lower-level modules for panics that callers can trigger. Mantle should use only `ReedSolomonEncoder` and `ReedSolomonDecoder`.

**README benchmarks [V].** Single-core AVX2 on an AMD Ryzen 5 3600; *"On an Apple Silicon M1 CPU throughput is about the same (+-10%)"*. Shards are 1024 bytes. Throughput is measured over original + recovery bytes.

| Original : Recovery | Encode | Decode (1% loss ; 100% loss) |
|---|---|---|
| 32 : 32 | 10.237 GiB/s | 254.24 MiB/s ; 253.60 MiB/s |
| 64 : 64 | 8.6758 GiB/s | 459.18 MiB/s ; 456.83 MiB/s |
| 128 : 128 | 7.3891 GiB/s | 753.11 MiB/s ; 758.65 MiB/s |
| 256 : 256 | 6.3753 GiB/s | 1.0391 GiB/s ; 1.0323 GiB/s |
| 1 000 : 100 | 5.6079 GiB/s | 1021.7 MiB/s ; 1022.0 MiB/s |
| 32 768 : 32 768 | 1.6049 GiB/s | 681.39 MiB/s ; 667.93 MiB/s |

The README also says: *"This crate is the fastest in all cases on my AMD Ryzen 5 3600, except in the case of decoding with about 42 or fewer recovery shards. There's also a one-time initialization (< 10 ms) for computing tables."*

The README gives **no** numbers for mantle's shapes (6:3, 10:4, 9:6), and the crate's shipped `benches/benchmarks.rs` starts at 32:32. **UNVERIFIED:** throughput at mantle's shapes and chunk sizes.

The README also warns: *"This crate does not detect or correct errors within a shard … include an error detection hash with each shard … CRC32c"*.

### B1.2 `reed-solomon-erasure` 6.0.0 (details)

**Identity and lineage [V].**

- MIT. 6.04 M downloads. The repo moved from `darrenldl/reed-solomon-erasure` to `rust-rse/reed-solomon-erasure`.
- README: a port of Backblaze's Java implementation, Klaus Post's Go implementation, and NicolasT's Haskell implementation. The SIMD C file is NicolasT's, under MIT.

**Construction.** The generator is Vandermonde made systematic: `vandermonde.multiply(&top.invert().unwrap())` at `core.rs:435`. That is the correct Plank–Ding-style construction.

**Fields and limits.** `galois_8` and `galois_16`. `ReedSolomon::new(data, parity)` *"Returns Error::TooManyShards if data_shards + parity_shards > F::ORDER"* (`core.rs:444`).

**The `simd-accel` feature [V].**

- The feature pulls in `cc` and `libc`. `build.rs` compiles `simd_c/reedsolomon.c` only on x86_64 or aarch64, and not on MSVC, Android or iOS.
- On x86_64 it adds `-march=haswell` unless `RUST_REED_SOLOMON_ERASURE_ARCH` is set (`build.rs:160-181`).
- README: *"simd-accel is tuned for Haswell+ processors on x86-64 and not in any way for other architectures"*.
- The C code picks SSE2, SSSE3, AVX2, AVX-512, NEON or AltiVec with `#if` at compile time.

**Consequence [Analysis].** An x86_64 binary built with `simd-accel` requires AVX2-class CPUs. There is no runtime fallback, so it would crash with an illegal instruction on older hosts.

**README performance.** The only table covers versions 2.1 through 4.0 on an i5-3337U: "10x2x1M ~4500MB/s", against about 7800 MB/s for klauspost's Go library. The README adds: *"Versions >= 4.0.0 have not been benchmarked thoroughly yet"*.

**Panic audit [V].**

- The RS API returns `Result`. That includes `IncorrectShardSize`, `TooFewShardsPresent`, `InvalidIndex` and the `ShardByShard` `SBSError::TooManyCalls`.
- Several `pub` field helpers panic: `galois_8::div` panics with *"Divisor is 0"* (`galois_8.rs:77`); `galois_8::mul_slice` and `mul_slice_xor` do `assert_eq!(input.len(), out.len())` (`galois_8.rs:141–317`); `galois_16.rs` has `panic!`s on divide or invert by 0 (lines 244, 287, 313). I did not check whether those functions are `pub`.
- Internal: `core.rs:209,227` unwraps after `ShardByShard` checks, and `core.rs:722` does `sub_matrix.invert().unwrap()`, which holds by the MDS invariant.

**Maintenance.** No commits since 2022-11-11.

### B1.3 `reed-solomon-novelpoly` 2.0.0 (paritytech)

**[V]**

- README: *"Runs encoding and reconstruction in O(n lg(n)) … for small number n there is a static offset due to a walsh transform over the full domain in reconstruction"*. The stated goal is *"Be really fast for n > 100"*.
- `CodeParams::derive_parameters(n, k)` (`src/novel_poly_basis/mod.rs:43-60`) sets `k = next_lower_power_of_2(k)` and `n = next_higher_power_of_2(n)`. It errors if n exceeds 65536.
- The API encodes a whole byte payload into shards and reconstructs the payload. This is Polkadot's availability-store model.
- The AVX path is compile-time only (`src/lib.rs:15`, `src/field/*`).
- An optional C++ alternative implementation is built with `cc` and `bindgen`.

**[Analysis].** Mantle needs RS(10,4) to mean exactly 10 data chunks laid out systematically. This crate cannot express that, so it is unsuitable.

### B1.4 Others

- **`leopard-codec` 0.2.0 [V].** The README checklist leaves GF(2^16) unchecked, so only GF(2^8) is implemented. There is no `std::arch` or SIMD; the only `unsafe` is `get_unchecked` in the lookup tables (`lut.rs`). It requires `shard_size % 64 == 0` (`InvalidShardSize`) and parity ≤ data.
- **`rustfs-erasure-codec` 9.0.0 [V].**
  - Edition 2024, `rust-version = "1.96"`.
  - README: *"classic Reed-Solomon over GF(2^8) and GF(2^16) … Leopard GF8 and Leopard GF16 … runtime-dispatched SIMD backends for galois_8"*. *"Runtime dispatch is guarded. Unsupported ISAs fall back to scalar execution."*
  - `build.rs` only generates tables; there is no C.
  - [Analysis] It is feature-rich but very young and churning, with three semver-major releases in three months. Watch it, but do not adopt it now.
- **ISA-L bindings [V].** `erasure-isa-l-sys` 1.1.0 vendors `isa-l`. Its build script tries the system `libisal` first and *"fall[s] back to building from source"*. [Analysis] Useful as a benchmark baseline. As a dependency it brings a C/asm toolchain.

## B2. CRC crates, and what S3 compatibility requires

### B2.1 What an S3-compatible API needs [V]

Sources: AWS S3 User Guide pages "Checking object integrity in Amazon S3" and "Checking object integrity for data uploads", fetched 2026-09-28.

**Algorithms.** CRC-64/NVME (`CRC64NVME`, the default), CRC-32, CRC-32C, SHA-1, SHA-256, MD5, XXHash64, XXHash3, XXHash128, and SHA-512. *"If you don't specify a checksum algorithm and the SDK also doesn't calculate a checksum for you, then S3 automatically chooses the CRC-64/NVME (CRC64NVME) checksum algorithm."*

**Multipart checksum types:**

| Algorithm | Full object | Composite |
|---|---|---|
| CRC64NVME | Yes | No |
| CRC32 | Yes | Yes |
| CRC32C | Yes | Yes |
| SHA1, SHA256, MD5, XXHASH64, XXHASH3, XXHASH128, SHA512 | No | Yes |

- **Full-object checksums depend on CRC combine:** *"Full object checksums in multipart uploads are only available for CRC-based checksums because they can linearize into a full object checksum … S3 can compute the checksum of the whole object from the part-level checksums."*
- **Composite checksums** *"aggregate[] the part-level checksums (from the first part to the last) to produce a single, combined checksum"*. Part numbers must be consecutive starting at 1, or the request fails with HTTP 500.
- **Multipart ETag:** *"Amazon S3 concatenates the bytes for the MD5 digests together and then calculates the MD5 digest of these concatenated values … adds a dash with the total number of parts."*
- **Trailing checksums:** the `x-amz-trailer` values are crc32, crc32c, crc64nvme, sha1 and sha256. The value is *"a base64 encoding of the big-endian checksum value"*.

**Parameters** (from the `crc-catalog` 2.5.0 source, `src/algorithm.rs`):

- CRC-32/ISCSI (= CRC-32C): poly 0x1edc6f41, init 0xffffffff, reflected in and out, xorout 0xffffffff, check 0xe3069283 (line 2270).
- CRC-64/NVME: poly 0xad93d23594c93659, init and xorout all-ones, reflected in and out, check 0xae8b14860a799888 (line 2500).

**Implication [Analysis].** Mantle needs CRC32, CRC32C and CRC64NVME with **combine** (checksum of A‖B from crc(A), crc(B) and len(B)). It needs MD5 for ETags and Content-MD5, and SHA-256 for SigV4 payload hashing (`x-amz-content-sha256`) and for SHA256 checksums.

### B2.2 CRC crate comparison

| Crate | Latest (date), MSRV | Algorithms | x86_64 acceleration | aarch64 acceleration | Dispatch | Combine | Notes |
|---|---|---|---|---|---|---|---|
| **crc-fast** | 1.10.0 (2025-12-31), MSRV 1.89 | all catalogued CRC-16/32/64 + custom | `avx512-vpclmulqdq` → `avx512-pclmulqdq` → `sse-pclmulqdq`; CRC-32C "fusion" with native `crc32` | `neon-pmull-sha3` (EOR3) → `neon-pmull`; fusion with native CRC32C | runtime (`is_x86_feature_detected!` / `is_aarch64_feature_detected!` in `src/feature_detection.rs:144-196`) | `checksum_combine(alg, crc1, crc2, len2)` (`src/lib.rs:1054`) plus `*_with_params` | default features `std, panic-handler, ffi`; Miri + libFuzzer per README; dependents include `aws-smithy-checksums`, `object_store`, `garage_api_s3`, `s3s`, `minio` |
| crc32c | 0.6.8 (2024-06-09) | CRC-32C only | SSE4.2 `_mm_crc32_u64` (`hw_x86_64.rs:46-54`); no PCLMULQDQ | `__crc32cd` behind `cfg(armsimd)` (rustc ≥ 1.80) (`hw_aarch64.rs:46-62`, `build.rs:187-197`) | runtime (`lib.rs` `crc32c_append`) | `crc32c_combine(crc1, crc2, len2)` | small and simple; repo last commit 2025-12-11 |
| crc32fast | 1.5.2 (2026-09-12), MSRV 1.63 | CRC-32 (IEEE) only | PCLMULQDQ; VPCLMULQDQ (AVX2/AVX-512) with Rust ≥ 1.89 | ARMv8 `crc32` instructions, 3-way interleaved | runtime (`Hasher::new`) | `Hasher::combine(&mut self, &other)` | `no_std` disables SIMD |
| crc64fast-nvme | 1.2.1 (2025-11-14) | CRC-64/NVME | PCLMULQDQ; VPCLMULQDQ nightly-only | PMULL | runtime | none found | **README: "⚠️ DEPRECATED - No Longer Maintained … We recommend using crc-fast-rust instead"** |
| crc (crc-rs) | 3.4.0 (2025-11-26), MSRV 1.83 | all catalogued (via `crc-catalog`) | none (table / slice-by-16) | none | n/a | none found | baseline only |

**crc-fast README performance [V].** "1KiB / 1MiB, GiB/s". The README does not state whether these are single-threaded.

| CPU (target) | CRC-32/ISCSI | CRC-64/NVME |
|---|---|---|
| Intel Sapphire Rapids, c7i.metal-24xl (avx512-vpclmulqdq) | ~61 / ~111 | ~28 / ~88 |
| AMD Genoa, c7a.metal-48xl (avx512-vpclmulqdq) | ~26 / ~54 | ~22 / ~55 |
| AWS Graviton4, c8g.metal-48xl (neon-pmull-sha3) | ~23 / ~54 | ~28 / ~41 |
| AWS Graviton2, c6g.metal (neon-pmull) | ~11 / ~17 | ~11 / ~16 |
| Apple M3 Ultra (neon-pmull-sha3) | ~60 / ~99 | ~58 / ~72 |
| Apple M4 Max (neon-pmull-sha3) | ~56 / ~94 | ~52 / ~72 |

The README also says: *"The crc crate … is ~0.5GiB/s by default."* The old crc32fast README table (v1.0.0) lists 7314 MB/s for pclmulqdq against 207 MB/s for crc 1.8.1.

**crc-fast panic audit [V].**

- `checksum(CrcAlgorithm::Crc32Custom | Crc64Custom | CrcCustom, …)` panics with *"Custom CRC… requires parameters via CrcParams::new()"* (`src/lib.rs:856-881`, `1257-1269`). A caller can reach this by passing those enum variants.
- Custom-width asserts: `combine.rs:114,158`, `algorithm.rs:129`.
- Built-in algorithms and `checksum_combine` for built-in algorithms have no panic path.
- The `#[panic_handler]` is compiled only with `not(feature = "std")` (`src/lib.rs:146-154`).

**crc32c panic audit [V].** The only hits are `.unwrap()` on a 3-block iterator inside the hardware loops (`hw_x86_64.rs:92-94`, `hw_aarch64.rs:80-82`), which the loop structure guarantees.

## B3. MD5 and SHA-256

| Crate | Latest (date), MSRV | MD5? | SHA-256 hardware path | Dispatch | Notes |
|---|---|---|---|---|---|
| **sha2** (RustCrypto) | 0.11.0 (2026-03-25), MSRV 1.85 | — | x86/x86_64 SHA-NI (`x86_sha`, needs sha+sse2+ssse3+sse4.1); aarch64 ARMv8 SHA2 (`aarch64_sha2`); also riscv-zknh, loongarch64 asm, wasm simd128 | runtime via `cpufeatures::new!(shani_cpuid, "sha","sse2","ssse3","sse4.1")` and `cpufeatures::new!(sha2_hwcap, "sha2")` (`src/sha256.rs:52-77`); force with `--cfg sha2_backend="soft"` etc. | pure Rust + intrinsics; the 0.11 line has **no `asm` feature** (`sha2-asm` 0.6.4 dates from 2024-05-07 and belongs to 0.10) |
| **md-5** (RustCrypto) | 0.11.0 (2026-03-27), MSRV 1.85 | yes | n/a | `src/compress.rs`: `soft` on every arch except loongarch64 asm | no x86/aarch64 acceleration in 0.11 (`md5-asm` 0.5.2, from 2024, was the 0.10 `asm` feature); README: *"MD5 is cryptographically broken"* |
| md5 (stainless-steel) | 0.8.1 (2026-07-09) | yes | n/a | — | alternative pure-Rust implementation |
| ring | 0.17.14 (2025-03-11), MSRV 1.66 | **no** (`digest` offers SHA1_FOR_LEGACY_USE_ONLY, SHA256, SHA384, SHA512, SHA512_256; `src/digest.rs:403-491`) | ARMv8 `Sha256` → `sha256_block_data_order_hw`; x86_64 `(Sha, Ssse3)` → SHA-NI path (`sha256rnds2` in `crypto/fipsmodule/sha/asm/sha512-x86_64.pl:526-582`); else NEON/AVX/SSSE3/nohw | runtime (`src/digest/sha2/sha2_32.rs:31-50`) | BoringSSL/CRYPTOGAMS perlasm |
| aws-lc-rs | 1.18.1 (2026-09-01), MSRV 1.71 | **no** in `digest` (SHA1, SHA224, SHA256, SHA384, SHA512, SHA512_256, SHA3-*; `src/digest/sha.rs:63-189`) | AWS-LC assembly (**UNVERIFIED** here: which SHA-256 kernels AWS-LC selects; the `aws-lc-sys` sources were not inspected) | AWS-LC runtime dispatch (**UNVERIFIED**, not inspected) | README: non-FIPS builds never need CMake, bindgen or Go; FIPS builds need CMake and Go; binds AWS-LC-FIPS 4.x; a C compiler is still needed |

**[Analysis] MD5 has no hardware path in any of these options.**

- What I verified: none of the inspected Rust crates contains an x86_64 or aarch64 MD5 kernel beyond scalar code, and neither ring nor aws-lc-rs exposes MD5.
- What I did not verify: I did not check the x86 or Arm ISA manuals in this pass. To my knowledge neither ISA has MD5 instructions; the SHA extensions cover SHA-1 and SHA-2 only.
- Consequence: MD5 is a scalar, serial loop in every option. See B4 for magnitudes.

## B4. Local measurement (for scale, not as evidence about the Rust crates)

**Setup.** Apple M5 Max (`sysctl`: FEAT_SHA256=1, FEAT_CRC32=1, FEAT_PMULL=1, FEAT_SHA3=1). Homebrew OpenSSL 3.6.4. Command: `openssl speed -seconds 2 -bytes 1048576 -evp {sha256,md5,sha1}`. Single thread, one run, 2026-09-28.

**Results:**

- SHA-256: 3,295,674 kB/s (≈ 3.3 GB/s).
- MD5: 935,815 kB/s (≈ 0.94 GB/s).
- SHA-1: 3,256,382 kB/s.

**Caveats.** This is n = 1 on one machine, using OpenSSL rather than the Rust crates. It only shows that, where SHA-2 instructions exist, SHA-256 runs about 3.5× faster than MD5, and MD5 is roughly 1 GB/s per core. **UNVERIFIED:** the Rust crates' throughput on mantle's target servers.

---

# R. Recommendations for mantle

Every recommendation carries a label: **[Evidence]** when it restates or directly applies a verified source, **[Analysis]** when it is my own synthesis. The cluster size in *failure domains* (racks, or whatever the operator declares as the top correlated domain) is the main input that gates which profiles are allowed.

## R1. Erasure-coding schemes and parameters

### R1.1 Block lifecycle

1. **Open blocks are replicated.** Write them as 3-way replicated *partial-block quorum appends*: acknowledge at 2 of 3, and commit the post-append length and checksum to block metadata before acknowledging. Allow a single appender per block. **[Evidence: Tectonic §5.2; WAS §1]**
2. **Seal, then re-encode.** On seal (size threshold, time threshold, or writer close), re-encode the block to its RS profile. Each target does one large write. Delete the replicas only after verification. **[Evidence: Tectonic §5.2 "reencodes the block … once the block is sealed"; WAS §1 "erasure codes a sealed extent lazily … original 3 full copies … are deleted"]**
3. **Verify after every encode, re-encode, and repair.** In memory, decode a sample of erasure patterns: every single-chunk loss, plus random multi-chunk losses up to m. Compare the result with the pre-encode CRC32C of the block. If anything fails, abort and keep the replicas. **[Evidence: Azure §4.4]**
4. **Full-block writers may encode client-side** (large sequential PUTs, multipart parts ≥ block size). They use *reservation-hedged* writes: reserve n+Δ targets in distinct failure domains, write to the first n that accept, and acknowledge at n (or n−1 when m ≥ 3, repairing the straggler offline). **[Evidence: Tectonic §5.1, which uses Δ = 4 and acknowledges at 14/15 for RS(9,6)]** Acknowledging at n−1 when m ≥ 3 is **[Analysis]**: it temporarily consumes one unit of margin.

### R1.2 Profiles to ship

Mantle should implement *systematic MDS RS(k, m)* in general. It should expose and test only the named profiles below. Every profile requires **one chunk per failure domain**: n ≤ #failure domains. **[Evidence: Azure §3.2 caps width by fault-domain count; Ford §5.2 rack-aware ≈ 3× stripe MTTF; Tectonic §3.2 chunks in different fault domains; f4 uses 14 racks for n = 14]**

| Profile | Overhead | Tolerates | Needs ≥ domains | When | Evidence |
|---|---|---|---|---|---|
| R3 | 3.0× | 2 | 3 | Open blocks; tiny clusters (< 9 domains); small or hot objects | Tectonic §5.2, WAS §1. Ford Table 3: RS(4,2) (1.80E6 d) is *less* available than R=3 (6.82E6 d) under correlated failure, so narrow EC is not an upgrade on small clusters |
| **RS(6,3)** | 1.5× | 3 | 9 | **Default for 9–11 domains** | Ford Table 3: 1.03E7 d vs R=3 6.82E6 d at a quarter of the overhead. It is Azure's RS comparison point (Table 1) |
| **RS(8,4)** | 1.5× | 4 | 12 | **Default for ≥ 12 domains** | Ford Table 3: 5.11E7 d, the best 50%-overhead code in the table and 7.5× R=3. Repair reads 8 chunks rather than 10. A6.3: lower loss probability than RS(10,4) at equal copyset count |
| RS(10,4) | 1.4× | 4 | 14 | Opt-in "capacity" profile; requires copyset-constrained placement (R2) | Production-proven at 1.4×: HDFS-RAID (XORing Elephants), f4 (14 racks), Tectonic blob re-encode. **Caveat:** Ford's nearest data point, RS(9,4) at 44%, was *worse* than R=3 under correlated failures (2.39E6 d); Ford does not evaluate RS(10,4) itself (UNVERIFIED) |
| RS(9,6) | 1.67× | 6 | 15 | Opt-in "durable" profile for long-lived or critical data | Tectonic §5.1 ("Long-lived data is typically RS(9,6)"). A6.3: 0.39% loss probability even with random placement at 10⁸ stripes |
| *(v2)* LRC(12,2,2) | 1.33× | any 3, plus decodable ≥ 4-failure patterns | 16 (Azure uses 20 racks) | Large clusters where repair traffic and degraded-read latency dominate | Azure §2.4, §3.3, §5; requires an MR coefficient construction and decodability checker (Azure §2.2.1–2.2.3) and group-aware placement (Azure §4.3) |
| *(v3, optional)* Hitchhiker over the chosen RS | same as RS | same as RS | same | If repair traffic becomes the bottleneck and wider LRC stripes cannot be placed | Rashmi 2014: 25–45% less repair traffic and I/O, same storage, still MDS |

**Why RS(8,4) and not RS(10,4) as the large-cluster default [Analysis].**

- **Correlated failures.** Ford's Table 3 is the only peer-reviewed comparison in this set that models correlated failures. At equal overhead it favours a larger m: RS(8,4) ≫ RS(6,3) ≫ RS(4,2), all at 50%. It penalizes widening k at fixed m: RS(9,4) < R=3.
- **Cost.** RS(8,4) costs 7% more raw capacity than RS(10,4) (1.5× vs 1.4×). It cuts single-chunk repair reads by 20% (8 reads instead of 10), which matters because single-chunk repairs are 98% of repairs (A4.3). It needs 2 fewer failure domains.
- **Keeping RS(10,4).** RS(10,4) stays available because it is the most widely deployed blob code in this literature. It should only be allowed together with the copyset placement in R2, which A6.3 shows lowers its simultaneous-failure loss probability by several orders of magnitude.

### R1.3 Stripe geometry and codec engineering

- **Contiguous layout.** Each data chunk holds a contiguous 1/k of the block, not striped cells. Most range reads are then a single direct IO, and reconstruction happens only on failure or hedge. **[Evidence: Tectonic §6.4]**
- **Block size.** Use tens to hundreds of MiB, with each chunk = block/k. **[Analysis]** Reference points: Tectonic benchmarks 72 MB RS(9,6) blocks (§5.1, Fig. 3a); f4 uses 1 GB blocks, trading metadata volume against rebuild cost (§5.3, "Individual f4 Cell"); WAS seals extents at about 1 GB (§1). Mantle should choose the block size together with the metadata-store budget (one chunk list per block) and the repair granularity.
- **Record the codec in metadata.** Store `codec_id` (library, field or basis, construction, version) with every block. Parity is not portable across RS libraries (A1.2, A2). **[Analysis]**
- **Chunk-size alignment.** Make chunk byte-lengths multiples of 64. **[Evidence: reed-solomon-simd README, "if your shard size is a multiple of 64, it remains compatible across all versions"; Leopard legacy API requires multiples of 64]**
- **Integrity is separate from coding.** RS crates do not detect corruption (reed-solomon-simd README). Store a CRC32C per sub-chunk. 64 KiB is an **[Analysis]** choice; WAS uses per-append-block CRCs checked on every read (§4.4). Also store a whole-block CRC in metadata, verify on every read and repair, retry with other chunk subsets on mismatch (Azure §4.4), and scrub continuously. GFS scrubbing found mismatches in 1 in 10⁶–10⁷ older blocks (Ford §3.1).
- **Exhaustive decode tests in CI.** For every shipped profile, decode all erasure patterns of size ≤ m. **[Evidence of necessity: Plank & Ding 2003/2005]**

## R2. Placement policy

1. **Explicit placement recorded in metadata**, not CRUSH-computed. **[Evidence: Tectonic §3.3/§3.5; rationale in A7]**
   - The block record holds an ordered `chunk_index → disk_id` list.
   - A reverse index `disk_id → blocks` drives repair and drain.
   - A repaired chunk keeps its index. **[Evidence: CRUSH §3.2.2 "Replica Ranks"]**
2. **Failure-domain hierarchy:** disk ⊂ host ⊂ rack ⊂ (row/power), plus an orthogonal *upgrade-domain* label.
   - Hard constraint: at most one chunk per stripe per rack (or per the top declared domain).
   - Maintenance constraint: a rolling upgrade or drain batch may take at most one chunk of any stripe offline; this can be relaxed to m−1 in an emergency.
   - **[Evidence: Azure §4.3; Ford §5.2 and §10, since planned reboots dominate unavailability]**
3. **Copyset-constrained placement via consistent shuffles.** **[Evidence: Tectonic §3.5; Cidon 2013 §4; parameters are Analysis]**
   - Keep *P* random permutations of the eligible disks per storage class. Arrange them (with rejection or repair) so that every window of n consecutive disks satisfies the domain constraints. Tiered Replication's greedy incremental algorithm handles joins, leaves and extra constraints **[Evidence: Cidon 2015, Alg. 1]**.
   - Each block hashes to candidate copysets.
   - Scatter width is S ≈ P(n−1). *P* sets the trade-off Cidon describes: larger *P* gives faster, more parallel rebuilds but more distinct stripe sets, hence a higher chance that a correlated event destroys some stripe.
   - **Default P = 10** (S ≈ 80–140 for n = 9–15). Revisit with the simulator in R6. A6.3 shows P = 10 keeps the loss probability for a simultaneous 1% node loss at ≤ 0.6% for RS(6,3) and ≤ 0.06% for RS(8,4)/RS(10,4) with N = 5000, versus ≈ 46–100% for random per-stripe placement at 10⁷–10⁸ stripes. Tectonic uses about 100 shuffles.
   - Copysets are *best-effort* under churn, and the rebalancer restores membership over time. **[Evidence: Tectonic §3.5]**
4. **Load and free-space balancing with two choices.** Draw two candidate copysets (or, for single-chunk placement, two feasible disks). Pick the one with the better score (free-space fraction, recent queue depth/latency). Never pick "the global least-loaded" from stale statistics. **[Evidence: Mitzenmacher 2001 (d = 2 gets most of the benefit); Mitzenmacher 2000 (stale information makes least-loaded harmful); Dean & Barroso (probing herds clients)]** The score function is **[Analysis]**.
5. **Background services**, mirroring Tectonic §3.5:
   - a *rebalancer*, which decides moves on failures, capacity additions and drains, preserving copysets and domain constraints;
   - a *repair service*, which reconciles each disk's reverse index against the block records;
   - a *disk inventory* and *health checker*;
   - *scrubbers*.

## R3. Repair prioritization

1. **Priority queue keyed by remaining margin** μ = (readable chunks − k), ascending. μ = 0 is critical. Break ties by the size of the current failure burst and by data class. **[Evidence: Ford §5.1 "prioritize reconstruction of stripes which have lost the most chunks"]**
2. **Handle transient failures with a delay**, but only when there is margin to spend.
   - Start repair immediately for μ ≤ 1.
   - Otherwise wait for a delay *D*: 15 min by default (GFS). Make it dynamic by failure class: planned reboot, process restart, disk failure, host failure, rack event. Adjust it using recent failure history.
   - **[Evidence: Ford §1 (<10% of events exceed 15 min; GFS waits 15 min) and §10 ("dynamic delay … based on failure classification and recent history")]**
3. **Detect failures quickly.** With correlated failures, faster recovery helps little: a 90% faster recovery gives only 6% less unavailability. Once past a single failure, repair rates are dominated by detection and trigger time. So invest in detection latency for multi-failure stripes, and in placement, before raw repair bandwidth. **[Evidence: Ford §8.2; Azure §3.1.1]**
4. **Rate limits.**
   - Keep separate token buckets for repair versus client I/O, per disk, per NIC and per rack uplink.
   - Budgets rise automatically while μ = 0 stripes exist.
   - Erasure coding of newly sealed blocks is scheduled so it keeps pace with ingest.
   - **[Evidence: Ford §5.1 (explicit bandwidth trade-off, operator-adjustable rate limits); Azure §4.4 (all I/O types throttled; EC must keep up with ingest)]**
5. **Choose repair sources to spread load.** Pick the k survivors with two-choice load selection. Place the new chunk under the same constraints as R2. After writing, verify its CRC. **[Evidence: Azure §4.4 for CRC verification; the rest is Analysis]**
6. **Size for repair traffic.** Plan for repair traffic in the order of Facebook's 180 TB/day of cross-rack RS repair on a multi-thousand-node cluster **[Evidence: Hitchhiker §1]**. This motivates LRC in v2 and Hitchhiker in v3.

## R4. Hedged and tied read policy

The primitive for all of the following is a **per-request-class latency histogram** kept per storage class and per node. The hedge threshold is the class p95. **[Evidence: Dean & Barroso, "defer … until … the 95th-percentile expected latency … limits the additional load to approximately 5%"]**

| Read shape | Primary action | Tail action | Guards |
|---|---|---|---|
| Replicated block (open, or R3 profile) | Read one replica, chosen by two choices | Either a hedge at p95 to another replica, or a **tied request**: send to two replicas about 1 ms apart with cross-cancellation. Tied requests suit the disk-queueing-dominated case. **[Evidence: Dean & Barroso Table 2: −16% p50, −38% p99.9]** | Tag hedges low-priority; cancel the loser |
| RS block, range inside one data chunk (common case) | Direct read of that chunk **[Evidence: Tectonic §6.4]** | At p95, a **reconstruction read** of the same byte range from k other chunks (two-choice selection, excluding probation nodes), sent low-priority | Global **reconstruction budget ≤ 10% of reads** (Tectonic §6.4). Per-node admission control rejects hedges when overloaded (Azure §4.4). Disabled when the budget is exhausted, to prevent a *reconstruction storm* |
| RS read that already needs k chunks (degraded, full-stripe, repair source) | Read **k + 1**, decode from the first k | — | **[Evidence: Azure §5.1 (13-of-15 = 151 ms vs 12-of-15 = 305 ms); EC-Cache (Δ = 1 at k = 10 gives >50% tail reduction at ≤10% extra bandwidth)]** Use Δ = 0 when bandwidth-bound: large ranges or NIC utilization above a threshold **[Evidence: Azure §5.2, where extra fragments "hurts latency" for 4 MB reads on 1 Gbps NICs]** |
| Chronically slow disk or node | — | **Latency-induced probation**: stop sending primaries, keep sending shadow requests until healthy **[Evidence: Dean & Barroso]** | Automatic exit after N healthy shadow responses **[Analysis]** |
| Full-block RS writes | Reservation-hedged writes: reserve n+Δ targets, write to the first n **[Evidence: Tectonic §5.1]** | — | Do not duplicate payloads |

Hedging is not used for metadata mutations; consistency comes from the metadata store's own quorum protocol. **[Evidence: Dean & Barroso "Mutations" section]**

## R5. Crates

| Need | Choice (pin at) | Why | Configuration and guardrails |
|---|---|---|---|
| RS codec | **`reed-solomon-simd` 3.1.0** | Pure Rust. Runtime SIMD dispatch (SSSE3/AVX2/NEON via `cpufeatures`, scalar fallback), so one portable binary. MDS systematic Leopard codec. `Result`-based API with no user-reachable panics found in the high-level API. Reusable encoder/decoder (`reset` reuses buffers). Maintained (2025-10 release, repo active 2026-09). License MIT AND BSD-3-Clause. | Use only `ReedSolomonEncoder` / `ReedSolomonDecoder` behind a mantle `Codec` trait carrying `codec_id`. Chunk sizes are multiples of 64 B. Keep one encoder and decoder per worker thread and reuse them. **Acceptance gate:** benchmark encode and decode (1..m erasures) at 6:3, 8:4, 10:4 and 9:6 with 64 KiB, 1 MiB and 8 MiB chunks, on x86_64 (AVX2 and AVX-512 hosts) and aarch64 (Graviton-class), against `reed-solomon-erasure` + `simd-accel` (built with `RUST_REED_SOLOMON_ERASURE_ARCH` matched to the host) and ISA-L via `erasure-isa-l`. This matters because the crate's README says it is *not* fastest at decoding with ≤ ~42 recovery shards. If decode falls short of disk or NIC rates, fall back to a small in-house GF(2^8) split-table (PSHUFB/TBL) matrix codec (FAST'13 technique). Rejected: `reed-solomon-erasure` (unmaintained since 2022; `-march=haswell` compile-time SIMD; panicking `pub` helpers), `reed-solomon-novelpoly` (power-of-two n and k), `leopard-codec` (GF(2^8) only, no SIMD), `rustfs-erasure-codec` (too new and volatile). |
| CRC32C | **`crc-fast` 1.10.0** | Fastest verified option. Its README reports CRC-32C at 1 MiB from ~17 GiB/s (Graviton2) up to ~111 GiB/s (Sapphire Rapids). Runtime dispatch to VPCLMULQDQ / PCLMULQDQ / PMULL(+EOR3) with native CRC32C fusion. Has `checksum_combine`, which S3 full-object CRCs and chunk→block CRC rollups need. One crate covers CRC32C, CRC32 and CRC64-NVME. Adopted by `aws-smithy-checksums`, `object_store`, `garage_api_s3`, `s3s`, `minio`. Miri and fuzz tested per its README. | `default-features = false, features = ["std"]` to drop `ffi`; `std` implies `alloc`, which combine needs. Expose only `Crc32Iscsi`, `Crc32IsoHdlc` and `Crc64Nvme` through a mantle enum, so the `*Custom` panic paths are unreachable. Fallback: `crc32c` 0.6.8 (SSE4.2 / ARMv8-CRC, has `crc32c_combine`, no PCLMULQDQ folding). |
| CRC32 (IEEE) | `crc-fast` (same crate) | as above | Alternative: `crc32fast` 1.5.2 (`Hasher::combine`, runtime PCLMULQDQ/VPCLMULQDQ/ARMv8-CRC). |
| CRC64-NVME | **`crc-fast` 1.10.0** | S3's default checksum. `crc64fast-nvme` is marked **deprecated** by its authors in favour of `crc-fast`. `crc`/`crc-catalog` are table-driven at ~0.5 GiB/s with no combine. | Test against the catalogue check value 0xae8b14860a799888. |
| MD5 | **`md-5` 0.11.0** (RustCrypto) | Needed only for S3 compatibility: single-part ETag, `Content-MD5`, `x-amz-checksum-md5`, and the multipart "MD5-of-MD5s-N" ETag. Pure Rust. Neither ring nor aws-lc-rs offers MD5. | Never use it for integrity; CRC32C and SHA-256 do that. It runs at roughly 1 GB/s per core (B4, OpenSSL on M5 Max), so run it in parallel with the disk write, and per part for multipart uploads. Compute it only where the API semantics require it. |
| SHA-256 | **`sha2` 0.11.0** (RustCrypto) | Pure Rust plus intrinsics, runtime SHA-NI and ARMv8-SHA2 dispatch via `cpufeatures`, no C toolchain. Needed for SigV4 payload hashing and `x-amz-checksum-sha256`. | If `aws-lc-rs` is already in the dependency graph (for example for TLS), `aws_lc_rs::digest::SHA256` is an acceptable single implementation; do not ship both. `ring` 0.17 is also acceptable, but it has no MD5. |

**MSRV consequence [Analysis].** crc-fast requires Rust 1.89 and sha2/md-5 0.11 require 1.85, so mantle's MSRV must be at least 1.89. crc-fast follows a stable-minus-2 policy, and each MSRV bump is a minor version.

## R6. Validation work before these defaults are frozen [Analysis]

1. **Codec benchmark gate** (R5, row 1).
2. **Durability simulator.** Implement Ford's burst model (120 s windows, rack affinity, 15-min threshold) plus Cidon's combinatorics, using mantle's copyset placer. Sweep P, profile and cluster size (number of domains and nodes). Choose the default P and confirm that RS(8,4) beats RS(10,4) under mantle's expected topologies. This decides whether §R1.2's defaults hold.
3. **Staging calibration.** Tune the p95 hedge thresholds per request class, the reconstruction budget (starting at 10%), the Δ = 1 cutoffs, and the repair token-bucket sizes.
4. **Fault-injection tests.** Cover rack loss, simultaneous random 1% host loss, and rolling-upgrade batches. For each, verify: no stripe drops below μ = 0; no stripe drops below μ = 1 during planned maintenance; the repair queue drains in priority order.

## R7. Open questions for the owner

- **Target deployment sizes.** How many failure domains (racks) and nodes? This gates which profiles are legal (R1.2).
- **Multi-site.** Is geo-replication in scope? If so, f4's XOR-across-regions (2.1× total with RS(10,4)) and Ford's multi-cell results (§8.5) become relevant.
- **Target CPUs.** x86_64 only, or aarch64 too? This decides where the R5 benchmark gate runs.

---

## Consolidated UNVERIFIED items

1. **Blömer et al., ICSI TR-95-048.** Primary PDF not retrievable (ICSI FTP and mirrors return errors). Described via Plank et al. FAST'09 §2.2.
2. **Plank 1997 tutorial: exact bound on n + m versus w.** The math glyphs do not extract. The peer-reviewed restatement (FAST'09 §2.1: n ≤ 2^w + 1; w = 8 used for fewer than 256 disks) is cited instead.
3. **Proceedings page ranges** for Huang et al. (ATC'12) and Ford et al. (OSDI'10). Neither PDF has page footers, and DBLP lookups timed out.
4. **Four-failure fraction for LRC(12,2,2).** The Azure paper gives 86% only for LRC(6,2,2).
5. **RS(10,4) under Ford's correlated-failure model.** Not evaluated in the paper; RS(9,4) is the nearest data point.
6. **Rust codec throughput at mantle's shapes** (6:3, 8:4, 10:4, 9:6) and chunk sizes. No published numbers were found, and none was measured.
7. **Throughput of the md-5, sha2, ring and aws-lc-rs crates** on mantle's target servers. Only OpenSSL was measured locally (B4).
8. **aws-lc-rs SHA-256 kernel selection and dispatch.** The `aws-lc-sys` C/asm sources were not inspected.
9. **ISA-level absence of MD5 instructions** on x86 and Arm. Not checked against the ISA manuals in this pass.
10. **Whether crc-fast's README benchmarks are single-threaded.** The README does not say.

## References (primary sources consulted)

1. J. S. Plank. "A Tutorial on Reed-Solomon Coding for Fault-Tolerance in RAID-like Systems." *Software—Practice & Experience* 27(9):995–1012, 1997. DOI 10.1002/(SICI)1097-024X(199709)27:9<995::AID-SPE111>3.0.CO;2-6. TR CS-96-332 copy.
2. J. S. Plank, Y. Ding. "Note: Correction to the 1997 Tutorial on Reed-Solomon Coding." *SP&E* 35(2):189–194, 2005. DOI 10.1002/spe.631. TR UT-CS-03-504 (2003).
3. J. Blömer, M. Kalfane, R. Karp, M. Karpinski, M. Luby, D. Zuckerman. "An XOR-Based Erasure-Resilient Coding Scheme." ICSI TR-95-048, 1995. *(Not retrieved.)*
4. J. S. Plank, J. Luo, C. D. Schuman, L. Xu, Z. Wilcox-O'Hearn. "A Performance Evaluation and Examination of Open-Source Erasure Coding Libraries for Storage." *FAST '09*, pp. 253–265.
5. J. S. Plank, K. M. Greenan, E. L. Miller. "Screaming Fast Galois Field Arithmetic Using Intel SIMD Instructions." *FAST '13*, pp. 299–306.
6. S.-J. Lin, W.-H. Chung, Y. S. Han. "Novel Polynomial Basis and Its Application to Reed-Solomon Erasure Codes." *FOCS 2014*, pp. 316–325. DOI 10.1109/FOCS.2014.41. arXiv:1404.3458.
7. S.-J. Lin, T. Y. Al-Naffouri, Y. S. Han, W.-H. Chung. "Novel Polynomial Basis With Fast Fourier Transform and Its Application to Reed–Solomon Erasure Codes." *IEEE Trans. Inf. Theory* 62(11):6284–6299, 2016. DOI 10.1109/TIT.2016.2608892.
8. C. A. Taylor (catid). Leopard-RS, `github.com/catid/leopard`: README, `leopard.h`, `Benchmarks.md`, master as of 2026-09-28.
9. C. Huang, H. Simitci, Y. Xu, A. Ogus, B. Calder, P. Gopalan, J. Li, S. Yekhanin. "Erasure Coding in Windows Azure Storage." *USENIX ATC '12*.
10. M. Sathiamoorthy, M. Asteris, D. Papailiopoulos, A. G. Dimakis, R. Vadali, S. Chen, D. Borthakur. "XORing Elephants: Novel Erasure Codes for Big Data." *PVLDB* 6(5):325–336, 2013. DOI 10.14778/2535573.2488339.
11. A. G. Dimakis, P. B. Godfrey, Y. Wu, M. J. Wainwright, K. Ramchandran. "Network Coding for Distributed Storage Systems." *IEEE Trans. Inf. Theory* 56(9):4539–4551, 2010. DOI 10.1109/TIT.2010.2054295. Text read from arXiv:0803.0632.
12. K. V. Rashmi, N. B. Shah, D. Gu, H. Kuang, D. Borthakur, K. Ramchandran. "A 'Hitchhiker's' Guide to Fast and Efficient Data Reconstruction in Erasure-coded Data Centers." *SIGCOMM '14*, pp. 331–342. DOI 10.1145/2619239.2626325.
13. K. V. Rashmi, N. B. Shah, D. Gu, H. Kuang, D. Borthakur, K. Ramchandran. "A Solution to the Network Challenges of Data Recovery in Erasure-coded Distributed Storage Systems: A Study on the Facebook Warehouse Cluster." *HotStorage '13*.
14. D. Ford, F. Labelle, F. I. Popovici, M. Stokely, V.-A. Truong, L. Barroso, C. Grimes, S. Quinlan. "Availability in Globally Distributed Storage Systems." *OSDI '10*.
15. A. Cidon, S. M. Rumble, R. Stutsman, S. Katti, J. Ousterhout, M. Rosenblum. "Copysets: Reducing the Frequency of Data Loss in Cloud Storage." *USENIX ATC '13*, pp. 37–48.
16. A. Cidon, R. Escriva, S. Katti, M. Rosenblum, E. G. Sirer. "Tiered Replication: A Cost-effective Alternative to Full Cluster Geo-replication." *USENIX ATC '15*, pp. 31–43.
17. S. A. Weil, S. A. Brandt, E. L. Miller, C. Maltzahn. "CRUSH: Controlled, Scalable, Decentralized Placement of Replicated Data." *SC 2006*. DOI 10.1109/SC.2006.19.
18. S. Pan et al. "Facebook's Tectonic Filesystem: Efficiency from Exascale." *FAST '21*, pp. 217–231.
19. M. Mitzenmacher. "The Power of Two Choices in Randomized Load Balancing." *IEEE TPDS* 12(10):1094–1104, 2001. DOI 10.1109/71.963420.
20. M. Mitzenmacher. "How Useful Is Old Information?" *IEEE TPDS* 11(1):6–20, 2000. DOI 10.1109/71.824633.
21. J. Dean, L. A. Barroso. "The Tail at Scale." *CACM* 56(2):74–80, 2013. DOI 10.1145/2408776.2408794.
22. K. V. Rashmi, M. Chowdhury, J. Kosaian, I. Stoica, K. Ramchandran. "EC-Cache: Load-Balanced, Low-Latency Cluster Caching with Online Erasure Coding." *OSDI '16*, pp. 401–417.
23. L. Huang, S. Pawar, H. Zhang, K. Ramchandran. "Codes Can Reduce Queueing Delay in Data Centers." *ISIT 2012*, pp. 2766–2770. DOI 10.1109/ISIT.2012.6284026. arXiv:1202.1359.
24. G. Joshi, Y. Liu, E. Soljanin. "On the Delay-Storage Trade-Off in Content Download from Coded Distributed Storage Systems." *IEEE JSAC* 32(5):989–997, 2014. DOI 10.1109/JSAC.2014.140518. arXiv:1305.3945.
25. S. Muralidhar, W. Lloyd, S. Roy, C. Hill, E. Lin, W. Liu, S. Pan, S. Shankar, V. Sivakumar, L. Tang, S. Kumar. "f4: Facebook's Warm BLOB Storage System." *OSDI '14*, pp. 383–398.
26. Amazon Web Services. *Amazon S3 User Guide*: "Checking object integrity in Amazon S3" and "Checking object integrity for data uploads in Amazon S3" (`docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity*.html`), fetched 2026-09-28.
27. Crate sources, all from `static.crates.io`, versions as named: reed-solomon-simd 3.1.0, reed-solomon-erasure 6.0.0, reed-solomon-novelpoly 2.0.0, reed-solomon-16 0.1.0, leopard-codec 0.2.0, rustfs-erasure-codec 9.0.0, erasure-isa-l-sys 1.1.0, crc-fast 1.10.0, crc32c 0.6.8, crc32fast 1.5.2, crc64fast-nvme 1.2.1, crc 3.4.0, crc-catalog 2.5.0, sha2 0.11.0, md-5 0.11.0, ring 0.17.14, aws-lc-rs 1.18.1, cpufeatures 0.3.1. Metadata from the crates.io API and the GitHub API, 2026-09-28.
