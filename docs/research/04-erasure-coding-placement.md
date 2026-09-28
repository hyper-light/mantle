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
| Which codes? | Replicate open blocks with R=3 and quorum appends. Re-encode to RS once a block is sealed. Ship a small menu of RS profiles: RS(6,3) for small clusters, RS(10,4) as the large-cluster default, RS(9,6) for high durability. Consider LRC(12,2,2) in v2. | WAS seals extents and then erasure-codes them lazily (Huang §1). Tectonic writes quorum appends and then re-encodes to RS(10,4) (Pan §5.2). Tectonic uses RS(9,6) for long-lived data (Pan §5.1). Ford Table 3 compares codes under correlated failures. |
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
  - R=2×2 with a 1-day inter-cell recovery reaches 1.08E+10 days, *"two orders of magnitude longer MTTF than R = 4"*, at about 6.8 MB/day of inter-cell bandwidth per user PB.
  - RS(6,3)×2 reaches 5.32E+13 days (1-day recovery) and 1.22E+15 days (1-hour recovery).

### Recommendations the authors made [V] (§10)

- *"correlation among node failures dwarfs all other contributions to unavailability"*.
- The concrete recommendations:
  - determine acceptable battery-transfer rates;
  - *"Focusing on reducing reboot times, because planned kernel upgrades are a major source of correlated failures"*;
  - *"Moving towards a dynamic delay before initiating recoveries, based on failure classification and recent history of failures in the cell."*

### Implications for mantle (A5) [Analysis]

1. **Model correlated failures.** Any durability claim mantle makes, or any profile it picks, has to be evaluated under a correlated-failure model: bursts at the rack, power and upgrade-domain level. Independent-failure MTTDLs, like those in A3, are optimistic by 2–8 orders of magnitude (Table 3).
2. **Stripe width.**
   - In Table 3, RS(9,4) at 44% overhead is *less* available than R=3 under correlated failures (2.39E6 vs 6.82E6 days). RS(8,4) at 50% is 7.5× better than R=3.
   - Adding data chunks at fixed m increases exposure to bursts. That is my inference from Table 3, not a stated conclusion.
   - Ford does not evaluate RS(10,4) (UNVERIFIED how it would fare). One would expect it to sit near RS(9,4). Mantle's large-cluster default of RS(10,4) (§R1) is therefore acceptable **only with** strict rack-aware and copyset-constrained placement (A6) and fast repair of low-margin stripes. Otherwise mantle should use RS(8,4) or RS(9,6).
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
| RS(10,4) | 1.53e-7 | 0.0054% | 0.054% | 0.54% | 78.2% | 100% |
| RS(9,6) | 3.92e-11 | 1.3e-6 % | 1.3e-5 % | 1.3e-4 % | 0.039% | 0.39% |

The columns P=1 through P=100 are copyset-constrained placement. For sensitivity, RS(10,4) with P=100 gives 0.014% at F=25 and 16.4% at F=100.

What the table shows:

- With random per-stripe placement, any mid-sized cluster holds 10⁷ to 10¹⁰ stripes; Tectonic's cluster held 15 B blocks. At that size, a simultaneous 1% node loss almost certainly destroys *some* stripe unless m is large, as RS(9,6) shows. Rack-aware placement alone does not help against this kind of failure: a power event that kills a random 1% of hosts spreads across racks.
- Constraining stripes to a bounded family of stripe sets cuts that probability by orders of magnitude. The same trade-off Cidon describes applies: larger P (scatter width) means faster, more parallel rebuilds but more distinct stripe sets.
- For EC the constant is kinder than for replication, because loss needs m+1 coincident failures rather than R.

---
