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

