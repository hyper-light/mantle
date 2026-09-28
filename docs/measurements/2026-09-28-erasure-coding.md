# Erasure coding throughput per code

**Question.** How fast does one core encode a block into chunks and rebuild it from what is
left, for each code docs/research/04 §0 weighs for mantle?

**Method.** `mantle bench ec` (crates/mantle/src/bench_ec.rs), release build, Apple M5 Max,
macOS 26; `reed-solomon-simd` 3.1.0 selects NEON at run time. Each point runs repeatedly
for half a second. Throughput counts the block's bytes (data chunks × chunk size). "Rebuild"
decodes the block after losing one data chunk, and after losing as many data chunks as the
code tolerates.

| Code | Chunk | Encode | Rebuild, 1 lost | Rebuild, most lost |
|---|---|---|---|---|
| RS(6,3) | 64 KiB | 6.58 GB/s | 893 MB/s | 901 MB/s |
| | 1 MiB | 8.36 GB/s | 1.87 GB/s | 1.90 GB/s |
| | 8 MiB | 8.11 GB/s | 1.91 GB/s | 2.00 GB/s |
| RS(8,4) | 64 KiB | 8.75 GB/s | 1.16 GB/s | 1.17 GB/s |
| | 1 MiB | 10.4 GB/s | 2.30 GB/s | 2.40 GB/s |
| | 8 MiB | 10.1 GB/s | 2.45 GB/s | 2.50 GB/s |
| RS(10,4) | 64 KiB | 9.09 GB/s | 1.35 GB/s | 1.38 GB/s |
| | 1 MiB | 9.29 GB/s | 2.54 GB/s | 2.62 GB/s |
| | 8 MiB | 8.96 GB/s | 2.65 GB/s | 2.75 GB/s |
| RS(9,6) | 64 KiB | 5.49 GB/s | 852 MB/s | 871 MB/s |
| | 1 MiB | 5.56 GB/s | 1.27 GB/s | 1.27 GB/s |
| | 8 MiB | 5.39 GB/s | 1.27 GB/s | 1.33 GB/s |

**Reading.** Encoding runs at 5–10 GB/s per core, above the durable write rate the chunk
store reaches on this machine's SSD (docs/measurements/2026-09-28-chunk-store-benchmark.md),
so encoding does not limit writes here. Rebuilding runs at a quarter of that. Losing one
chunk costs as much as losing the most the code tolerates, because the FFT decoder works
over the whole code's domain rather than per lost chunk (docs/research/04 A2). At 64 KiB
chunks both halve, since each call builds the decoder's working space afresh.

**Correctness.** The crate's tests rebuild every combination of lost chunks within the
tolerance of RS(2,1), RS(3,2), RS(4,2), RS(6,3), RS(8,4), RS(10,4) and RS(9,6) and compare
the result byte for byte, and a property test covers random codes up to RS(16,6), random
block lengths and random losses (crates/ec/src/lib.rs).
