# Engine ZSTD against the reference library, 2026-10-06

The port's ZSTD codec (`crates/engine/src/codec/zstd`) against zstd 1.5.7, the reference C
library, on table blocks as RocksDB compresses them: one frame a block, at RocksDB's default
level 3.

## Setup

- Apple M5 Max, macOS 26.4.1, rustc 1.94.1, release profile.
- The reference: zstd 1.5.7 through the `zstd` crate (zstd-sys 2.1.0), the tests' oracle, one
  compression context, one decompression context and one output buffer reused across blocks.
- The port: `cargo bench -p mantle-engine --bench codec` at 1e14dec, called as the table code
  calls it (`util::block_compression`), one `Workspace` reused across blocks. Each compressed
  block is returned in a buffer of its own, as a table builder takes it.
- Input: the text inputs of the test corpus (`tests/support/corpus.rs`), cut into 520 blocks of
  4 KiB (RocksDB's default `block_size`) and 128 of 16 KiB.
- Both decoders read the reference's frames, so the decode times compare like for like.
- The machine was in its usual service load, load average 5 to 6 over the run.

## Method

59 rounds (Wilks: the extremes of 59 bound the 95th percentile at 95%), the two sides
alternating which goes first. A round compresses then decompresses every block. *ns* is the
median time per block, with its distribution-free 95% interval (order statistics 22 and 38 of
59). *ratio* is the port's median over the reference's. *allocs* counts allocations over a
round. *bytes* is the total compressed size of the blocks.

## Results

| Operation | Block | Reference ns [95%] | Port ns [95%] | Ratio | Allocs ref/port | Bytes ref/port |
|---|---|---|---|---|---|---|
| compress | 4 KiB | 9,871 [9,748–9,936] | 15,153 [15,133–15,204] | 1.54 | 0/520 | 126,065/125,926 |
| compress | 16 KiB | 24,790 [24,538–24,970] | 40,589 [40,256–40,784] | 1.64 | 0/128 | 107,155/107,178 |
| decompress | 4 KiB | 3,741 [3,714–3,773] | 6,102 [6,040–6,169] | 1.63 | 0/0 | — |
| decompress | 16 KiB | 10,281 [10,190–10,396] | 16,262 [16,082–16,470] | 1.58 | 0/0 | — |

The port's one allocation per compressed block is the buffer it returns; the reference side of
the benchmark writes into one buffer it reuses, which the table builder's interface does not
offer yet.

The port's frames are smaller than the reference's at 4 KiB and within 0.02% at 16 KiB. It codes
a repeat offset wherever an offset equals one, where the reference's fast searches code only the
first.

## Where it started

At b6972e4, before this day's work:
- compression took 7.05 times the reference's time at 4 KiB, with 265 allocations a block;
- decompression took 3.87 times, with 3.75 allocations a block;
- the frames were 2% larger at 4 KiB and 7.5% larger at 16 KiB.

Each step is recorded in its commit message and in `docs/design/engine.md` §9, with its
steering measurement.

## Measured and not kept

Each was measured in alternating A/B runs of the two builds, six runs of 21 rounds each:
- four Huffman streams decoded interleaved, as `HUF_decompress4X1` does: 2–3% slower on these
  blocks, which carry few literals;
- the decode loop's sequence closure forced inline: 5% slower;
- the double-fast search compiled for each minimum match length, as the reference compiles it:
  5% slower.

## Open

Both directions are still slower than the reference: 1.5–1.6 times for compression, 1.6 for
decompression. The remaining time is spread across:
- decompression: the sequence loop (38%), copies (23%), and table builds (20%);
- compression: match finding (48%), entropy coding (22%), and Huffman tables (11%).

These are the reference's own designs; what remains is per-instruction cost, to be closed with
cycle-level profiles.
