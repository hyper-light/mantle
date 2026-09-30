# What erasure coding copies

**Question.** `mantle-ec` copied more than its callers asked for (audit P08). Encoding copied
every data chunk out of the block before computing parity, and the gateway's PUT kept those
copies, so a block being coded was held twice. Decoding copied every data chunk present out
of its caller's buffers and then into the block. Rebuilding one chunk copied every data chunk,
and cloned the wanted ones again. Now the parity is computed from the data chunks where they
lie in the block, the PUT stores slices of the block as its data chunks, decoding copies each
byte of the block once, and rebuilding copies only the chunks wanted. What does that change?

**Method.** `mantle bench ec --sizes 64K,1M,8M`, release builds of the same tree with and
without the change, run in turn, twice each, on an Apple M5 Max under macOS 26.4.1, otherwise
idle; `reed-solomon-simd` 3.1.0 selects NEON at run time. Throughput counts the block's bytes.
"For a PUT" is what a PUT computes: before, `encode`, every chunk a copy; after, `parity_of`,
the parity alone, the data chunks being slices of the block. "Read" decodes the block with
every data chunk present; "1 lost" and "most lost" with one data chunk and with as many as
the code tolerates missing; "data chunk" rebuilds one lost data chunk, and "parity" one lost
parity chunk.

## Findings

**1. Reading a block whose data chunks are all present is two to six times as fast.**

| Code | Chunk | Read before | Read after |
|---|---|---|---|
| RS(6,3) | 64 KiB | 13.5 · 14.7 GB/s | 85.7 · 83.3 GB/s |
| RS(6,3) | 8 MiB | 31.1 · 33.3 GB/s | 69.9 · 66.6 GB/s |
| RS(9,6) | 64 KiB | 16.5 · 20.9 GB/s | 86.2 · 82.5 GB/s |
| RS(9,6) | 8 MiB | 33.7 · 32.7 GB/s | 68.6 · 63.0 GB/s |

It is now one copy of each byte, from the chunk into the block.

**2. Coding a block for a PUT holds it once, and runs 10–30% faster from 1 MiB chunks.**
RS(6,3) at 8 MiB went from 6.81 · 7.51 GB/s to 9.11 · 8.62 GB/s, RS(8,4) at 1 MiB from
10.4 · 9.55 to 11.8 · 11.5, and RS(10,4) at 1 MiB from 9.23 · 8.78 to 10.1 · 10.0; at
64 KiB the two are within a run's spread. The PUT no longer holds a copy of every data chunk
beside the block, 48 MiB for a 48 MiB block under RS(6,3) with 8 MiB chunks; only a last data
chunk the block does not fill is copied, to pad it.

**3. Rebuilding a parity chunk runs 15–25% faster; decoding lost chunks is the decoder's.**
One lost parity chunk: RS(8,4) at 1 MiB from 10.2 · 9.32 GB/s to 11.4 · 12.2 GB/s, RS(9,6) at
8 MiB from 5.30 · 4.66 to 5.41 · 5.53. With data chunks lost, reading and rebuilding a data
chunk stayed within a run's spread of each other, 0.8–2.7 GB/s: the decoder's work is what
costs there, and a copy of the chunks is a small part of it.

**4. The coder's own work space is the larger memory.** `reed-solomon-simd` sizes an
encoder's work space at ⌈data/p⌉·p chunks, with p the parity count's next power of two, and a
decoder's at the next power of two of p + data chunks (`src/rate/rate_high.rs`,
`work_count`, 3.1.0): with 8 MiB chunks, 64 MiB to encode and 128 MiB to decode under RS(6,3),
and 128 MiB to encode and 256 MiB to decode under RS(9,6). A coder is made anew for every
block, so this is allocated per block. Keeping one and resetting it measured 6–10% faster to
encode 1–8 MiB chunks and 26% at 64 KiB (5.71 → 5.18 ms for RS(6,3) at 8 MiB), and 0–8% to
rebuild, in a release run of 40 codings each on this machine; the gain is its allocation.
Which coders to keep, and how many, is the gateway's memory admission to decide
(docs/design/gateway.md §4), since a kept work space holds its memory between blocks.
