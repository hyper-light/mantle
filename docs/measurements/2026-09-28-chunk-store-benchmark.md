# The chunk store against the device it runs on

**Question.** How close do the chunk store's puts and reads come to what the device does
through the same file layer, and where does the difference go?

**Method.** `mantle bench chunk <dir>` (crates/mantle/src/bench.rs), release build, macOS 26
on an Apple M5 Max, APFS on the internal Apple SSD AP8192Z. The command calibrates the
device, formats a 4 GiB volume in a scratch file, writes the whole volume once, then for
each chunk size and number of closed-loop workers puts chunks for a second (or until a
third of the volume is written), reads the same chunks back in random order for a second,
reads the same number of bytes at random places in the volume's segments directly through
the file layer, and deletes the chunks. Profiles were taken with `sample(1)` during steady
read and put loads.

**Device, through the file layer.** A 4 KiB write followed by `F_FULLFSYNC`: 4.7 ms. 32 MiB
written and then flushed: 5.3 GB/s. 4 KiB random reads: 15K/s with one in flight, 230–240K/s
with 64.

## Findings

**1. Reads keep up with the file layer.** With the environment quiet (finding 2), reads
through the store reach 92–98% of the file layer's rate at every concurrency above one:
for example 64 KiB chunks with 16 in flight, 123K/s against 127K/s; 1 MiB chunks with 16
in flight, 14.4 GB/s against 14.7 GB/s. A single stream of large chunks pays for the
checksum verification and one copy that the file layer does not do: 83% of the file
layer's rate at 1 MiB, 65% at 8 MiB (1.18 ms against 0.72 ms per 8 MiB chunk).

**2. This machine stalls reads in bursts, whatever issues them.** Every thread's reads
stop at the same moment for about 6 ms, repeatedly, for tens to hundreds of milliseconds at
a time, at random moments. A timeline of reads slower than 2 ms showed the bursts in raw
file-layer reads as well as in the store's reads, with all threads stalled together, so the
cause is below mantle (the file system or the drive). They land in the p99 of whichever
rows they coincide with, which is why the benchmark prints the file layer's row next to each
of the store's: the pair shows what mantle adds apart from the environment.

**3. A zeroed buffer per read cost 12.6% of a reader's time.** Before the fix, every read
allocated a zeroed aligned buffer and copied the span out of it. macOS's allocator
(`xzm`) zeroes a large allocation by calling `madvise` and faulting fresh pages back in:
628 of 4,982 samples of a reader thread were under `_xzm_segment_group_clear_chunk` →
`madvise`, against 4,186 in `pread`. glibc serves blocks over its mmap threshold with
`mmap`, so Linux pays a similar cost. Reads, verification and the writer now take buffers
from a bounded pool (`mantle_disk::buf::Pool`) and read spans in place, and
`Volume::read_into` fills a caller's buffer so a streaming reader allocates nothing.

**4. Combining checksums cost more than computing them.** A profile of the writer under
8 MiB puts: 27% of its time was in `crc_fast`'s CRC combination, used to derive a record's
whole-payload CRC from its 64 KiB block CRCs, against 3% computing the block CRCs.
`crc_fast` rebuilds the shift operator on every call. `mantle_crc::Crc32cShift` builds it
once (zlib's method), which raised 8 MiB puts with 16 writers from 1.63 to 2.22 GB/s.

**5. Few writers alternated between batches.** A batch is formed from whatever is queued
when the previous one finishes, so the writers it just answered miss the next batch: with
4 writers of 4 KiB chunks, batches alternated between 1 and 3 requests and each put took
two flushes (472 puts/s, p50 9.4 ms). The writer now waits for the submitters it just
answered for as long as that is expected to lower total latency, a bound it computes from
the measured batch service time and the learned rate at which answered submitters return
(crates/chunk/src/writer.rs, module documentation). With it, 4 KiB puts scale with the
number of writers at one flush each: 238/s with 1, 885/s with 4, 3.71K/s with 16 (p50
4.72 ms); 64 KiB puts 231/s, 893/s, 3.51K/s.

**6. Large puts reach under half of the device's durable bandwidth.** 8 MiB puts peak at
2.2–2.5 GB/s against the 5.3 GB/s the device sustains for 32 MiB flushed at a time. The
writer lays out, writes and flushes each batch in turn, so the device sits idle while the
writer computes and the writer sits idle during the flush: after finding 4, the flush took
55% of its time, data and log writes 18%, copying payloads 20%, and checksums 5%.

**7. Results depend on the drive's recent history.** The first put points after the fill
pass ran at half their usual rate (4 KiB puts with one writer: 102/s) and at the usual rate
when run later in the same process. One pass per point measures the drive's state as much
as the store; how many repetitions a point needs is open (docs/research/11).

**8. The index frame's separate write costs a quarter of durable bandwidth here.** Raw
32 MiB batches through the file layer, each written and then flushed: 5.31 GB/s cycling
within 256 MiB and 5.36 GB/s advancing across 4 GiB, so where the data goes does not
matter; adding one 4 KiB write to a separate region before each flush, as the chunk store
does with its index frame, drops both to 4.0–4.1 GB/s (p50 6.82 → 8.39 ms per batch). After
checksums moved to the senders and records were encoded in place, 8 MiB puts reach 3.0–3.25
GB/s: 75–80% of what their own I/O pattern allows, with the writer 92% in I/O (flush 61%,
data write 23%, frame write 7.6%).

**9. Writes proceed partly while another flush runs.** Two threads each writing and
flushing 32 MiB batches on separate regions reach 5.99 GB/s against 5.12 GB/s for one;
four reach 5.55 GB/s.

## Consequences

- Buffers for I/O come from pools; nothing on a read or write path allocates per I/O.
- The whole-payload CRC is combined with a fixed shift.
- The group-commit wait is derived from measured quantities (finding 5).
- Checksums are computed by the sender and records are encoded in place (finding 8's
  numbers include both).
- Open: whether the index frame must be a separate write in the flush path (finding 8 and
  docs/design/chunk-store.md §3.2), and a writer that overlaps a batch's writes with the
  previous flush to the depth calibration measures (finding 9).
