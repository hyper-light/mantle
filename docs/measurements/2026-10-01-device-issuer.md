# Batches through the device's issuer

**Question.** The chunk writer started a thread for each region of a batch after the first
(`write_together`), and `bench::tests::clients_cost_no_threads` caught the thread count moving
under load: `[22, 22, 23]` where alone it is always 22. Every write and flush now goes through
one issuer per device, a thread and a fixed pool of workers started when the device opens
(STATUS item 4; chunk-store.md §4). Does that keep the thread count fixed, and does it make a
batch's median or p99 latency worse?

**Machine.** macOS 26.4.1 on an Apple M5 Max, APFS on the internal Apple SSD AP8192Z, direct I/O
and `F_FULLFSYNC`, release builds. Another session ran a test suite on the machine throughout:
load averages were 5 to 16, and the device's own durable 4 KiB write measured anywhere from
4.33 to 8.65 ms between calibrations of the same build minutes apart.

## Threads

The process's threads as the OS counts them (`mantle_disk::threads::count`):

| Run | Before | After |
|---|---|---|
| `bench::tests::clients_cost_no_threads`, 18 / 180 closed / 180 open clients, depth 4 | 22, 22, 23 under load (the failure) | 27, 27, 27 |
| `tests/issuer.rs`, batches of up to 21–22 writes (one region per put), depth 4 | — | 9 idle, 9 while writing, at most 4 writes in flight |
| `bench chunk`, one client, three runs | 4, 10, 8 | 21 (depth 16) or 69 (depth 64) |
| `bench chunk`, 16 clients, three runs | 19, 19, 24 | 36 or 84 |
| `bench chunk`, 8 clients at 8 MiB | 12, 12, 13 | 28 or 76 |
| `bench chunk`, 64 clients at 4 KiB | 22, 21, 21 | 38 or 86 |
| `bench chunk`, 16 clients, 4 MiB segments (several regions a batch) | 19, 23, 19 | 36 or 84 |

Before, the count at a point varied from run to run by the regions in flight. After, it is
the drivers, the volume's writer and cleaner, the issuer and its workers: fixed for a depth.
The depth is calibration's random-read saturation (16 or 64 on this device, from run to run)
under the OS's queue of 253 (`issuer::depth`).

## Latency in the store, alternated

**Method.** `mantle bench chunk DIR --sizes 4K,1M,8M --workers 1,16 --rounds 10 --seconds 1`,
then `--sizes 4K --workers 64`, then `--segment-size 4M --sizes 1M --workers 16`, each run
calibrating first. The build before (5c6eaa7) and the build after ran alternately, three times
each. Each cell is the median over the three runs of the
point's p50 and p99 in ms. The 8 MiB point ran at 8 clients, the most the queue admits.

| Point | Before p50 | After p50 | Before p99 | After p99 |
|---|---|---|---|---|
| put 4 KiB, 1 client | 4.33 | 4.46 | 13.9 | 21.0 |
| put 4 KiB, 16 | 9.18 | 4.46 | 19.9 | 14.7 |
| put 4 KiB, 64 | 4.33 | 4.46 | 13.1 | 20.4 |
| put 1 MiB, 1 | 4.33 | 8.65 | 13.1 | 19.9 |
| put 1 MiB, 16 | 7.60 | 9.70 | 29.9 | 28.3 |
| put 8 MiB, 1 | 6.82 | 6.82 | 16.5 | 18.4 |
| put 8 MiB, 8 | 21.0 | 23.1 | 57.7 | 40.9 |
| put 1 MiB, 16, 4 MiB segments | 16.3 | 14.2 | 36.7 | 40.9 |

The same point moved by 2–3x between runs of one build (put 1 MiB, one client: 4.33, 4.33 and
12.8 ms before; 4.33, 12.8 and 8.65 after), so these medians do not resolve a difference of
less than that. Points move both ways. The one that looked consistent, put 1 MiB at one client,
was run alone six more times alternately: before 4.33, 4.33, 4.33 ms; after 4.33, 5.51, 5.24 ms.

## Latency, interleaved batch by batch

Alternating builds cannot separate the change from the machine's drift, so the two paths were
compared within one process, one batch of each in turn.

**The raw batch.** `cargo run --release -p mantle-disk --example issuer_batch -- DIR KIB 300
DEPTH`: one region and a 4 KiB frame, written at once and flushed, either with a thread started
for the frame (as `write_together` did) or through an issuer.

| Region | Depth | Threads p50 / p99 | Issuer p50 / p99 |
|---|---|---|---|
| 4 KiB | 64 | 8.58 / 16.85 | 8.59 / 17.32 |
| 4 KiB | 64 | 18.45 / 34.95 | 18.39 / 29.28 |
| 1 MiB | 16 | 4.28 / 10.57 | 4.35 / 10.76 |
| 1 MiB | 64 | 4.36 / 11.72 | 4.30 / 12.17 |
| 1 MiB | 64 | 19.33 / 35.62 | 19.48 / 34.85 |
| 8 MiB | 64 | 15.74 / 41.67 | 15.37 / 42.02 |
| 32 MiB | 64 | 15.29 / 24.44 | 15.12 / 24.54 |

**The store.** The writer built with a switch between the two paths (a temporary patch, not
committed), one volume of each in turn in one process, 400 puts by one client (150 at 8 MiB),
three rounds, depth 64:

| Chunk | Threads p50 / p99 | Issuer p50 / p99 |
|---|---|---|
| 1 MiB | 4.25, 4.24, 4.24 / 7.71, 9.25, 9.37 | 4.24, 4.25, 4.24 / 6.97, 9.14, 8.73 |
| 4 KiB | 4.26, 4.26, 4.30 / 7.06, 8.99, 10.63 | 4.27, 4.29, 4.33 / 8.81, 10.55, 12.72 |
| 8 MiB | 11.58, 6.88, 8.35 / 35.71, 21.15, 25.66 | 10.76, 6.90, 10.18 / 27.45, 21.42, 31.37 |

**Reading.** Batch by batch the issuer costs what the threads did, within the noise at every
size: the 1 MiB batch that alternated runs suggested was twice as slow is 4.24 ms either way.
The issuer's hand-offs (submitter to issuer, issuer to worker and back, for the writes and again
for the flush) cost at most a few tens of microseconds at the 4 KiB median, 4.26 to 4.27–4.33
ms, under 2% of a flush. The 4 KiB p99 was higher through the issuer in two of three rounds and
the 8 MiB p99 in one; at these sample sizes p99 is the 4th-largest of 400, and the rounds that
went the other way are as large. No difference here is larger than the run-to-run spread of
either path.

## A defect the first build had

The first build gave the index frames' buffers back to the writer's buffer pool but allocated
each frame's buffer fresh. A pool keeps at most twice a batch of free bytes, so after some
16,000 batches it held 64 MiB of 4 KiB frame buffers, and every region buffer given back was
freed; each batch then allocated and zero-filled its region buffer again, the cost the pool
exists to avoid (buf.rs). Frames now take their buffers from the pool as regions do, so the
pool's free buffers are what the next batches take. The tables above are the corrected build;
the first build's three alternated runs were within the same spread.

**Consequence.** The issuer replaces `write_together` with no thread started on the write path
and no batch latency difference this machine can resolve. Measuring the remaining few tens of
microseconds, and whether the flush should go to the worker that finished the batch's last write
rather than back through the issuer's thread (io_uring's linked flush does the equivalent),
needs a quiet machine.
