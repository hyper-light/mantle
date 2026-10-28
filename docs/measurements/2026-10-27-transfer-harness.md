# The transfer harness's first runs: every check, and the harness's own bounds

**Question.** Does `mantle bench transfer` (plan step 9; research/76) hold every check it makes on
real cells through both doors (every byte read back the generator's; every block's chunks on
distinct nodes; every node's chunk bytes accounted; every object read back through the others with
each node SIGKILLed in turn and through it started again; its controls caught), and do its own
memory and threads stay bounded as its objects go from 1k to 100k?

**Method.** A release build of `mantle bench transfer` on adacachy, the bare-metal box (AMD Ryzen 9
9950X3D2, 16 cores and 32 threads, 122 GiB, `/home` on Samsung SSD 9100 PRO NVMe, Linux
7.2.9-2-cachyos), run alone but for other agents' CI legs. Each case is a fresh cell of three that
`mantle init` makes on loopback, its founders in three failure domains, so each block's three copies
sit on three nodes; the small sets at seed 1, half put natively and half by boto3 through the third
node's S3 listener, each read back through both doors and another node. Each case's volumes are
stated to hold its set's copy: 256 MiB for 1k, 2 GiB for 10k, 16 GiB for 100k. The harness
process's peak resident set (`VmHWM`) and threads are sampled from `/proc` every five seconds.

**Measured.** 2026-10-27 from 15:06 local, the binary built from 4c27fd5a (md5
`44c995764c2a46299645789e31aa0c23`), load average 2.1 at the start. Latency is per object, one
object at a time; rows as the harness says them (`~/transfer-runs/sized-20261027-150608` on
adacachy keeps every line and sample).

| case | objects | held a node | put p50 / p99, native | put p50 / p99, http | read p50 native / http | placement | kills | controls | peak resident | threads | seconds |
|---|---|---|---|---|---|---|---|---|---|---|---|
| small 1,000 | 1,000 | 65.5 MB | 53.5 / 79.7 ms | 71.3 / 127.9 ms | 0.52 / 0.85 ms | 999 blocks, all distinct | 6,000 reads, 0 wrong, 0 missing | 3 of 3 caught | 15.2 MiB | 4 | 117 |
| small 10,000 | 10,000 | 654 MB | 60.8 / 102.8 ms | 59.8 / 98.6 ms | 0.56 / 0.74 ms | 9,999 blocks, all distinct | 60,000 reads, 0 wrong, 0 missing | 3 of 3 caught | 15.1 MiB | 4 | 730 |
| small 100,000 | 100,000 | 6.55 GB | 43.0 / 83.9 ms | 59.8 / 100.7 ms | 0.49 / 0.74 ms | 99,998 blocks, all distinct | 600,000 reads, 0 wrong, 0 missing | 3 of 3 caught | 13.2 MiB | 4 | 5,722 |

Every node's chunk bytes were exact in every case: what it received over QUIC and put itself is
what its volume holds, nothing kept by none. Nothing was asked again. A set's empty objects hold no
block: `s0`, a boundary, in each, and at 100k `s33776`, whose drawn size is 0 (seed 1's keystream
of sizes, made again with `openssl enc -aes-256-ctr` from the key and counter `generate.rs`
states). Peak resident is the most `VmHWM` said; threads are the most `Threads` said, the main
thread and a reader a node; seconds run from the case's start to its last line.

**What the first runs found, each fixed at its cause.**

- The harness grew 1.5 KB an object: its peak resident set was 16,640 KiB at 1k and 29,740 KiB
  at 10k (the build before 4c27fd5a, md5 `da0344461debebf11aeffe42178704e4`; on adacachy,
  `~/transfer-runs/bounds-20261027-142725` and `sized-20261027-144422`). Each node's reader thread
  told the case every `status` line, several an object, and the case took them only as it waited;
  each reader now tells a range's leader as it changes (4c27fd5a), and 10k and 100k peak no higher
  than 1k.
- Reads through the others after a SIGKILL ended `Stalled`: a node read a chunk from its killed
  peer until the transport judged the peer gone, two idle timeouts, as long as its client waited.
  A node now reads nothing from a peer its detectors suspect (b905b400; gateway.md §3).
- The 10k and 100k sets on 256 MiB volumes filled the cell at 145 MB of chunks a node and refused
  the rest: a volume keeps its cleaner's reserve and runway past what client writes take, which is
  half of so small a volume. Their volumes are stated now.

**Not yet explained.** A small put takes 43-71 ms at the median on this NVMe, one object at a
time. Each waits on its chunks' three durable copies and then on its ranges' commits in turn, with
no other put's flush to share; plan step 10 measures where the time goes.
