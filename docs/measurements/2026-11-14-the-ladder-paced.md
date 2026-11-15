# The doors' closed ladder of 2026-11-13: a paced cell, and a stop on one step

Status: measurement record, 2026-11-14. For goal 16's write path (research/87; the owner: "Why
can a cell only handle 100 RPS? That's miserable. I expect two orders of magnitude greater").
Machine: adacachy2 (Linux x86_64, AMD Ryzen 9 9950X3D2, 32 threads, 122 GiB, the cell on
`/home`'s btrfs, 90% full, shared with other sessions' builds). Every time below is the machine's
local time (PST).

## What was seen

A closed ladder of 1-byte PUTs through both doors, n 1,000 a step, four runs A B B A
(`~/mantle-rps/ladder.sh`): A lean-doors `40a4f84e`, B no-fixed-counts `6d95f931`.

| run | build | window | native best | http best |
|---|---|---|---|---|
| 1 | A | 19:07:27–19:13:35 | 127/s at 24 | 91/s at 16; 503 on new connections past ~30 |
| 2 | B | 19:13:35–19:24:03 | 156/s at 16, then 5.5/s at 32 (183 s, max 163 s, 32 asked again) | 1,477/s at 128, then 368/s at 256; 1 client 5.5/s (181 s, max 30.3 s) |
| 3 | B | 19:24:03–19:28:20 | 66/s at 8, ladder stopped | 106/s at 8, ladder stopped |
| 4 | A | 19:28:20–19:34:45 | 136/s at 16 | 77/s at 16 |

## The collapse and the crawl: the cell was frozen

kiln's headroom paces the work it runs when a machine's health targets suffer: "each group runs
that share of every period (`cgroup.freeze`): only for the resource that's short" (kiln
`docs/headroom.md`, "Pacing"). A benchmark kiln runs is such a group, the three `mantle serve`
processes and the harness together. Its journal on adacachy2 for run 2's window (`journalctl
--user -u kiln-agent*`, the `headroom` events, quoted):

| time | event |
|---|---|
| 19:13:54 | runner disk: a probe has been out 1003 ms, its tolerance 1000 ms; kiln's writes cut to 0 MB/s |
| 19:14:22 | kiln's writes raised to 1048 MB/s |
| 19:16:21 | a probe has been out 146.5 ms, typical 6.1 ms; writes cut to 44 MB/s; CPU cut to 5.9 cores |
| 19:16:38 | a probe has been out 1004 ms; kiln's writes cut to 0 MB/s |
| 19:17:07 – 19:18:28 | writes raised to 37, 333, 555 MB/s; CPU to 17.5, 23.2, 26.1, 29.0 cores |
| 19:19:34 | a probe has been out 1004 ms; kiln's writes cut to 0 MB/s |
| 19:20:03 – 19:23:12 | writes raised to 35, 52, 70, 139, 279, 558, 560, 570 MB/s, 27 s apart |

No event falls in runs 1, 3 or 4. Run 2's steps, placed back from its log's end at 19:24:03 by
their elapsed times (the HTTP steps after the first 101.5 s, the native steps before the 32 89.2 s,
the cell's start 64 s including the freeze at 19:13:54):

| step | ran | the pacer meanwhile | measured |
|---|---|---|---|
| native 1–16 | 19:14:39–19:16:08 | unpaced | 32, 41, 58, 98, 156/s |
| native 32 | 19:16:08–19:19:12 | cut to 44 MB/s, frozen 19:16:38–19:17:07, 37 MB/s to 19:18:01 | 5.5/s, max 163 s, 32 asked again |
| http 1 | 19:19:12–19:22:13 | frozen 19:19:34–19:20:03 (29 s), then 35–139 MB/s to 19:21:24 | 5.5/s, p50 107 ms, max 30.3 s |
| http 2–256 | 19:22:13–19:23:55 | 279–570 MB/s | 21, 42, 83, 114, 193, 835, 1,477, 368/s |

The HTTP step's longest operation, 30.3 s, is the 29 s freeze from 19:19:34; its median of 107 ms
against run 3's 38 ms is the duty cycle after it. The native 32's operations waited through the
freeze and the paced minute after it; its clients' connections, both ends frozen, met their idle
timeouts on thawing, and its 32 again are those requests asked again. The trouble that started
each cut was the runner disk's probe, a small write made durable in kiln's data folder on the same
btrfs as the cell: at 19:13:54 the cell's three volumes being preallocated ("trouble began at
1674 MB/s"), at 19:16:21 the cell's own flushes at 32 clients. A durable benchmark on a file
system it shares with the pacer's probe slows the probe and is frozen for it.

Pacing on adacachy2 has since been harder. From 17:34:57 on 2026-11-14 kiln held its writes at
0 MB/s ("kiln's writes cut to 0 MB/s: trouble began at 51 MB/s"), and runs of this work froze
whole within seconds of starting, a ladder on btrfs before its first phase
(`/tmp/nfc2/freezes.log`, each kiln run cgroup's `cgroup.freeze` sampled every 20 ms) and one on
tmpfs while still reading its binary, writing nothing.

## The ladder's stop

Run 3 ran unpaced and stopped its native ladder at 8 clients, 65.7 against 61.8 a second, 6%
short of the 10% the harness's `GROWTH` asked, and its HTTP ladder at 16, 96.9 against 105.6: one
step each, n 1,000, a single throughput against a single throughput. The same build's best was
156/s at 16 the run before. A step's rate moved 13–30% from run to run on the doors (research/71,
step 9), so a single step decides where the ladder stops by chance.

## The learned limit

Traced (diag-nfc2, each node's admission and its phases, on tmpfs so nothing paced it): every
node's limit stayed at its ceiling, 144,767 and 144,796 operations, at 1, 2 and 64 clients, its
descent `Waiting`, no phase in which its limit turned an operation away. At 256 MiB volumes the
data's wants leave the operations most of a 122 GiB machine (research/87 §4, rule D), so the
limit does not bind in any of these ladders, and nothing in runs 1–4 is its instability.

## A ladder to 1,024 clients, unpaced

adacachy2's runner went offline at 20:50 on 2026-11-14 and the box's heavy-run lock had been held
for two days, so the rest was measured on a Mac Studio (`adas-mac-studio.local-2`: Apple M3 Ultra,
32 cores, 96 GiB, macOS 26.4, the cell on APFS, every flush `F_FULLFSYNC`), where nothing paced
the runs. One closed ladder of 1-byte PUTs, diag-nfc2 `7fd3ba28` (the harness above with the trace,
a diagnostic build), each step judged in rounds:

| clients | native ops/s | interval | asked again | failed | http ops/s | interval | asked again | failed |
|---:|---:|---|---:|---:|---:|---|---:|---:|
| 1 | 13.5 | (ordered) | 0 | 0 | 12.6 | 12.4–12.9 | 0 | 0 |
| 2 | 15.6 | 15.0–16.3 | 0 | 0 | 19.0 | 18.8–19.5 | 0 | 0 |
| 4 | 24.5 | 23.3–24.8 | 0 | 0 | 30.1 | 29.6–31.1 | 0 | 0 |
| 8 | 35.7 | 35.2–36.6 | 0 | 0 | 45.2 | 43.9–46.5 | 0 | 0 |
| 16 | 56.0 | 55.2–58.6 | 0 | 0 | 72.2 | 70.4–73.1 | 0 | 0 |
| 32 | 105.1 | 102.7–109.3 | 0 | 0 | 137.0 | 134.5–142.3 | 0 | 0 |
| 64 | 212.2 | 206.1–217.3 | 0 | 0 | 270.6 | 264.2–274.6 | 26 | 0 |
| 128 | 397.5 | 386.2–415.0 | 45 | 0 | 534.1 | 522.8–560.8 | 88 | 0 |
| 256 | 662.3 | 574.4–737.1 | 3,015 | 5 | 841.1 | 817.8–863.9 | 1,630 | 5 |
| 512 | 1,138.9 | (ordered) | 13,489 | 133 | 1,435.1 | (ordered) | 14,592 | 188 |
| 1,024 | 2,016.6 | 1,797.6–2,281.9 | 63,067 | 1,828 | 1,050.6 | (ordered) | 22,335 | 1,757 |

Throughput doubled with the clients while a PUT's latency held at 300–400 ms, the flushes' group
commit taking each step's added operations. From 128 clients PUTs were answered `SlowDown` and
asked again, and from 256 some failed every attempt; the HTTP door's rate fell at 1,024.

## Under load, chunks a peer stored were refused `Closed`

Traced (diag-nfc2 `2f5ababa`–`ce8efb9e`, an open loop offered 4,000 PUTs a second on 1,024 clients):
every `SlowDown` was a put whose block found no volume for a chunk (`PutError::Unplaced`), and
every such chunk's exchange was refused `Closed` by the peer that stored it (10,534 of 10,534,
`by_peer true`), with no node-to-node connection lost. The storing node answers a chunk with a
head alone and ends the exchange at once (`Node::answer_chunk`); hyper-transport's end waited for
a trailer but not for a head, and reset any exchange whose reply head its connection's credit had
not yet taken, the transfer class's credit behind the classes above it. Counted at each end
(`hyper_transport` instrumented in the diagnostic build): 2,045, 666 and 7,344 ends on the three
nodes found the head unwritten, 10,055 in all, against 10,055 chunk puts refused `Closed`. Each was
a chunk durable on its volume that its put took for refused: the put answered `SlowDown`, its
client waited one to twenty seconds, and the chunk was left named by no block.

hyper-raft `31aa800` (branch `transport-end-whole-reply`) makes the end wait for the head of a
reply its owner gave whole, as it waits for a trailer; its test,
`a_head_alone_ended_before_credit_took_it_still_reaches_the_peer`, fails before it with exactly
the cell's refusal (`Refused { refusal: Closed, by_peer: true }`) and passes after, with the
transport's other 180 tests.

### Before and after, A/B

Interleaved W F W F on the same Mac Studio, release builds: W `a26adb35` (the harness alone,
md5 `7f4e7933`), F `e107b571` (the harness and hyper-raft `31aa800`, md5 `11e9cdf3`); a closed
ladder of 1-byte PUTs to 512 clients, each step judged in rounds, the best step's median and its
95% interval:

| arm | trial | native ops/s | interval | http ops/s | interval | PUTs failed |
|---|---:|---:|---|---:|---|---:|
| W | 1 | 1,202.6 | 1,156.2–1,262.5 | 1,327.7 | 1,236.3–1,420.9 | 222 |
| F | 2 | 1,663.5 | 1,590.2–1,734.2 | 1,739.7 | (ordered) | 0 |
| W | 3 | 1,213.8 | 1,173.3–1,257.9 | 1,448.3 | (ordered; 998.7–1,811.6) | 260 |
| F | 4 | 1,674.7 | 1,625.3–1,753.0 | 1,704.8 | (ordered; 839.5–2,078.0) | 0 |

The A/A spread of each arm's best native step is 0.9% (W) and 0.7% (F), its HTTP step's 9.1% and
2.0%; F's native best is 38% above W's, its interval disjoint from both of W's, and its HTTP best
20% above. Every step of F asked no PUT again and failed none; W at 512 asked 7,551 (native) and
14,718 (HTTP) again and failed 45 and 151, and each W trial ended not exact. A PUT's median latency
at 512 native clients fell from 386 to 294 ms.

### Past 512 clients

F alone, one ladder to 2,048 clients (8 GiB volumes): native 1,934.8 a second at 1,024 (1,690.4–2,368.7),
1,412.0 at 2,048 with rounds ordered in time; HTTP, run after it with the native door's 2,048
clients still connected, 434.3 at 128 and then falling, 935.0 at 1,024. Two other refusals appear
there, neither traced yet: native PUTs refused three times before a node took them, the last
`Pending` (a node's bound on handshakes under way: 163 at 512, 760 at 1,024, 12,766 at 2,048, each
step opening as many new clients at once as it adds), and HTTP PUTs answered `SlowDown` from 256
clients (1,077 asked again at 256, 189,379 at 1,024), where the A/B's HTTP steps to 512, run with no
native clients held, asked none again. The volumes filled during the HTTP door's 2,048 step (553,889
refused), so nothing past it is a measure.

## Large PUTs

One client, closed, the same Mac Studio and diagnostic build (`7fd3ba28`), each PUT's time split
by what it had out (`diag/large.py`):

| size | door | PUTs | per PUT | MiB/s | chunks out | commands alone | body and seal |
|---|---|---:|---:|---:|---:|---:|---:|
| 64 MiB | native | 8 | 0.65 s | 99 | 74–84% | 13–19% | 3–8% |
| 64 MiB | http | 8 | 0.49 s | 130 | (same cell) | | |
| 1 GiB | native | 4 | 8.1 s | 126 | 76–99% | 1–24% | 0–1% |
| 1 GiB | http | 4 | 5.5 s | 185 | (same cell) | | |

The chunks' writes are the PUT: an 8 MiB chunk took 60–70 ms at the median to be durable on its
three volumes with one block's window, against 8 ms for the device to write and `F_FULLFSYNC` 8 MiB
alone (1,011 MiB/s, one writer; 1,187 MiB/s, three). The first PUT of a run holds one block's
chunks at once and commits a block in 85 ms (11.6 s for 1 GiB); once the window is learned it
holds two to four (5.2–7.0 s). The three volumes share the one SSD, so a 1 GiB PUT writes 3 GiB to
it: 378–555 MiB/s of the device's 1,187.

## The log's flushes under load

Each node's log over the 1,024-client phase (20.4 s, hyper-log's `LogStats`): 637–746 frames,
2.2 updates a frame, 976–1,106 flushes (1.5 a frame, the rest confirmations written on their own),
a flush 13.4–14.5 ms at the mean, an update's commit wait 46 ms at the median. An entry a node asks
of a range carried 58–81 commands at the mean (at most 330) and took 76–100 ms from asked to
answered: two of those commit waits, the leader's and a follower's.

## What changed

The harness judges each closed step in rounds and ends the ladder three steps after the last that
grew, and every phase says how long its process was stopped (measurement.md §12,
`crates/mantle/src/bench_doors.rs`); hyper-transport's end waits for a whole reply's head
(vendor/UPSTREAM.md).
