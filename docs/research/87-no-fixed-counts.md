# 87. No fixed counts in admission, batching or sessions

Status: research note and plan, 2026-11-12. For the owner's change to goal 16, verbatim: "No fixed
counts in admission, batching or sessions: only learned and data-derived values." Three counts are
fixed at lean-doors `c1813126`: the operations a node admits at once (`derive::OBJECTS`, 8), the
commands one of a node's entries carries (`derive::batch`, `gateway.objects` times
`gateway.window`, 8), and the answers a session keeps (`Rules::max_answers`, set to the batch). Sources
[1] to [9] are papers, a dissertation and kernel source, read on 2026-11-12 at the addresses given;
[10] is mantle's own notes and code at `c1813126`. Every claim carries its source or says it is
derived and how. The owner's answer to the ceiling question (§4) is quoted where it is used.

## Sources

[1] D. Ongaro, "Consensus: Bridging Theory and Practice", PhD dissertation, Stanford, 2014,
https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf. §6.3, pp. 70–72; §10.2.2, p. 143.
Already summarised in research/06 §A1.8–A1.9; re-read for this note.

[2] C. Lee, S. J. Park, A. Kejriwal, S. Matsushita, J. Ousterhout, "Implementing Linearizability at
Large Scale and Low Latency" (RIFL), SOSP 2015, pp. 71–86,
https://web.eecs.umich.edu/~manosk/assets/papers/rifl.pdf. §4.2, "Garbage collection".

[3] P. J. Denning, J. P. Buzen, "The Operational Analysis of Queueing Network Models", ACM Computing
Surveys 10(3), 1978, pp. 225–261, https://www.cs.ucf.edu/courses/cop6614/fall2004/p225-denning.pdf.
§"Bottleneck analysis", pp. 239–241.

[4] L. Kleinrock, "Internet Congestion Control Using the Power Metric: Keep the Pipe Just Full, But No
Fuller", Ad Hoc Networks 80, 2018 (preprint of 2018-06-29 read),
https://www.lk.cs.ucla.edu/data/files/Kleinrock/ — its abstract and §3, after Kleinrock's 1978 and
1979 papers, which were not read.

[5] N. Cardwell, Y. Cheng, C. S. Gunn, S. Hassas Yeganeh, V. Jacobson, "BBR: Congestion-Based
Congestion Control", ACM Queue 14(5), 2016. Read through its implementation [6], whose header cites
it; the article itself was not read.

[6] Linux v6.12, `net/ipv4/tcp_bbr.c`,
https://raw.githubusercontent.com/torvalds/linux/v6.12/net/ipv4/tcp_bbr.c, lines 1–45 (the model),
130–181 (the constants), 920–945 (PROBE_RTT).

[7] J. D. C. Little, "A Proof for the Queuing Formula: L = λW", Operations Research 9(3), 1961,
pp. 383–387. Cited for the law as research/27 and node.md §2.6 already use it; not re-read.

[8] P. Helland, H. Sammer, J. Lyon, R. Carr, P. Garrett, A. Reuter, "Group Commit Timers and High
Volume Transaction Systems", Tandem TR 88.1, §2; D. J. DeWitt et al., "Implementation Techniques for
Main Memory Database Systems", SIGMOD 1984, §5.2. As research/71 step 9 cites them; not re-read.

[9] Research/27 §5.1 (Breakwater, CoDel) and §6.1, and node.md §2.5–§2.6, §4.5: the admission
authority the design names and the budgets it derives by Little's law.

[10] Mantle at `c1813126`: `crates/node/src/derive.rs` (`OBJECTS`, `gateway`, `batch`, `cell`,
`asks`, `objects`, `derive`), `crates/node/src/ask.rs` (`Asking::next_entry`, `answered`),
`crates/node/src/node.rs` (`entry_limits`, `take_door`), `crates/node/src/node/host.rs`
(`take_object`, `take_local`), `crates/meta/src/session.rs` (`Rules`, `Touched::record`),
`crates/meta/src/apply.rs` (`apply_entry`), `crates/meta/src/record.rs` (`Session::row_bytes`),
`crates/meta/src/wire.rs` (`largest_answer_bytes`, `largest_row_bytes`); research/71 step 9.

## 1. What the sources establish

**A session's responses are bounded by the client's outstanding commands, and the bound is the
client's to keep.** Ongaro: "This approach also generalizes to allow concurrent requests from a
single client. Instead of the client's session tracking just the client's latest sequence number
and response, it includes a set of sequence number and response pairs. With each request, the
client includes the lowest sequence number for which it has not yet received a response, and the
state machine then discards all responses for lower sequence numbers" [1, §6.3 p. 71]. The
dissertation bounds sessions, not responses: "sessions cannot be kept forever, as space is limited",
and its two options are "an upper bound on the number of sessions" with LRU, or expiry "based on an
agreed upon time source" [1, p. 71]. It states no bound on the pairs a session holds.

RIFL makes the trade-off explicit and fixes a count: "Using a single sequence number to pass garbage
collection information between clients and servers ... forces a trade-off between RPC concurrency
on clients and space utilization on servers. If a client issues an RPC that takes a long time to
complete, then concurrently issues other RPCs, the completion records for the later RPCs cannot be
garbage collected until the stalled RPC completes. This could result in an unbounded amount of
state accumulating on servers. In order to limit the amount of state on servers ... RIFL sets an
upper limit on the number of non-garbage-collectible RPCs a given client may have at one time. This
number is currently set at 512. Once this limit is reached, no additional RPCs may be issued until
the oldest outstanding RPC has completed" [2, §4.2]. Mantle's `max_answers` is RIFL's 512 in another
place, and like RIFL's it is the client's to respect: a client past it is simply not served. What
the sources leave open is the unit of the bound. RIFL's reason is "worst-case server memory
utilization" [2, §4.2], which is bytes; a count bounds bytes only through the largest record.

**Expiry for room loses exactly-once; refusal keeps it.** Ongaro's expiry must be deterministic,
and "a command with no record of the session" is refused, not processed [1, p. 72]. A command whose
session expired has an unknown outcome to its client (research/06 §A1.8). Mantle expires a session
whose kept answers pass its bounds (`Touched::record`): the command past the bound is applied and
answered, and its session ends. If that reply is lost, every retry of the entry meets an expired
session, `SessionExpired`, and the gateway re-sends each command in a new session (`Asking::answered`),
where only the commands that carry a file are recognised as copies (replica.md §1). DERIVED from [1]
and [2]: a bound reached by *refusing the command before it takes effect*, its session kept,
keeps exactly-once. The refused command applied nothing and kept nothing. A copy of it reaching the
log later is a first application. The command can apply only once at its serial, since once it
applies its answer is kept until acknowledged, and after that it is `Repeated`. So refusal is the
second option CLAUDE.md §2 allows, "a typed refusal", and expiry for room was the third. It
was the wrong one for a bound the client can reach under correct operation.

**A batch is what waited, bounded by bytes, never by a count.** LogCabin's double buffering: "The
leader would keep one outstanding RPC to each follower. When that RPC returned, it would send
another one with any log entries that had accumulated in the meantime ... This approach is
appealing because it dynamically adjusts to load. As soon as load increases, entries will
accumulate, and the next batch will be larger, improving efficiency. Once load decreases, batches
will shrink in size, lowering latency" [1, §10.2.2 p. 143]. The cap on an AppendEntries is bytes,
"up to one megabyte" (research/06 §A1.9). Group commit has the same rule: one log write and flush
commits whatever waited for it [8]. A node keeps one entry out per range (`Asking::next_entry`),
so its entries already double-buffer. Only `max_commands`, the count, stops the batch below what
waited.

**The knee of a closed system is its throughput times its no-load latency.** Denning and Buzen: with
`k` jobs that never delay each other, `X₀ = k/R₀`, and since `X₀ ≤ 1/V_bS_b` (the bottleneck's
rate), "k ≤ N* = R₀ / V_bS_b ... In words, k > N* would imply with certainty that jobs queue
somewhere in the system. Since N* thus represents the load beyond which queueing is certain to be
observed, we call N* the saturation point of the system" [3, p. 240]. So `N* = X_max · R₀`. Kleinrock
calls the same point power's optimum, "Keep the pipe just full, but no fuller". Loading a system
there makes "the optimized average number in the pipe ... exactly equal to the Bandwidth-Delay
Product" [4, abstract]. Below `N*` throughput grows with every operation admitted; past it, each
operation adds one service time to everyone's latency and no throughput [4, §3; 3, Figs. 9–10].
Research/71 step 9's measured knee is this point found by hand: 30, 39, 72, 79 and 93 PUTs a second
at 2, 4, 8, 16 and 32 operations [10].

**BBR learns `N*` online from two filters, and probes past it by a gain cycle.** "bottleneck_bandwidth
= windowed_max(delivered / elapsed, 10 round trips); min_rtt = windowed_min(rtt, 10 seconds); ...
cwnd = max(cwnd_gain * bottleneck_bandwidth * min_rtt, 4)" [6, lines 6–10]. It starts at a gain of
`2/ln 2`, "the smallest pacing gain that will allow a smoothly increasing pacing rate that will
double each RTT", and stops once "after 3 rounds w/o significant bw growth" of 1.25× [6, 150–181].
In steady state it cycles its gain over eight rounds, `5/4` "probe for more available bw", `3/4`
"drain queue", and six of `1` "cruise ... without creating excess queue" [6, 162–168]. The drain
round is what refreshes the minimum: with fewer in flight than `N*`, nothing queues and the latency
measured is the no-load one. Where a flow never drains for the filter's ten seconds, PROBE_RTT cuts
it to the least that moves for 200 ms, "roughly 2% (200ms/10s)" of its throughput [6, 920–945]. A
max filter can be fed only by samples that measure the bottleneck. BBR takes an application-limited
sample only where it raises the maximum (`bbr_update_bw`), since a sample taken when the sender
had less to send says nothing about the pipe's limit [6].

**The design already names this form.** Node.md §2.6 derives every queue's bound by Little's law,
`Q = μ·d` [7], and §4.5 asks an admission authority whose concurrency "follows the server's grant
rather than a setting", Breakwater's credits [9]. The learned limit is the first piece of it: one
number, the operations a node runs at once, learned as `N*`. The sojourn test, the credits and
per-principal shedding stay as §4.5 plans them.

## 2. Each count, and what it becomes

| count | at `c1813126` | becomes | basis |
|---|---|---|---|
| `Rules::max_answers` (8) and `max_answer_bytes` (18,482) | a session past either expires as its entry applies | removed. A session keeps answers while its row fits a row, `Rules::max_value_bytes`, the largest S3 admits (`wire::largest_row_bytes`), so no page grows. A command whose answer would pass that is refused before it takes effect, `Answer::AnswersFull`, its session kept, and so is each of the session's later commands in the entry | [1] §6.3, [2] §4.2: bytes are RIFL's own reason; refusal keeps exactly-once (§1) |
| `derive::batch` (8) and `entry_limits`' commands | an entry carries at most 8 commands | an entry carries every command waiting that fits its bytes, `max_entry_bytes` (`wire::largest_entry_bytes`, 561,804) | [1] §10.2.2, [8]: the batch is what waited |
| the volume's batch (`identities × batch`) | the chunks the cell's operations may write a volume at once | the cell's identities times each node's admission ceiling times its window | the same rule, now from the ceiling |
| `derive::OBJECTS` (8) | the operations a node runs at once, and its native clients | a learned limit within `[1, C]`, the point of greatest power found by probing, from the operations' own completions; `C` as many operations as the operations' bytes hold, each charged its own (§4, §4a) | [3] p. 240, [4], [6], [7] |

The window, `gateway.window`, is one block, "the floor" (node.md §1.4). It is not one of the three
counts; node.md §2.6 plans it as `⌈R·T/B⌉`, and it stays.

## 3. The learned limit

**BBR's estimator does not carry over, measured.** BBR's `N* = X̂·R̂` needs `R̂` to be the no-load
latency, and takes it as the least sample in ten seconds. A path's round trip has one such value.
A node's operations do not: a 64 KiB PUT and an 8 MiB PUT differ a hundredfold. A model of
Denning and Buzen's bottleneck shows it: a closed system of `c` servers, first come first served,
exponential service, a thousand clients (`nfc-proto/sim.py`, kept with the experiment's data).
Built on BBR's form, with `R̂` the least of each round's mean latency, the limit fell to 1 or 2
operations at every knee from 3 to 40: 12% of the throughput at 8 servers, 2% at 40. A round of
one or two operations has a mean as noisy as one sample, and the least of noisy means lies below
the true mean, so each `X̂·R̂` undershot the last. Taking `R̂` as the mean of the round run below
the knee instead ran the limit up to the ceiling, its windowed maximum `X̂` as biased up as the
least was down. Little's law says why both must fail: `X·R̄ = N̄` in every phase, so a product of
throughput and mean latency only measures the load the node already admitted.

**Power has its maximum at the knee.** Kleinrock's power is throughput over delay, `P = X/R̄` [4].
By Little's law `R̄ = N̄/X` [7], so `P = X²/N̄`. Below the knee `X = N/R₀` and `P = N/R₀²`, which
rises with each operation admitted. Past it `X = X_max` and `P = X_max²/N`, which falls. So the
limit of greatest power is the knee, "Keep the pipe just full, but no fuller" [4, abstract]. That
holds for any mix of operations, since `R̄` is every operation's mean and no single one's least.

DERIVED from [3], [4] and [6]: the limit climbs to the point of greatest power by BBR's probing:

- **A phase is the limit held, then measured.** It begins once the operations admitted until its
  first completion have completed, BBR's packet-timed round [6]. That covers both those admitted
  before it and the burst its own limit let in, so the operations it measures ran under its limit.
  It then counts `n` completions and their latencies over its length. Its power is
  `n²/(length · Σ latency)`, and phases of equal `n` compare by `length · Σ latency`.
- **`n` is what tells a probe's change from chance.** A rate over `n` completions has a relative
  error of `1/√n`, and the log ratio of two phases' powers an error of `√(2/n)`. A probe a quarter
  away moves the log of power by `ln(5/4)` at the least. At the normal distribution's two-sided
  99% point, `z` 2.576, `n ≥ 2·(z/ln(5/4))² = 267`.
- **From the ceiling, halve while power does not fall a fifth**, the reciprocal of BBR's startup
  threshold `5/4` (`bbr_full_bw_thresh`). Past the knee halving raises power, or leaves it where
  the limit did not bind; below it power halves with the limit, and the halving is undone. The
  node starts where a fixed limit stood, admitting what its memory was split for. A first model
  started at one and doubled, as BBR does, and failed four of `crates/mantle/tests/serve.rs`'s 14
  on Studio 3, where lean-doors failed none: a native request past a limit of one at a node's
  start is refused `SlowDown`, and its client's backoff left the cell quiet past the harness's
  second.
- **Then cycle eight phases about a base**: a quarter past it, a quarter below, six at it (BBR's
  `bbr_pacing_gain` cycle [6]). At each cycle's end the base moves to the limit of greatest power
  among the six phases at the base, pooled, and each probe whose limit bound. A probe in which no
  operation was turned away measured what its clients asked, not its limit, and moves nothing, so
  a quiet spell does not walk the limit down below what the next burst asks.
- **The limit is at least 1, the least that moves, and at most `C`**, the ceiling the memory was
  split for (§4).

The same model, with the limit learned this way (`nfc-proto/power7.py`, n 60,000 completions a
case, the second half's mean, a thousand clients), at a ceiling of 1,024: the limit kept 1.1, 3.0,
8.0 and 39.9 at knees of 1, 3, 8 and 40 servers, and 2.9, 17.6 and 8.5 at knees of 3, 16 and 8
with sizes mixed a hundredfold. Throughput was 100%, 94%, 96% and 96% of the most, and 80%, 92%
and 93% mixed. At a ceiling of 64 it was the same within a limit's step; at 8 the limit held the
ceiling wherever the knee lay past it. A fixed limit at the knee gets
100% in this model, whose knee is a corner, with no gain past it at all. The probes' quarter below
and chance moves cost the rest. A real cell's knee is rounder: research/71's doubling past 8 still
added 10%.

## 4. The ceiling: an experiment

The memory a node may hold is split at start (`derive::floor`, `derive::shares`). The floor holds
each class at what its configuration lets it hold. What remains past the floor goes to each
volume's index and each engine's resident metadata by their wants, the fragments and rows of the
objects the cell's volumes hold at capacity. At `c1813126` the objects class's floor is `OBJECTS`
operations, each at its most bytes (`derive::objects`): about 42 MB at the largest blocks, a
third of the node's floor on the doors' baseline box (measurements/2026-11-06-doors-baseline.md).
A learned limit needs its ceiling `C` fixed when the memory is split, and the sources give no rule
for splitting memory between concurrency and capacity. An index's want is its volume's capacity:
fewer fragments indexed is fewer chunks the volume takes. Every start-time rule is a priority, a
fraction or a borrowing:

- **A. Operations first, to the clients' patience.** `C` is the most operations the memory past
  every other class's floor holds, and no more than an operation's clients can use. A client waits
  60 s (`bounds::STOCK_PATIENCE`, research/77 D2), so past `X·60 s` an admitted operation ends
  past its client's patience. The data takes what remains.
- **B. Borrowing.** The floor holds one operation. Further operations borrow, at run time, the
  memory the indexes' and engines' shares hold and do not yet use, and give it back as the volumes
  fill.
- **C. A cited fraction.** The operations take a fixed part of the memory past the floor, as Linux
  sizes `tcp_mem` from free memory (`net/ipv4/tcp.c`, `tcp_init_mem`).
- **D. The data first.** The operations take what the indexes' and engines' wants leave, at least
  one. DERIVED: an index wants 352 bytes a fragment at a fragment a 4 KiB block
  (`mantle_chunk::index::FRAGMENT_BYTES`), 94 GB for a 1 TB volume. On any box whose volumes
  outsize its memory by about 12,000 times, D is one operation, the 20–47 writes a second of
  research/71 step 9.

The owner, asked to choose: "Each of these has distinct advantages and disadvantages. This is an
ideal time to experiment. A/B our options, profile and benchmark across a variety of scales from
small to massive, and determine which one or combination works best." So `C` is chosen by
measurement. Each rule is built behind one derived value, the experiment runs, and the rule that
wins stays alone.

**The matrix.** Scales, by the allowance and the volumes stated:

| scale | allowance | volumes |
|---|---|---|
| small | 2 GiB (a container's `memory.max`) | one of 4 GiB |
| laptop | 16 GiB | one of 256 GiB |
| box | 64 GiB | four of 1 TiB |
| massive | 512 GiB | 24 of 16 TiB, derived only |

What is measured:

1. **Derived, every scale and rule.** `C`, the memory each class holds, the fragments each
   index holds (the volume's capacity in chunks), and each engine's resident extents, from
   `derive::derive` alone. A rule that gives a scale fewer fragments than its volumes hold of
   small objects, or fewer operations than the knee, is named with what it costs.
2. **Measured, small to box on Studio 3.** The doors' write and read throughput and latency,
   closed ladder to past the knee, and an open loop below it. Three `mantle serve` under each
   scale's memory limit, in Docker for the Linux limits. The learned `L` is traced over each run.
3. **Profiled.** The node's peak resident memory against its derived floor, at the ceiling each
   rule gives, so a rule whose ceiling is real memory is shown to hold within it.

## 4a. Bytes, not a count: each operation charged its own (2026-11-12)

The ceiling `C` stood at `derive::OBJECTS`, 8 operations, each charged the most any operation holds:
42 MB, a put of the largest blocks under the widest scheme with its copies (`derive::objects` at
`c1813126`; the doors' baseline's floor, 83.5 MB at two). A PUT of one byte held a few hundred
kilobytes of that. Every count the node sized from it followed: its native clients, its asks,
the transport's peers and a volume's batch. Research/71 step 9 measured the cell's writes at
30–93 a second at 2 to 32 operations a node, so `N = X·R` [7] at the owner's 10,000 a second and
the 100 ms a PUT takes at one client (seven flushes on its path, research/71) asks about a
thousand operations in the cell at once. A count of 8 a node, or 42 MB each, holds 24.

What changed:

- **An operation is charged what it holds at its own size.** A write's body length is in its
  head before its fields are read (`Head::written`, `Op::written`). It is charged its first
  segment and first block, the largest of its own, each chunk of a block copied to send, and a
  piece of its body, under the scheme that holds the most of it (`put::bytes_for`,
  `derive::Charges`). The put driver reserves its segment at the same length, where it reserved a
  whole 64 KiB segment for any body. Every other operation is charged the most, since a get's
  object's size is known only once its row is read. A body past one block is charged what the
  largest put holds.
- **The operations' memory is bytes**, `gateway.objects_bytes`, and an operation is admitted only
  where its bytes fit beside the running ones' (`crate::admit`). One turned away for bytes says
  nothing of the limit. One turned away by the limit tells it its clients ask more.
- **The ceiling `C` is as many operations as the share holds at what one costs**: the least it
  is charged, a write of no bytes, and what one more operation adds to the node's other classes,
  measured on the derivation's own model as the difference between one operation and two
  (`derive::operations`): its native client's connections and kept exchange, its pooled exchange,
  its chunk asks' exchanges and each range's state for its read. The share holds one operation
  of the largest blocks first. The native clients a node admits are as many. No count is stated.
  A first build counted `C` at the least charge alone, and its clients as peers whose connections
  each hold every stream a peer node's may: 12 TB of transport at a 16 GiB node, a peer's streams
  times its clients. So clients became a pooled role (below), and their cost entered `C`.
- **Native clients draw their exchanges from a pool** the node sizes to its learned limit,
  hyper-transport's `StreamCredit::Pooled`, Breakwater's credit (Cho et al., OSDI 2020): a
  client's connection keeps one exchange, mantle-client's one at a time, and draws the rest from
  the pool, so it costs its own connections and that exchange, and a client past the pool waits
  at its end, costing the node nothing, as the audit's #1 replacement names.
- **A native request past the limit waits** with its body unread until room frees, as the S3
  door's does (node.md §4.5), vouched each beat and answered `SlowDown` only at its client's
  patience. Answered at once, it cost its client a throttle's backoff, one to twenty seconds of
  full jitter (mantle-client's botocore-derived `SLOWED_BASE`, `SLOWED_MOST`), a ladder's whole
  step at a few hundred clients.
- **A volume's batch** carries every chunk waiting that a segment's room holds at a block each,
  within the records its frame holds (`layout::most_batch_requests`), where it was the cell's
  identities times the operations' chunks.

The bytes' rule is §4's experiment, among the rules derived or learned from the node's own
facts: A (operations first, to their clients' stated patience), B (borrowing at run time what the
indexes and engines hold and do not yet use) and D (the data first). A fraction copied from
another system's tuning, as Linux's `tcp_mem` share of free memory (rule C, briefly the first arm
here), is not eligible: it is another implementation's tuning however well it is cited (the
constants audit of 2026-11-12; the owner's rule of no arbitrary values). The branch derives **rule
D** until the experiment chooses: the floor with one operation of the largest blocks, the
indexes' and engines' wants beside it, each taken whole, and what the memory holds past both is
the operations', with the one (`derive::derive`). D gives the engines' spare, what no want takes,
to the operations instead. Rules A and B are built as variants beside it and measured as §4 asks.

## 5. What remains unknown

- Whether `N*` learned from mixed sizes holds for the large ones. Each operation holds at most a
  window of one block, so a large and a small operation hold the node alike while they run. A
  large one runs longer, which `R̂` (the least) does not see. Measured in §4 step 2 at 8 MiB and
  64 MiB.
- A session whose commands' answers are all `Moved` at the longest lineage (12,775 bytes,
  `wire::largest_answer_bytes`) gets one applied an entry, the rest refused and asked again. That
  is correct, and slow while a range's keys move. Not measured.
- §4.5's sojourn test, credits and per-principal shedding are not built; the learned limit is the
  node's one admission number until they are.
