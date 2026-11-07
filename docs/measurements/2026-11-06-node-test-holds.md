# The node test on a host that holds a process still

**Question.** Why did `objects_round_trip_through_any_node_and_read_back_after_one_is_killed`
(`crates/mantle/tests/node.rs`) fail under load? It failed once in the gates of `aa5cbf62` on this
Mac at load ~70, its harness quiet 4.92 s after node 1 was killed, started again and enlisted, reads
through it moving nothing. It failed twice in six rounds on the box on 2026-11-06 between 16:42 and
17:00 (main `cc720c05`, test binary SHA-256 `079c4a871d6422c8…`, pinned to two CPUs beside four busy
loops): round 3 ended "o5 through another node: Lost(Closed)" at 83 s, in the first reads, before
any kill; round 5 ended "quiet for 4.013634842s with nothing moved" at 130 s, in the reads through
node 2 started again, six of its ten done. The four passing rounds took 188 to 250 s.

**The host then.** In that window the box also ran the validate-targets agent's 100M-key `ranges`
bench, 32 threads in a container whose memory it capped on a host already short of it (branch
`validate-targets`, `9ce07c94`). kiln's headroom log on the box shows its disk probes out past
their 1 s tolerance again and again in the window. The box swaps to zram: at 17:43, 32.9 GiB of
122.9 GiB in use, swapping in at 551 MB/s, with memory pressure "some" at 3 % and I/O pressure
"some" at 22 %.

**Load alone did not fail it again.** After that bench ended, every run below passed but two, and
those two failed on the logging (below). The diagnostic build is `cc720c05` with a patch that never
lands: every QUIC connection's establishment, close and loss with quinn's reason, every node line
and every object operation stamped with the wall clock, and, at a quiet watch's end, each process's
threads with their state, wait channel and `schedstat`. The box's load ran 41 to 135 from others'
work.

| Binary | CPUs | Busy loops | Besides | Passed | Failed |
|---|---|---|---|---|---|
| main's | 0, 1 | 4 | an `eu-stack` sampler, which yama's `ptrace_scope` 1 refused: it sampled nothing | 16 | 0 |
| diagnostic | 0, 1 | 8 | | 30 | 1, the logging's |
| diagnostic | 2 | 4 | one CPU | 15 | 1, the logging's |
| diagnostic | 4, 5 | 4 | the unit's `memory.high` at 700 MiB | 26 | 0 |
| diagnostic | 6, 7 | 4 | `memory.high` at 600 MiB, its reads held to 4 MB/s | 7 | 0 |
| diagnostic | 6, 7 | 4 | every round's lines kept | 18 | 0 |

**The logging's own failures.** Two loops failed at the same instant (17:51), "quiet for 3.35 s"
and "3.43 s". Their lines went to a file on the box's root file system, btrfs at 97 % full, and in
each test process one of the harness's reader threads sat in D state in btrfs's `reserve_bytes`
holding stderr's lock, so no node's lines reached the watch. With the lines on tmpfs, no round
failed. The quiet watch fails when the test process itself is held still.

**Round 3, reproduced exactly.** A probe holds one node still (SIGSTOP, then SIGCONT) at a chosen
operation of the object client, with no busy loops:

| Node held | At | For | Outcome |
|---|---|---|---|
| 1 | o5's whole read in the first reads | 2 s | passed |
| 1 | o5's whole read in the first reads | 4 s | "o5 through another node: Lost(Closed)" |
| 2, started again | o4's read through it | 2 s | passed |
| 2, started again | o4's read through it | 5 s | "o4 through node 2 started again: Lost(Closed)" |

In the 4 s hold, node 2 lost its link to node 1 `TimedOut` about 3.1 s in. Node 1, continued about
4.1 s in, timed out the client's connection and its link to node 3 on its first turn, and node 2
reset it. The client's own idle timer ran out 4.65 s in, later than 3.072 s because an endpoint
restarts the timer when it sends its first ack-eliciting packet since it last received one (RFC 9000
§10.1). Op 9 ended `Lost(Closed)`. The idle timeout is three probe timeouts at the initial estimates,
3.072 s (`mantle_protocol::bounds::idle_timeout`, RFC 9000 §10.1's floor). A node silent that long
is gone to every peer, and an exchange on its connection ends unanswered. The node crate's client
hands that end to its caller, as mantle's client does ("an exchange that ended otherwise without a
reply is the caller's"); the test's object client asks nothing again, and the test fails on it.

**Round 5 is not a held node.** Held 5 s at round 5's point, node 2 gives `Lost(Closed)`, not a
quiet watch: the client's idle timeout ends the read before the watch's 4.01 s. Round 5's read had
not ended 4.01 s after the last movement, so either the client's connection to node 2 had not gone
silent for 3.072 s, or the client was not running to see it. A test process held still fits: the
watch's thread, its deadline passed during the hold, wakes before the reader threads forward what
the nodes said, as the logging's failures showed. What round 5 kept does not rule out a stall
inside node 2's read with its transport still answering. In passing rounds every node says a line
at least every 0.19 s after node 2 starts again, and the link counters round 5 showed (node 1 to
node 2: 6 dials, 5 ended) appear in passing rounds too.

**A defect the trace found.** hyper-rt-transport's driver (`Driver::poll_event`) drains only the
sockets whose readiness it took in an earlier turn. It fires the timers due, and only after that
takes the readiness that fired (`poll_ready`). So a node continued after a hold fires its
connections' idle timeouts before reading the datagrams waiting in its sockets. In the 4 s hold,
node 1 timed out the client's connection on its first turn while the client still had 0.55 s on its
own timer, and whatever the client sent during the hold sat unread in node 1's listener socket. A
diagnostic driver changed the order: it takes the readiness before the drain, and fires timers only
at a clock read before a drain that left no socket holding datagrams. It changed none of the probes'
outcomes: 2 s passed; 4 s and 6 s ended `Lost(Closed)` with the client's own timer running out
first, because the client had sent nothing ack-eliciting during the hold. The order still matters
when a whole host is held still with datagrams in flight: each end then reads the other's datagrams
before its timers fire, so the connection is kept. hyper-raft fixed the same order for its E2E
members' heartbeats (`d712a5f`: "poll their streams only at a clock read before a drain that
emptied their socket"). The driver is hyper-raft's, and the change is made there.

**What stays open.** If a host holds a process still past 3.072 s, the test fails, because it asks
every read to succeed with no deadline. Two decisions remain, and neither one is a timeout raised
to quiet the test:

- Should mantle's links outlast such holds? That would take the idle timeout from the liveness
  detector's bound for starved hosts (hyper-raft `e6b3372`) rather than RFC 9000's floor, and the
  research for it is not done.
- Should the harness's watch count time its own process was held still? Today it does.

The probes' and loops' runs are under `~/transfer-runs/{node-flake,node-diag,stall-probe,order-probe,quiet-probe}-*`
on the box.
