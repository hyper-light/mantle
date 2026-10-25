# The roll on a busy host

**Question.** Why does the roll test (tests/serve.rs, a cell of three made by init, each node
stopped in order and started again) see a member say a range has no leader on CI's busy job, two
CPUs beside two busy loops, while the others pass? Its criterion: from the moment both remaining
members name a range's new leader until the stopped node is back, neither says that range has no
leader.

**What failed.** CI run 40139142704 (411ec501) on linux-x86_64-2cpu-busy, and in diag run
40149410865 (abef6120 with scratch branch `diag/roll-busy3`, whose harness keeps every term and
leader move, every link change and the roll's steps, and prints them at a failure) two rounds of
sixteen, on two of three ubuntu-24.04 runners, the suite on CPUs 0-1 beside two busy loops.

| runner, round | stop | what the members said |
|---|---|---|
| busy-a, 3 | the third | The leader of every range, restarted 4 s before, suspected the only other live voter three times in 1.7 s, its connection to it up and keyed throughout. Each time it found no trusted quorum and stepped down with no trusted heir, and was elected again 150 to 250 ms later: terms 5 to 9. |
| busy-c, 5 | the first | The two nodes that had run longest suspected each other 250 ms after their links keyed, as the cell formed; neither suspicion was told again. Once the third node stopped, each leader lost its trusted quorum the moment it suspected it, and terms went 3 to 8 within about 100 ms, each new leader stepping down 7 to 20 ms after its election. |

**The detectors plan these mistakes.** hyper-raft's configurator (`hyper_timing::configure_arrivals`,
313ada8) chooses each pair's interval and margin to minimize the time a group cannot commit,
pricing a crash's detection once an MTBF and each false suspicion at an election's cost
(hyper-raft `docs/timing.md` §2.2). Run on one lateness distribution with the MTBF it is given
varied (scratch program over the vendored crate; timer granularity 1 ms, sender's floor 10 ms,
lateness mean 5 ms and deviation 40 ms, a mature history of 5,000 independent arrivals), the
mistake recurrence it promises, per pair, for an election cost of 100 and 300 ms:

| MTBF | margin | a false suspicion every |
|---|---|---|
| 2 s | 187, 250 ms | 2.1, 4.8 s |
| 10 s | 283, 373 ms | 7.0, 15.9 s |
| 60 s | 443, 580 ms | 26.7, 59.9 s |
| 10 min | 777, 1,010 ms | 145, 318 s |
| 1 h | 1,188, 1,527 ms | 518, 1,102 s |
| 1 day | 2,389, 2,983 ms | 4,237, 8,256 s |

With a young history (60 arrivals) the same configurator promises a mistake 3 to 5 times as often
(MTBF 1 h: every 137 to 243 s).

**The MTBF a start gave them.** Each start began with no history (`Exposure::new`, node.md §2.4
item 7, not built then), and the stream's MTBF before a failure is `2T` over the node time it has
watched (the Jeffreys posterior, hyper-raft timing.md §2.6): seconds for a process started seconds
ago, and every restart it sees, an orderly stop's included, counts as a failure. The leader in
busy-a had run 4 s; the two nodes in busy-c had watched no pair before the cell formed.

**What changes.**

- Item 7 (branch `cell-history`): the root range keeps each node's time run and crashes, the runs
  that ended without an orderly stop, and every start gives the cell's sum to its stream; an
  orderly stop is no crash. A restarted node's detectors start from the cell's evidence, not from
  its own seconds.
- In a group whose live voters are exactly a quorum, as during a roll, a suspicion between them
  cannot lead to a better leader: a follower's campaign can win only by the vote of the leader it
  suspects, and a leader's step-down hands its quorum to no voter that can lead without it. Yet
  each one steps a leader down or arms a campaign, so every planned mistake costs the range its
  leader for an election. That rule is hyper-raft's core's (asked of it, 2026-10-25).
- Open: why busy-c's suspicions were never withdrawn while both nodes ran (scratch branch
  `diag/roll-busy4` traces every trust change and every heartbeat around it; its first run,
  40152571831, failed no round of sixteen).
