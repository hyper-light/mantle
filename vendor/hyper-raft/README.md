# hyper-raft

The Raft core slates, focal and mantle share: elections and the log as a state machine
with no clock, no disk and no network. It is told what time has passed (`RawNode::tick`)
and what arrived (`RawNode::step`), and it says what to persist, send and apply
(`RawNode::ready`). A durable shell drives it: focal's is `focal-consensus`; the shared one
will be `hyper-durable` (`docs/raft.md`).

It began as focal's own core, `focal-raft`, and moved here with its history. `ORIGIN.md`
records the source revision and every change since.

It is Raft as Ongaro's thesis states it, with pre-vote, check-quorum, election priority,
learners, joint consensus, leader transfer, an inflight window with conflict hints,
ReadIndex and snapshots. It keeps the log and speaks the messages of `raft-rs` 0.7
(`raft-proto`), which focal's groups ran on before. What it decides differently, and
why, is in the module header of `src/raft.rs` and in focal's
`docs/archictecutre/27-consensus-roadmap-and-slates-port.md` §4.5 (focal `a8e95f7`).

A group may have the fast track (`Config::fast`, `fast.rs`, `track.rs`; focal 27 §4.6): a
member that does not lead proposes to every voter at once (`RawNode::propose_fast`),
and its entry is committed when three quarters of the voters hold it, or a majority
holds it from the leader, whichever is first.

## Rules

- Nothing unwinds. `Error` says whether an operation was refused and changed nothing,
  whether a peer's message contradicted what the member holds, or whether the member's
  state no longer adds up (`Error::is_fatal`), which alone stops the replica.
- Everything that grows has a bound (`Limits`, `MAX_MEMBERS`).
- A run is its seed: election timeouts are drawn from `Config::seed`.

## Tests

| Where | What |
|---|---|
| `src/**` | the log, quorums, configurations, progress, reads; what the core refuses and the bounds it keeps |
| `tests/differential.rs` | this core and `raft-rs` on one schedule, compared after every step |
| `tests/group.rs` | groups of this core, and of both cores together, under schedules: safe whatever the schedule, and settled once the network is whole; the decisions of focal 27 §4.5 |
| `tests/fast.rs` | the fast track: committed by the fast quorum, taken again by the leader that follows, and groups that propose by it under schedules |
| `benches/replicate.rs` | what replication costs with either core |

The fast track's TLA+ model is still in focal (`docs/models/FastTrack.tla`). It moves here
with the fast-track decision (`docs/raft.md`).

`HYPER_RAFT_SEEDS`, `HYPER_RAFT_STEPS` and `HYPER_RAFT_SEED` set how many schedules
run, how long each is and where they begin. A failure names its seed and its step:

```
HYPER_RAFT_SEED=1 HYPER_RAFT_SEEDS=1 cargo test -p hyper-raft --test differential
```

Every count the tests print is fixed by the seed but one: in
`a_group_of_both_cores_is_safe_and_settles`, `raft-rs` draws its own election timeouts
from `rand::thread_rng`, so the terms each core led vary from run to run.
