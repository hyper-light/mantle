# 32 — One transport, one membership layer and one Raft for mantle, slates and focal

**Status:** research input for a decision the owner has taken in principle. This note does not
decide; §3–§5 propose and §6 lists what the owner still has to settle. It touches the design
records for the node's transport (`node.md` §3), the Raft log (`raft-log.md`) and the replica
(`replica.md`), and the corresponding records in slates (`docs/wip/SLATES_DESIGN.md` §4.8,
§4.10a, `docs/wip/fleet-transport.md`) and focal (`docs/archictecutre/27-consensus-roadmap-and-slates-port.md`).
**Compiled:** 2026-09-30.

**The owner's decisions, verbatim (2026-09-30).**

1. "A shared transport crate: the patched quinn-proto plus the application layer could become one
   crate that slates, focal and mantle all use, so each refinement lands once for all three" —
   agreed.
2. "We should also likely do a shared RAFT crate given we've re-implemented parts of it here, in
   ../slates, and in ../focal."
3. "Note that we also use a separate udp protocol for SWiM. Dual-layer." The transport is two
   layers in all three designs: QUIC for stateful transfers, and a separate sealed UDP datagram
   protocol for SWIM membership and failure detection and for small consensus control messages.
4. "That also means all of slates and focal's enhancements." The shared crates carry every
   enhancement either project has built. The only exclusions are the defects already identified
   (slates' capped PTO backoff, its 1 ms first handshake retransmit, its payload-only bytes in
   flight, its class carried in the stream ID and its whole-exchange retention; focal's fixed
   constants) and anything else that measurably conflicts with an RFC or with the rules, each
   stated with its reason. Where a defect sits beside a good mechanism, the mechanism stays and
   the defect is fixed.

**Scope.** (1) An inventory of every Raft, transport, datagram and membership component in the
three repositories, with its tests and its quality against the rules (§2), and a ledger of every
enhancement and fix either sibling has built, with its evidence and where it lands (§2.11–§2.13).
(2) The shared crates: boundaries, sans-io APIs, and what each repository gains and gives up
(§3). (3) Where they live and how they are versioned (§4). (4) An ordered, reversible migration
for each repository (§5). (5) Risks and open decisions (§6).

**What this note does not repeat.** The QUIC choice and the per-mechanism verdicts on slates'
and focal's transport are note 30 §6; this note extends them to the whole enhancement record and
to SWIM and Raft. focal's stack as it stood on 2026-09-28 is note 07; slates' is note 08. The
audit's comparative findings are `docs/audit/2026-09-29_audit.md` §11 ("audit §11.x"). The
runtime and its thread model are notes 25 and 26.

---

## 0. How to read this note

**Revisions read.** Every path and line below is at these revisions, working trees included:

| Repository | Revision | Branch | Uncommitted |
|---|---|---|---|
| mantle | `fc8dacd` | `dev` | many (git status at the start of this work); none in `crates/log` or `crates/range` that this note relies on beyond what the tree shows |
| slates | `fdaa51d2fd373f81231b1ad54d446497cb1d8b4a` | `main` | `crates/cluster/src/content.rs`, `crates/server/src/daemon.rs`, docs |
| focal | `ff5f666` | `slates-port` | `examples/` (untracked) |
| quinn-proto | 0.11.18 | crates.io, as focal resolves it | — |

mantle pins focal-raft at `1395e223c065e85158ec1611fdc72073d28fd8a0`
(`crates/range/Cargo.toml:13`). Five focal commits since then touch `crates/focal-raft`
(`c1af831`, `078d9ac`, `653616e`, `58c8141`, `1f87bfc`); focal's checked-out branch is now
`slates-port`, not the `r10-r11-windows-ci` note 07 §0 named.

**Citation tags.** Source as `repo path:line`. slates bug records as `slates bug <date>-<name>`
(each is `docs/bugs/<date>-<name>.md` in slates). slates measurements as `slates BENCHMARKS
"<heading>"` (`docs/wip/BENCHMARKS.md`). focal's consensus plan as `focal 27 §x`. Earlier mantle
notes as "note NN §x".

**Evidence labels**, as in notes 12, 23 and 30:

- **primary**: a standard, vendor documentation or a library's source, read directly. Every
  sibling file:line in this note is primary.
- **RECORDED**: a measurement or bug record in a sibling's own ledger, read but not re-run here.
  Nothing in this note was built or run.
- **DERIVED**: arithmetic or interpretation made by this note.
- **INFERENCE / Recommendation**: reasoning for the shared crates, citing what it rests on.
- **UNVERIFIED**: stated by a record's title or a doc that the code was not traced for.

---

## 1. Decision-relevant summary

1. **There are two Raft cores, three durable shells and two QUIC transports today.** focal-raft
   is the core focal and mantle run (mantle through a git pin); slates runs its own hecate core
   (`slates crates/cluster/src/raft.rs`, 6,262 lines). The shells are focal's `DurableNode`
   over focal-log (disk, two fences per commit), mantle's `Replica` over its per-device
   `mantle_log::Log` (disk, one flush per frame, staged Ready, early leader sends, protocol-aware
   repair) and slates' `SavedRaft` publication (RAM only, by slates' rule R1). The transports are
   quinn plus focal's application layer, and slates' private RFC 9000-shaped dialect. mantle has
   no transport yet. (§2.1–§2.7)
2. **No repository runs SWIM on a separate UDP socket today.** The two-layer design is in all
   three design records, but slates sends SWIM probes and Raft messages as `Priority::Control`
   requests on its session plane (`slates crates/cluster/src/swim.rs:731`,
   `crates/cluster/src/raft_wire.rs:698`), focal sends probes as QUIC requests through its peer
   pool (`focal crates/focal-node/src/liveness/driver.rs:18-19`), and slates' sealed
   control-datagram codec is wired to nothing (note 08 §6). The shared datagram crate is new
   construction from slates' codec and mantle's keying design, not a merge of two running planes.
   (§2.8–§2.9)
3. **The enhancement record is large and almost entirely compatible.** §2.11–§2.13 list 54
   transport items, 10 datagram items, 17 SWIM items and 41 Raft items. The exclusions are the
   owner's list plus four more, each for a stated RFC or rule conflict: slates' `tls12` rustls
   feature, its counter-zero sealer, focal's allocate-before-admit receive (already fixed in focal
   since the audit, `02b2002`) and out-of-order commitment (slates' own model refuted it).
4. **Proposed crates** (working names, `x-` a placeholder prefix): `x-quic` (vendored quinn-proto
   with patches), `x-transport` (mantle's application protocol in slates' shape, sans-io),
   `x-datagram` (the sealed plane), `x-swim` (sans-io detector), `x-timing`, `x-raft` (the core),
   `x-durable` (the Ready pipeline and shell), `x-log` (the shared per-device log), and test
   infrastructure `x-sim` and `x-check`. Every one is sans-io or owner-threaded and takes
   budgets and wakers through `std` traits, so tokio (focal, mantle's network runtime), slates'
   thread-per-core runtime and mantle's shards each drive it with an adapter. (§3)
5. **The strongest implementation of each piece, by evidence.** Core: focal-raft (80.8 M steps
   equal to raft-rs, note 07 §1.7), with slates' measured core enhancements ported into it. Shell:
   mantle's Ready pipeline (early leader sends, staged parts, PAR repair, power-loss simulation)
   with focal's budgets, unwind boundary, decoder fences and nonblocking cross-group receipts.
   Log: mantle's (one flush per frame against focal-log's two fences of two file flushes, a
   directory flush and a rename each, note 07 §10.4). QUIC: quinn-proto with slates' measured
   refinements as patches (note 30 §6.3). Application layer: slates' shape with focal's measured
   lanes, waits and pool rules. SWIM: slates' sans-io detector with focal's witnessed extensions.
   Fast track: open between two algorithms; §3.8 proposes the evidence that decides it.
6. **Where:** a new repository under `github.com/hyper-light`, its own workspace, its rules the
   strictest union of the three, consumed by git revision. focal's toolchain (1.94.1) has to move
   to 1.98.0; slates takes its first git dependency; mantle's `deny.toml` gains one allowed source.
   (§4)
7. **Order:** core first (a move with no behaviour change, then slates' enhancements one at a
   time), then the shell and the log, then the QUIC fork with zero patches, then patches one at a
   time under both projects' grids, then the application layer, then the datagram plane and
   SWIM. slates' consensus and session plane move last, because they are the largest change and
   slates is the only one whose wire and core both change. Each step is reversible by pin until a
   durable format changes; the two format changes (focal's WAL, slates' group re-founding) are
   named as the one-way steps. (§5)

---

## 2. Inventory

### 2.1 What each repository runs

| Layer | mantle | focal | slates |
|---|---|---|---|
| Raft core | focal-raft at `1395e22`, git dependency (`crates/range/Cargo.toml:13`) | `crates/focal-raft` (11,324 lines incl. tests) | `crates/cluster/src/raft.rs` hecate core, own wire (`raft_wire.rs`) |
| Durable shell | `crates/range/src/replica.rs` (1,492 lines) | `crates/focal-consensus` (8,969) | `SavedRaft` publication (`raft.rs:611-635`) into RAM |
| Log | `crates/log` (4,976 lines src) per metadata device | `crates/focal-log` (5,391) per node | none: RAM only (rule R1) |
| Multi-log | — | — (focal's "multi-log" is leader balancing across independent groups, focal 27 §5) | `crates/cluster/src/multilog.rs` (MLRaft) |
| Timing laws | `Settings` fixed per node (`replica.rs:45-62`) | `crates/focal-timing` (1,053) | `crates/cluster/src/timing.rs` (866) |
| QUIC | none | quinn 0.11 + quinn-proto 0.11.18 + `crates/focal-wire` (21,015) | `crates/transport` (20,675), own dialect over `rustls::quic` |
| Congestion | — | `focal-wire/src/congestion.rs` (Copa behind quinn's trait) | `transport/src/congestion/copa.rs` |
| Datagram plane | designed (`node.md` §3.4) | none | `seal.rs`, `accept.rs`, `schedule.rs`, `enrollment.rs`, unwired |
| SWIM | designed (`node.md` §3.5) | `crates/focal-node/src/liveness/` (2,903) | `detector.rs`, `swim.rs`, `coordinates.rs`, `fixed.rs`, `membership.rs` |
| Simulation | `crates/range/tests/sim.rs`, `mantle_disk::sim` | `crates/focal-sim` (1,777), focal-raft `tests/` | `crates/rt/src/sim.rs`, `cluster/tests/support/timed.rs`, `explore.rs` |
| Model checking | — | `docs/models/FastTrack.tla`, TLC in CI | `cluster/tests/support/exhaustive.rs`, `slot_model.rs`, `prefix_model.rs` (Rust, exhaustive) |
| Linearizability | `crates/range/tests/support/linear.rs` (WGL with Lowe's memo) | — | named as nightly in slates `CLAUDE.md` §4; not found as code here |
| Runtime | tokio planned for the network side, owner threads for shards (`node.md` §1.2) | tokio | own thread-per-core executor; tokio banned (slates `CLAUDE.md` §2 item 2) |
| TLS provider | AWS-LC, vendored (`vendor/`) | AWS-LC, vendored (`focal vendor/`) | `ring`, rustls with `tls12` (`slates Cargo.toml:85`) |
| Toolchain | 1.98.0 | 1.94.1 (`focal rust-toolchain.toml:2`) | 1.98.0 |
| License | MIT, Hyperlight | MIT, Hyperlight | MIT, Hyperlight |

### 2.2 Raft cores

**focal-raft** (primary: `focal crates/focal-raft/src/lib.rs:13-27`). A state machine with no
clock, disk or network: `tick`/`step` in, one `Ready` out, `advance_append` and
`advance_apply_to` back (`node.rs:418, 488, 532`). Pre-vote, check-quorum, priority with
`Precedence::Log`, learners, joint consensus (`ConfChangeV2`), transfer, an inflight window with
conflict hints, ReadIndex (quorum, no lease), snapshots, and the fast track (`fast.rs`,
`track.rs`). Every queue has a bound in `Limits` (`raft.rs:33-55`). Errors are of three kinds:
a refusal that changed nothing, a contradicting peer message, or a state that no longer adds up,
which alone stops the replica (focal 27 §4.5). It keeps raft-rs's wire and log types through
`raft-proto` (`proto.rs:7-13`), pinned to an unmerged upstream revision with a protoc build
(note 07 §1.5). Tests: `tests/differential.rs` against raft-rs step for step (80.8 M steps
equal, note 07 §1.7), `tests/fast.rs`, `tests/group.rs`, `src/tests.rs`; benches `replicate`,
`allocs`. Term exhaustion is refused (`raft.rs:1187`). The `Storage` trait carries focal's
allocation audit: "The page is chosen before it is copied" (`storage.rs:18-35`).

**slates' hecate core** (primary: `slates crates/cluster/src/raft.rs:1-86`). Sans-io; its own
message structs (`RequestVote`, `AppendEntries`, `FastPropose`, `FastVote`, `TimeoutNow`,
`InstallSnapshot`) keyed by `HostId` from `slates_db::register`; pre-vote, check-quorum,
ReadIndex, joint consensus integrated in the log (`begin_membership_change`, `raft.rs:3045`),
compaction and install-snapshot, bounded appends with conflict hints, learners with catch-up
rounds (`catch_up`, `raft.rs:2669`), priority from measured quorum round trip
(`election_rank`, `priority_transfer`, `raft.rs:2784, 2807`), transfer (`raft.rs:2885`), a
fast track over a window of slots (`on_fast_vote`, `raft.rs:2116`; `fast_quorum`,
`raft.rs:363`), pipelining (`replicate_to`, `raft.rs:2228`) and out-of-order acknowledgement
within a term under a synced term (`synced_term`, `raft.rs:634, 1424, 2482`). Term exhaustion is
typed (`TermExhausted`, `raft.rs:723`). Tests: `tests/raft.rs` (conformance: Election Safety, Log
Matching, Leader Completeness, State Machine Safety), `tests/explore.rs` (400 seeds × 4,000
steps × 3 and 5 voters at full scale, slates consensus-enhancements build ledger slice 1),
`tests/prevote.rs`, `tests/priority.rs`, `tests/wan_election.rs`, `tests/pipelining.rs`,
`tests/fast_track.rs`, `tests/slot_model.rs`, `tests/prefix_model.rs`, `tests/multilog*.rs`,
plus real-process and KIND lanes in `slates-server` and `slates-cli`.

**Where they diverge.** focal 27 §2 and §8.2 describe slates' core as lacking transfer, priority,
fast track and pipelining; that was true when written and is stale: all four are built
(`raft.rs` lines above; slates BENCHMARKS "Consensus: leadership transfer", "Priority elections
across published inter-region round trips", "Pipelined replication across five regions"). The
two cores now overlap in nearly every feature and differ in their wire (raft-rs protobuf against
slates' fixed-layout codec), their fast-track algorithm (§2.4), their priority input (placement
rank against measured round trip) and their evidence (a differential against a deployed library
against exhaustive models and timed simulation on published WAN matrices).

### 2.3 Durable shells and logs

**focal: `DurableNode` over `SharedWal`** (primary: `focal crates/focal-consensus/src/lib.rs:13-20`,
`persistence.rs:1-31`). "Only `drain` emits committed entries, after Ready and LightReady commit
metadata are durable." One outstanding Ready per group; a nonblocking drain state machine
(`Phase::{Start, Ready, Light}`) holding a prepared update and a WAL receipt; staging reserved
before the Ready is taken; cross-group batching proven by twelve groups staged behind a barrier
(audit §11.3). It holds every message, a leader's appends included, until the fsync (note 07
§2.3). It adds memory and disk budgets (`focal_memory`), decoder fences for format evolution
(`decoder.rs`), restore images, and the unwind boundary `guarded_in` (`lib.rs:33`,
`catch_unwind`). focal-log (`crates/focal-log/src/lib.rs:13-29`): segments of CRC-chained postcard
records, a checksummed `CURRENT` fence file installed by rename, `File::sync_all` plus directory
sync; since `a8e95f7` (the audit's F14) a group's checkpoint writes only what the group keeps and
a moving base retires the rest, which closes note 07 §3.5's whole-WAL rewrite. One writer thread
per node (`writer.rs:807`), a `tokio::sync::oneshot` for async receipts (`writer.rs:16, 200`),
`SharedWal(Arc<Handle>)` (`writer.rs:91`).

**mantle: `Replica` over `mantle_log::Log`** (primary: `crates/range/src/replica.rs:915-1010`,
`crates/log/src/lib.rs:1-8`). `begin` takes the core's Ready, gives out at once the messages a
leader may send before its own write, submits the update and applies what is already committed,
then returns with `persisting` set; `drive` finishes once the update is durable
(`replica.rs:920-945`). Messages and ticks arriving meanwhile are held, bounded by one
flow-control window of bytes and `2 · election_tick` ticks (`replica.rs:113-124`). A Ready the log
refuses for room waits whole (`stalled`, audit S04). A member whose log may lack what it
acknowledged is marked uncertain, does not campaign, and asks the leader for a snapshot reaching
its mark, as in protocol-aware recovery (`replica.rs:690-735`). Commit is not fenced separately:
committed apply may run before the LightReady commit is persisted, and restart repairs the WAL
commit from the engine's durable state (`replica.md` §3; audit §11.3). The log writes one frame
per flush through aligned `BlockFile` I/O (`crates/disk/src/block.rs:8-27`), answers a submission
only once a later durable record confirms the frame, restores a lost last frame from its persist
record, marks damaged groups and serves them to no one (`lib.rs:126-154`), and orders a full frame
by `Class` with a fair queue (`lib.rs:117-124`). Tests: `crates/log/tests/log.rs` (151 test items,
including `every_acknowledged_update_survives_power_loss`), `fairness.rs`,
`crates/range/tests/group.rs`, `sim.rs` (crashes, device faults, bit flips at rest, member
replacement, linearizability per key), `linear.rs`.

**slates: `SavedRaft`** (primary: `slates crates/cluster/src/raft.rs:604-635`). The complete
retained state — term, vote, the whole log above the snapshot, commit, the snapshot, the window
of fast slots and the synced term — published as one value and validated on recovery
(`RaftRecoveryError`). The control shard re-encodes and checksums it before every consensus reply
(slates BENCHMARKS "Consensus log compaction and bounded replication"); compaction by the thesis's
size rule keeps it bounded by about three times the configuration plus the uncommitted tail
(RECORDED: 4,000 changes, 90,083 → 45,607 bytes, 41× less time per change).

**INFERENCE.** These are three answers to one question — when may a group's output leave — with
different durability media. A shared shell must be generic over the medium (disk log, RAM
publication) and must state its release rules as invariants, not as modes: leader appends may go
before the leader's own write (the core permits it, focal `node.rs:5-8`; mantle does it), a
follower's acknowledgement waits for its write, and a client answer waits for whichever durable
state covers it (§3.8).

### 2.4 The fast track: two algorithms

| | focal (`focal-raft/src/fast.rs`, `track.rs`; focal 27 §4.6) | slates (`raft.rs`; consensus-enhancements §3.7, §4) |
|---|---|---|
| What a member holds | at most one self-approved entry per index, beside the log, never in it | a window of slots above the log, each an accepted value |
| Fast quorum | ⌈3M/4⌉ | `fast_quorum`, the smallest `f` with `2f + q > 2n` (equal to ⌈3n/4⌉, asserted by `slot_model.rs`) |
| Leader's decision | takes the first entry it hears of for its next index and restamps it with its own term | fills a stalled index after two round trips; separates its fast application frontier from the commit index followers keep |
| When held state goes | when a leader-log entry takes its index | only under a classic commit (the corrected design, slates bug 2026-09-29-window-slots-dropped-before-a-classic-commit-lost-chosen-values) |
| Configuration | fast commit only under an applied, non-joint configuration | — (UNVERIFIED for joint configurations) |
| Durable | proposals on disk before a vote says they are held (`RecordKind::Proposal`); group flag `RecordKind::FastTrack` fixed at creation | slots retained in `SavedRaft.window` |
| Evidence | TLA+ `docs/models/FastTrack.tla` checked by TLC in CI (no reconfiguration, no liveness, note 07 §10.6); `tests/fast.rs`; latency against classic 0–10 % loss | exhaustive Rust searches (slot model up to 23.6 M classes; corrected prefix model up to 188 M by hand); counterexamples kept; five-region crossover |
| Measured crossover | LAN/three voters/5 % loss mean 17.9 vs 14.6 ms; regional/three/1 % p99 411.2 vs 366.1 ms (audit §11.5) | far proposer 435 → 275 ms median; leader-local proposer 156 → 201 ms; 10 % loss worse everywhere (slates BENCHMARKS "The fast track's crossover across five regions") |
| In service | no owner takes it (focal 27 §8.4) | both groups propose from their leaders and keep classic |

The audit's warning stands: "Do not transplant Slates' pruning rule into Focal in isolation"
(audit §11.5). The two are different algorithms with different recovery rules; a shared core
carries one (§3.8).

### 2.5 Timing laws and estimators

Both siblings derive election timing from Ongaro and Ousterhout §5.6 with `ELECTION_MARGIN = 10`
(`slates timing.rs:52`; focal-timing `lib.rs:1-21`). slates sets base `= 10 × max(tail,
heartbeat)` and span `= 10 × max(spread, heartbeat)` from an RFC 9002 estimator whose tail is
`smoothed + 4·rttvar` (`timing.rs:176-201`), derives a round's collection budget from the same
tail (`round_budget`), and the pipelining window `⌈2 × tail / heartbeat⌉` batches (slates
BENCHMARKS "Pipelined replication"). focal stretches the tick period instead of the tick count
(`TickPace::derive`), and measures each path "as the median of the latest sixteen and their
median absolute deviation, so that an answer that came late does not set a group's election
timeout" (focal 27 §3.1 P2) — while its module header still calls `PathRtt` an RFC 9002
estimator (`focal-timing/src/lib.rs:9-12`; note 07 §10.9 noted the drift). Both count timers in
owner periods so a starved node waits longer instead of campaigning. focal adds
`ProgressDeadline`, `RoundBudget`, `DeadlineExtender`, `RoundWait` (`progress.rs:36`,
`round.rs:30-194`). mantle's audit adds an obligation neither sibling has: the tail must include
the durable-acknowledgement time, not only the network round trip (audit §11.7).

### 2.6 Simulation, model checking and history checks

| Kind | mantle | focal | slates |
|---|---|---|---|
| Network | message-level delay, drop, duplicate, partition (`range/tests/sim.rs:1-27`) | `focal_sim::path`: Gilbert–Elliott loss, bottleneck with drop-tail queue, MTU, NAT expiry, all in PPM (`focal-sim/src/path.rs:1-25`) | `rt/src/sim.rs` seeded fabric with interface MTU refusal; `support/timed.rs` per-pair latency on Microsoft's published Azure P50 matrix |
| Disk | `mantle_disk::sim::SimFile`: crash losing unsynced data, write and flush faults, bit flips at rest | `focal_sim::disk` | none (RAM) |
| Real code under virtual time | the replica, log and engine | DurableNode, focal-wire endpoints (`tests/congestion.rs`) | real `RaftNode`, real timers, real transport endpoints |
| Exhaustive | — | TLC on `FastTrack.tla` | parallel breadth-first with symmetry reduction and fingerprints, memory ceiling from measured ratio (`support/exhaustive.rs`) |
| Differential | — | focal-raft vs raft-rs | N=1 vs simulated fleet (rule R8) |
| History | WGL + Lowe linearizability per key | `focal_sim::history` (domain-coupled to `focal_model`) | Election Safety, Log Matching, Leader Completeness, State Machine Safety after every step |

slates' rules forbid TLA+ tooling in its CI and on the owner's machine (slates `CLAUDE.md` §2
item 13); focal runs TLC in CI (`focal scripts/check-model.sh`). That is a direct conflict for
shared test infrastructure (§6).

### 2.7 QUIC transports

**focal-wire** (primary: `focal crates/focal-wire/src/lib.rs:13-15`, `transport.rs:40-235`).
quinn over tokio with rustls on AWS-LC; mutual TLS against a cluster CA, authorization by
certificate fingerprint re-checked per stream; one pooled connection per peer; Copa through
quinn's `ControllerFactory` (`congestion.rs:1-60`); classes by `TrafficClass`; lanes derived from
the consensus window (`WireLimits::for_consensus`); content striped over as many streams as the
window holds (`bulk_width`, `transport.rs:91`); progress-charged waits (`carried`,
`transport.rs:164`, an `async fn`); admission by identity (`admission.rs`); `gather` fan-out
(`round.rs`). The crate is coupled to focal's domain (`focal-model`, `focal-stream`) and states
that "Async transport entry points require a Tokio runtime" (`lib.rs:14`).

**slates' transport** (primary: `slates crates/transport/src/lib.rs:1-34`). Two planes over one
UDP substrate: a sealed control plane (codec only, unwired) and a session plane, "slates's owned
RFC 9000/9002-shaped QUIC dialect", sans-io (`connection.rs`) pumped by `endpoint.rs` on slates'
runtime. Fixed-layout little-endian frames, not QUIC varints, so not wire-compatible with RFC 9000
(note 08 §6). Packet and header protection, Handshake level sealed, 1-RTT key update
(`keys.rs`), absolute credits with a class reserve (`flow.rs`, `connection.rs:77`), credited
stream concurrency, ACK ranges and ACK-of-ACK, PTO, loss by packet and time thresholds, pacing,
Copa, DPLPMTUD, adaptive reordering. IPv4 only (note 08 §6). Unsafe budget zero; denies
`indexing_slicing`, `string_slice`, `panic_in_result_fn`, `unwrap_in_result` (`lib.rs:36-47`).
The one `Arc` in slates is rustls's config, which `rustls::quic` takes by signature
(`handshake.rs:9-12`).

### 2.8 Datagram planes

Only slates has code. A control datagram is a fixed-layout cleartext prologue (version, sender,
key epoch, sealed length) bound as AAD to an AES-256-GCM sealed region; nonces are `counter ‖
channel`, never random; the sealer refuses past `u64::MAX`; the opener accepts only strictly
increasing counters and advances its high-water only after the tag verifies
(`seal.rs:1-35`, high-water at `seal.rs:192`). Keys per `(sender, key_epoch, direction)` come
from HKDF-Expand-Label over an injected control secret (`schedule.rs:1-25`); the acceptance order
is length, prologue, keyring lookup (an unknown sender dies before any cryptography), AEAD,
envelope, replay (`accept.rs:1-14`). Fencing by epoch is "owed" (`accept.rs:10-13`); the
secret-distribution half of enrollment is owed (`lib.rs:23-26`); a reorder-tolerant replay window
is owed (`seal.rs:28-32`). `Enrollment::sealer` builds a counter-zero sealer
(`enrollment.rs:60`), the nonce-reuse hazard audit §11.8 names. mantle's design (`node.md` §3.4)
keys each epoch from the TLS exporter of the QUIC connection to the same peer, uses an RFC 4303
window widened to the measured reordering, packs one datagram per peer per heartbeat, and keeps
within RFC 8085.

### 2.9 SWIM and membership

**slates** (primary: `slates crates/cluster/src/detector.rs:1-30`, `swim.rs:1-40`). A sans-io
detector driven by `tick` (`detector.rs:388`) and messages: randomized probe order per round,
indirect ping-req through `k` peers chosen nearest the target by Vivaldi coordinates,
infection-style gossip piggybacked and bounded, the Lifeguard local-health multiplier
(`health_multiplier`, `detector.rs:234`), and the confirmation-count suspicion timeout `max −
(max−min)·log(C+1)/log(K+1)` in deterministic fixed point (`fixed.rs`). The codec rejects every
malformed field before allocating; floats travel as bit patterns. Members carry a per-start nonce
from which the member id derives with the certificate anchor (`swim.rs:46-48`). Tests:
`tests/swim.rs`, plus real-fleet tests.

**focal** (primary: `focal crates/focal-node/src/liveness.rs:1-3`, `liveness/driver.rs:1-60`).
SWIM with Lifeguard, "under-load deadline extensions" (an accused host asks for time with a
progress witness it cannot fake while stuck; an overloaded host is healed, never extended,
`liveness/wire.rs:21-36`), Vivaldi coordinates, a probe timeout `clamp(base, factor·rtt_ucb,
cap) × health`. Verdicts are committed by the partition owner. It is one tokio task per node
(`driver.rs:1-5`), uses domain types (`focal_model`, `driver.rs:17`) and fixed constants:
`MAX_MEMBERS 1024`, `MAX_INFLIGHT 16`, `MAX_EVENTS 32`, `INBOX_DEPTH 64`, `MAX_CONFIRMATIONS 64`,
`DIRECT_ANSWER 750 ms` (`driver.rs:36-49`). Tests: `swim_tests.rs`, `algorithm_tests.rs`.

**mantle** has the design only (`node.md` §3.5): SWIM within a cell on the datagram plane, period
at least three round-trip estimates, Lifeguard's remedy of stretching one's own timeouts when
slow, measured directly as the delay between a probe's arrival and its handling rather than by
Lifeguard's counter, whose limits "currently use heuristically determined values" (25 §8).

### 2.10 Quality against the rules

The rules: no panics; every resource bounded; no arbitrary constants; no `Arc` shortcuts; no
thread per unit of concurrency; portable; real and simulated tests. Findings that matter for
sharing (primary unless marked):

| Where | Finding | Rule |
|---|---|---|
| mantle `crates/log/src/lib.rs:29-31, 282-305, 327-331` | `Arc<Shared>`, `RwLock<State>`, `Mutex<Queue>`, `Condvar` | no-Arc (owner memory), note 26 §5 |
| mantle `crates/log/src/lib.rs:323`, `writer.rs:404` | `room.notify_all()` on a shared condition variable | note 26 §5 names broadcast wakes on a shared condvar as the hazard behind the recorded panic |
| mantle `crates/log/src/lib.rs:156-174` | `Pending` answers through `std::sync::mpsc`, which wakes nothing | `node.md` §1.3 already plans a `Waker` |
| mantle `crates/log/src/lib.rs:85-96` | entry and proposal bytes as `Arc<[u8]>` | no-Arc |
| mantle `crates/range/src/replica.rs:290, 400`, `store.rs:13` | `Arc<Log<F>>` shared by every replica on a device | no-Arc |
| mantle `crates/range/src/replica.rs:98-101` | `Replica` generic over `mantle_meta::Engine` and `Layer` | domain coupling: blocks sharing as is |
| mantle `crates/range/src/replica.rs:52-56` | `max_inflight_msgs` counts messages | audit §11.4: bound bytes |
| focal `focal-raft/src/raft.rs:56-69` | `Limits::default()` literals (65,536 messages, 4,096 reads, 16,384 entries per message, 256 proposals, `8 MiB − 64 KiB`, window 256, 64 MiB) | no arbitrary constants |
| focal `focal-raft/Cargo.toml:9`, workspace `Cargo.toml:23` | `raft-proto` git revision of an unmerged PR, protoc build | a C++ build tool; slates bans non-Rust tooling |
| focal `focal-consensus/src/lib.rs:67, 202` | `COMMITTED_PAGE_BYTES` 16 MiB, `DEFAULT_INFLIGHT_WINDOW` 128 | no arbitrary constants; focal 27 §8.4 says the window "derived from the path will follow" |
| focal `focal-log/src/writer.rs:16, 91` | `tokio::sync::oneshot` in the log; `SharedWal(Arc<Handle>)` | runtime coupling; no-Arc |
| focal `focal-wire/src/transport.rs:48, 62, 68` | `IDLE_TIMEOUT` 10 s, `STREAM_WINDOW_CEILING` 1 MiB literal, a 1 Gbit/s × 100 ms reference path | note 30 §6.1 rejected or adapted each |
| focal `focal-wire/src/lib.rs:14` | tokio required | slates bans tokio |
| focal `liveness/driver.rs:36-49` | fixed member, inflight, inbox, event, confirmation bounds and a 750 ms answer wait | no arbitrary constants |
| slates `transport/src/conn.rs:215-222` | bytes in flight count stream payload only | RFC 9002 §B.2 |
| slates `transport/src/connection.rs:1512` | PTO held under `max(PTO, initial PTO)` | RFC 9002 §6.2.1 |
| slates `transport/src/endpoint.rs` handshake retransmit (note 30 §6.1: lines 873, 920, 1445-1455 at `fdaa51d`) | first handshake retransmit at 1 ms | RFC 9002 §6.2.2 |
| slates `transport/src/streams.rs` | kind and class in the stream ID | audit §13.3 |
| slates `transport/src/connection.rs:497` | `open_exchange` copies the whole request; replies accumulate whole | audit §11.8 |
| slates `transport/src/enrollment.rs:60`, `seal.rs:132` | counter-zero sealer per key | nonce reuse after restart under an unchanged key (audit §11.8) |
| slates `Cargo.toml:85` | rustls `tls12` feature on | TLS 1.3 only (`node.md` §3.6; focal `transport.rs:236-290`) |
| slates `cluster/src/raft.rs:604-635` | the whole retained log re-encoded and checksummed before every consensus reply | bounded by compaction, but O(retained) per reply |
| quinn-proto 0.11.18 `src/` | about 550 textual `unwrap`/`expect`/`panic!`/`unreachable!`/`assert!` sites, 59 of them `debug_assert` (DERIVED: a grep of non-test files, in-file test modules included) | no panics: a dependency that can panic is called behind an unwind boundary, and its causes closed |
| quinn-proto `src/config/mod.rs:38-199` | `Arc<TransportConfig>`, `Arc<dyn HmacKey>`, `Arc<dyn ...>` by signature | slates D-8 exception 2 (a foreign API that takes `Arc`) |

What is strong and should travel unchanged: focal-raft's typed errors and bounded queues and its
differential; focal-consensus's reservations-before-transition and receipts; mantle's
power-loss, damage and repair tests; slates' sans-io discipline everywhere, its exhaustive
models, its bug corpus with a failing test first, and its measured-and-rejected records.

### 2.11 Ledger: transport (QUIC and application layer)

"Lands as": **quinn** = already in quinn-proto, kept and pinned by a regression test from the
sibling's record; **patch** = a patch to the vendored quinn-proto (§3.3); **app** = the
application layer (§3.4); **excluded** = with its reason.

| # | Mechanism or fix | Where, evidence | Lands as |
|---|---|---|---|
| T1 | Packet and header protection; Handshake level sealed | slates `flight.rs`, `keys.rs`; bug 2026-09-30-the-handshake-sent-its-certificates-in-plaintext | quinn; test that no certificate byte crosses in clear |
| T2 | 1-RTT key update, at most three key sets, integrity budget across keys | slates `keys.rs`; bug 2026-09-30-session-keys-never-updated-or-counted | quinn; test that an update begins before RFC 9001 §6.6's 2^23 packets; patch if it does not (note 30 §6.1) |
| T3 | Packet-number ceiling 2^62, no saturation into a repeat | slates `packet_number.rs`; bug 2026-09-30-packet-numbers-saturated-into-a-repeat | quinn; test |
| T4 | Exchange sequence numbers that cannot wrap into an acknowledged one | slates bug 2026-09-30-request-sequences-wrapped-into-acknowledged | app: `u64` exchange ids with typed exhaustion |
| T5 | Handshake flights fragmented to the path floor | slates `flight.rs:1-50` | quinn |
| T6 | A handshake retry that keeps its flight | slates bug 2026-09-14-handshake-retry-forgets-its-flight | quinn; test |
| T7 | A server's handshake flight independent of its roster (no root hints in CertificateRequest) | slates bug 2026-09-14-servers-handshake-flight-grows-with-its-roster | app: the rustls verifier wrapper |
| T8 | First handshake retransmit at 1 ms | slates `endpoint.rs` (note 30 §6.1) | **excluded**: RFC 9002 §6.2.2 starts at `2 × kInitialRtt` |
| T9 | Connection IDs from the exporter, one socket per plane, a re-dial replaces the session of the same certificate, per-certificate quotas | slates `demux.rs:1-40`; focal `admission.rs` (P5: a pending-handshake reservation apart from authenticated slots; the idle-longest of an identity replaced) | quinn for IDs; app for replacement and admission, bounds from the node budget (focal's 4 and 16 per identity excluded as fixed) |
| T10 | Redial bursts admitted without assuming one session per peer | slates bug 2026-09-16-redial-burst-assumes-a-per-peer-session-limit | app: admission; test |
| T11 | Fairness among authenticated sessions | slates bug 2026-09-17-authenticated-session-fairness (UNVERIFIED beyond the title) | app: admission; test |
| T12 | Stream concurrency enforced at the sender; data past the limit never acknowledged | slates `streams.rs:12-38`; bug 2026-09-28-past-the-stream-limit-data-was-dropped-but-acknowledged | quinn (RFC 9000 §4.6); test |
| T13 | No stream-ID reuse behind an unacknowledged reply; abandoned requests and content streams poison nothing | slates bugs 2026-09-13-reused-stream-id-collides-behind-an-unacked-reply, 2026-09-10-abandoned-request-retransmit-lockstep, 2026-09-10-abandoned-content-stream-poisons-reused-session | quinn (IDs never reused); tests |
| T14 | Class carried in the stream ID | slates `streams.rs:5-10, 43-55` | **excluded**: a peer would choose its class (audit §13.3); class from message kind and sender role |
| T15 | Strict priority among classes | slates BENCHMARKS scheduler grid (control p99 1.023× best; round robin 4.8× worse tail); focal 27 §7 classes table (1 Mbit/s 100 ms: 260.3 → 170.9 ms beside eight transfers) | app over quinn stream priorities |
| T16 | A connection credit reserve for the classes above; stream admission by class | slates `flow.rs:8-13`, `connection.rs:77`; bugs 2026-09-30-bulk-spent-the-connection-credit-a-control-exchange-needed (68 ms wait on a 40 ms path), 2026-09-30-bulk-exchanges-held-the-stream-credit-a-control-exchange-needed | app: node accounting on top of quinn's one window |
| T17 | Absolute credits kept a window ahead of consumption, autotuned when consumed within two round trips, ceiling from memory | slates `flow.rs:1-45` | app via `set_receive_window`; ceiling from the budget and T33 |
| T18 | Credit that elicits no acknowledgement, so an idle peer's delayed ack is not an RTT sample | slates bug 2026-09-28-idle-peer-acks-inflated-the-rtt (thin-link p99 about 7 s before) | test against quinn's ack-delay handling; patch if the inflation reproduces |
| T19 | A credit-blocked sender with nothing in flight reports it; a lone path probe arms no PTO | slates bug 2026-09-28-a-lone-path-probe-silenced-the-blocked-report | test; patch if quinn shows it |
| T20 | A probe copies the oldest packet and leaves the original in flight | slates bug 2026-09-27-session-plane-probes-hid-tail-losses | quinn (RFC 9002 §6.2.4); test |
| T21 | Probe copies bounded for a silent peer | slates bug 2026-09-28-silent-peer-probe-copies-grew-without-bound | test; patch if quinn's retained probes grow |
| T22 | ACK ranges bounded by a frame budget; received set bounded by ACK-of-ACK and a cap | slates `conn.rs:36-130` (353 entries against a bound of 40 before) | quinn; test under a pure receiver (note 30 §6.1) |
| T23 | RFC 9002 RTT estimator and PTO | slates `rtt.rs:61-120` | quinn |
| T24 | One clock for send stamps and timers | slates bug 2026-09-14-transport-rtt-sampled-on-the-wall-clock | quinn-proto takes `now` everywhere; each adapter passes its runtime's clock only; a simulation test |
| T25 | PTO backoff capped at the initial PTO | slates `connection.rs:1512` | **excluded**: RFC 9002 §6.2.1 doubles without a cap; the idle timeout bounds probing |
| T26 | Loss by packet and time thresholds; persistent congestion | slates `conn.rs:364-414` | quinn |
| T27 | Adaptive reordering tolerance, RACK-style, bounded memory | slates `reorder.rs:1-20`; BENCHMARKS "adaptive reordering" (capacity share 0.254 → 0.540, neutral on 1,120 + 260 runs) | patch (quinn has fixed `packet_threshold`/`time_threshold`, `config/transport.rs:164-171`) |
| T28 | Copa δ = ½, integer, RFC 9002 §7.8 bounding only growth, loss as persistent congestion only, competitive mode | slates `copa.rs`; bug 2026-09-28-copa-froze-an-overshot-window; slates bake-off (goodput shortfall 1.068 vs 14.0–15.8 for CUBIC/NewReno) | app via `ControllerFactory`, focal's port |
| T29 | Copa's slow start judged by what was sent after the last doubling; a round trip moves the window by at most half (`DEFAULT_STRIDE`) | focal `congestion.rs:20-60`; focal 27 §7 (10 Mbit/s 300 ms: 69 % → 96.9 %; stride table) | app; slates' law takes both |
| T30 | Pacing quantum 1 ms of the rate, floor two datagrams, cap 64 KiB, refill to one quantum after idle | slates `pacer.rs`, `congestion/mod.rs:119-132`; quinn `pacing.rs:145-151` uses 2 ms, floor 10, cap 256 datagrams | patch |
| T31 | Pacing at Copa's rate (2·cwnd/RTTstanding) | focal `congestion.rs:26-29`: "the window is Copa's and the pacing quinn's" | patch: let the controller supply its pacing rate |
| T32 | Bytes in flight counting stream payload only | slates `conn.rs:215-222` | **excluded**: RFC 9002 §B.2; quinn counts the packet (audit §11.8) |
| T33 | quinn's 1,024-span assembler limit and the per-stream window it bounds | quinn `assembler.rs:361`; focal `transport.rs:49-62`, focal 27 §7 (connections closed at 100 Mbit/s before) | patch to expose the limit; app derives the ceiling from it instead of a literal |
| T34 | Packets never past the datagram floor before the path is proven | slates bug 2026-09-28-packets-grew-past-the-datagram-floor | quinn; test |
| T35 | DPLPMTUD: a raise rechecks only the last failed size (≤ 3 probes); a refused probe (`EMSGSIZE`) returns its packet number; probes give no RTT sample; a PTO with above-floor packets in flight counts as black-hole evidence; black hole after three such losses | slates `pmtud.rs:1-44`; BENCHMARKS "Session-plane path MTU discovery" (loopback 1,625 → 7,840 Mbit/s; neutral on floor paths); bug 2026-09-28-a-shrunken-path-deadlocked-before-its-black-hole-was-seen | patch where quinn's `mtud` differs, each verified by the grid |
| T36 | Several streams per transfer, as many as the window holds plus one | focal `transport.rs:91`, `peers.rs:344`; focal 27 §7 (1 Gbit/s 100 ms: one stream 7.4 %, by window 81.1 %) | app |
| T37 | Per-peer lanes derived from the core's window; a message that finds its lane full waits its turn within its exchange's time instead of being dropped; a peer the pool cannot reach is reported to the core | focal `ad8232a`, focal 27 §7 | app; window from the path (focal's 128 excluded as fixed) |
| T38 | Raft acknowledgements admitted beside participants' traffic | focal `653616e` (the audit's F56) | app: replication and control classes never wait behind request admission |
| T39 | Waits charged to progress: an exchange ends when a period moves less than a datagram, not at a fixed time | focal `transport.rs:150-197`; focal 27 §7 ("a megabyte crosses a path of 4 Mbit/s ... between endpoints that give a request one second") | app, as a sans-io deadline object (focal's is `async`) |
| T40 | Exchange-time estimator doubling per abandoned exchange | focal `peers.rs:229-245` | app; the cap of six excluded, bound by the caller's budget (note 30 §6.1) |
| T41 | Equal-jitter pauses; callers that failed together do not return together | focal `peers.rs:1197-1212`; `d0ca3a9` (F64) | app |
| T42 | Cold calls to one route dial once and share the connection | focal `a15db07` (F60) | app: single-flight dial |
| T43 | A retired route closes its connection; a pending request ends when its peer's identity is replaced | focal 27 §3.1 P6 | app: owner-serialized, no lock |
| T44 | A session is never lent exclusively: a campaign or forward waits for a session that is out, within its round's deadline | slates bugs 2026-09-25-a-forward-refused-while-the-owners-session-was-out, 2026-09-26-a-session-hold-raced-its-own-link, 2026-09-29-a-campaign-asked-no-one-while-a-session-was-out (KIND median 2.77 → 1.91 s) | app: connections are owned by the endpoint; callers hold handles to exchanges, never the connection |
| T45 | Progress-aware fan-out: a round ends when every peer reported or nothing arrived in a stall window, extends while a quorum fills, folds late replies | slates `progress.rs`, `broadcast`; focal `round.rs`, `focal_timing::RoundBudget`; slates bugs 2026-09-12-broadcast-waits-out-dead-voter, 2026-09-12-fleet-consensus-hard-budget-under-load | `x-timing` + app |
| T46 | Pool defaults: 5 s, 10 ms retry, 2 s cooldown, two exchanges per peer | focal `peers.rs:30-62` | **excluded** (note 30 §6.1) |
| T47 | Idle timeout 10 s, keep-alive idle/4 | focal `transport.rs:48, 223-225` | **excluded**: idle ≥ 3 PTO (RFC 9000 §10.1), keep-alive from the measured NAT lifetime |
| T48 | Windows from a 1 Gbit/s × 100 ms reference path; 144 MiB per connection | focal `transport.rs:63-75, 197`; audit §11.8 | **excluded**: windows from each path's BDP and the node budget (`node.md` §3.3) |
| T49 | 0-RTT off both sides; TLS 1.3 only; mutual TLS; per-stream re-authorization | focal `transport.rs:236-290`; `node.md` §3.2, §3.6 | app; slates' `tls12` feature **excluded** |
| T50 | A frame's body allocated before its handler is admitted | focal `frame.rs:59` (audit §11.8); fixed in focal `02b2002` ("a body is permitted before it is allocated") | **excluded** as written; app reads a fixed header, then reserves, then reads the body |
| T51 | Whole-exchange retention | slates `connection.rs:497-520` | **excluded**; bodies stream through reservations |
| T52 | Migration and path validation; GSO/GRO and ECN; IPv6 | absent in slates (note 30 §6.1; note 08 §6) | quinn and quinn-udp; new to slates |
| T53 | TCP fallback carrying the same application protocol | `node.md` §3 | app |
| T54 | Congestion and scheduling bake-offs with selection rules fixed before any run | slates `examples/congestion_bench.rs`, `class_latency_bench.rs`, `path_mtu_bench.rs`; focal `tests/congestion.rs` | `x-sim` harness; both grids run against every patch (§5) |

### 2.12 Ledger: datagram plane and SWIM

| # | Mechanism or fix | Where, evidence | Lands as |
|---|---|---|---|
| D1 | Fixed-layout cleartext prologue bound as AAD | slates `lib.rs:6-17`, `seal.rs:1-6` | `x-datagram` |
| D2 | AES-256-GCM with counter nonces `counter ‖ channel`, refusal past `u64::MAX` | slates `seal.rs:8-15` | `x-datagram`, through AWS-LC instead of RustCrypto `aes-gcm` |
| D3 | HKDF-Expand-Label per `(sender, epoch, direction)` | slates `schedule.rs:1-25` | `x-datagram`, over the QUIC connection's exporter secret (`node.md` §3.4) instead of an enrolled control secret |
| D4 | Acceptance order: length, prologue, keyring lookup before crypto, AEAD, envelope, replay | slates `accept.rs:1-14` | `x-datagram`, with fencing by epoch and term added after the AEAD (owed in slates) |
| D5 | High-water advanced only after the tag verifies | slates `seal.rs:30-32` | `x-datagram`, kept inside an RFC 4303 window whose width follows the measured reordering |
| D6 | Counter-zero sealer per key | slates `enrollment.rs:60` | **excluded**: nonce reuse after a restart under an unchanged key (audit §11.8); exporter keys are new per connection epoch |
| D7 | Golden vector for a sealed datagram | slates `a_sealed_datagram_matches_its_golden_vector` | kept: AES-GCM is deterministic for a key, nonce and AAD, so the AWS-LC build must reproduce slates' vector byte for byte |
| D8 | Hostile-input and plane tests | slates `tests/hostile.rs`, `tests/plane.rs` | kept |
| D9 | One packed datagram per peer per heartbeat; size ≤ quinn's `max_datagram_size` for the peer, else 1,280 (IPv6) / 576 (IPv4); never more than RFC 8085's one datagram per RTT originated when sending few; nothing retransmitted | `node.md` §3.4 | `x-datagram` |
| D10 | A socket of its own, outside QUIC's congestion controller | `node.md` §3.4; 25 §6 | `x-datagram` |
| S1 | Randomized probe order per round | slates `detector.rs:20-22` | `x-swim` |
| S2 | Indirect probes through `k` relays nearest the target; wired, not just built | slates `detector.rs:24-30`; bug 2026-09-18-swim-indirect-probes-not-wired | `x-swim` |
| S3 | Infection-style gossip, piggybacked and bounded | slates `detector.rs:10-12`, `swim.rs:1-6`; focal `liveness/gossip.rs` | `x-swim` |
| S4 | Lifeguard local-health multiplier | slates `detector.rs:234`; focal `liveness/health.rs` | `x-swim`, beside mantle's measured self-lag (open, §6) |
| S5 | Confirmation-count suspicion timeout in deterministic fixed point | slates `fixed.rs`, `detector.rs:15-19`; focal `liveness/suspicion.rs` | `x-swim`, slates' fixed-point form (bit-identical across hosts) |
| S6 | Acknowledgements correlated to their ping by nonce | slates bug 2026-09-10-swim-stale-ack; focal "probe nonces" (focal 27 §3.3) | `x-swim` |
| S7 | Probe deadline from the measured path, never fixed | slates bug 2026-09-13-swim-fixed-probe-deadline-kills-a-starved-live-peer; focal `clamp(base, factor·rtt_ucb, cap) × health` | `x-swim` from the path estimator; focal's `base`, `cap` and `DIRECT_ANSWER` **excluded** as fixed |
| S8 | Detection windows counted in periods, not a fixed scheduler quantum | slates bug 2026-09-16-fleet-detection-windows-use-a-fixed-scheduler-quantum (UNVERIFIED beyond the title) | `x-swim` |
| S9 | One clock origin for every heartbeat | slates bug 2026-09-17-heartbeats-use-different-clock-origins (UNVERIFIED beyond the title) | `x-swim` reads only the driver's clock |
| S10 | Gossip reaches members outside a sparse neighbourhood | slates bug 2026-09-17-sparse-mesh-drops-membership-gossip (UNVERIFIED beyond the title) | `x-swim`; test |
| S11 | Direct contact kept with a seed whose real id is not yet learned (`UnlearnedSeed`) | slates bug 2026-09-28-a-gossiped-seed-death-stranded-an-unreached-peer | `x-swim` |
| S12 | A death retires a seat only once held for one election window; incumbents keep seats | slates bugs 2026-09-17-council-retires-a-suspected-voter, 2026-09-22-council-seats-follow-id-order-not-liveness; focal 27 §3.1 P4, §5 "Seats" | `x-swim` reports how long a verdict has stood; the seat rule stays with each owner (focal directory, slates council, mantle root range) |
| S13 | Witnessed extensions for a loaded host; overload heals, never extends | focal `liveness/wire.rs:21-36`, `suspicion.rs` | `x-swim` |
| S14 | Vivaldi coordinates with height carried on acks | slates `coordinates.rs`; focal `liveness/coordinates.rs` | `x-swim`, one implementation chosen by a determinism test (floats must encode identically) |
| S15 | Codec that refuses every malformed field before allocating | slates `swim.rs:12-17`; focal `liveness/wire.rs:1-3` | `x-swim` |
| S16 | Member identity = stable anchor + per-start nonce | slates `swim.rs:46-48`; bugs 2026-09-13/14 acknowledgements keyed on the ephemeral member id | `x-swim` |
| S17 | Verdicts as hints; decisions committed by the owner's consensus | focal (partition owner), slates (council), mantle (`node.md` §3.5) | each project; `x-swim` decides nothing durable |

### 2.13 Ledger: Raft

"Core" is `x-raft`; "shell" is `x-durable`; "log" is `x-log`; "timing" is `x-timing`.

| # | Mechanism or fix | Where, evidence | Lands as |
|---|---|---|---|
| R1 | Sans-io core, typed errors of three kinds, `Limits` | focal-raft `lib.rs:13-27`; focal 27 §4.5 | core (base) |
| R2 | Differential against raft-rs | focal `tests/differential.rs` | core dev-test (raft-rs as a dev-dependency only) |
| R3 | Pre-vote and check-quorum, audited on a timed simulation (term inflation 0 with pre-vote vs 22 without) | slates `tests/prevote.rs`, BENCHMARKS "The pre-vote audit" | core; slates' audit ported |
| R4 | A follower's timer counts silence since leader contact apart from its own age; a voter that yielded grants to the one it yielded to | slates bug 2026-09-29-a-yielding-voter-refused-the-voter-it-yielded-to; KIND succession median 6.47 → 3.08 s | port the test; patch the core if it fails |
| R5 | A member that missed its promotion still takes part in elections | slates bug 2026-09-29-a-member-that-missed-its-promotion-refused-every-election | port the test |
| R6 | A saturated term never lets two leaders share it | slates bug 2026-09-30-a-saturated-term-let-two-leaders-share-it | core has it (focal `raft.rs:1187`); slates' test ported |
| R7 | Independent election-jitter draws | slates bug 2026-09-28-correlated-election-jitter-livelocked-a-split-vote (up to 19 s) | core (focal seeds per member); test ported |
| R8 | Election base and span from measured tails and spread, margin 10, durable-ack included | slates `timing.rs:52, 170-201`; focal-timing `TickPace`; audit §11.7 | timing |
| R9 | A round's budget from the measured tail | slates bug 2026-09-14-consensus-round-expires-inside-the-wan-rtt | timing |
| R10 | Priority as an owner input; `Precedence::Log`; priority never vetoes a transfer | focal 27 §4.5, §5; slates `election_rank` (five regions 189 → 171 ms median) | core takes a priority and its spread; who computes it (placement rank, quorum RTT) stays with the owner |
| R11 | Leadership returns to the preferred leader with fit, quiet and rest rules | focal 27 §5 "Leadership returns" | shell policy; its multiples of the election timeout to be derived or cited (they are stated with reasons, not measured) |
| R12 | Leader transfer; graceful drain completes only once the successor is in office | both; slates build ledger slice 3 (drain 0.111–0.127 s vs 1.316 s election) | core (transfer) + shell (drain) |
| R13 | Learners staged in catch-up rounds, aborted when lag stops shrinking; one lagging member does not hold back the others | slates `catch_up` (`raft.rs:2669`; Figure 4.4(a) 21 → 1 rounds); bug 2026-09-30-one-lagging-member-held-back-every-council-promotion; mantle `membership.rs` | shell membership module |
| R14 | Joint consensus | focal `ConfChangeV2`; slates log-integrated | core (focal's); slates' membership tests ported |
| R15 | A leader that removes itself steps aside; voters set shrinks; voter state survives restart | focal 27 §3.3 (`LeaderLeaving`); slates bugs 2026-09-13-raft-voter-set-never-shrinks, 2026-09-14-raft-voter-state-loss | core + shell; tests |
| R16 | Pipelining with a derived window: ⌈2 × tail / heartbeat⌉ batches, bounded in bytes | slates `timing.rs`; BENCHMARKS "Pipelined replication" (2,000/s: 7,319 → 172 ms median) | core takes a byte window from timing; focal's 128 messages **excluded** as fixed |
| R17 | Out-of-order acknowledgement within a term ahead of a hole, in-order commit (ParallelRaft-CE's sync number) | slates consensus-enhancements §3.5 | core |
| R18 | Out-of-order commitment | slates consensus-enhancements §3.5 | **excluded**: the prefix model found a 12-step history that loses a committed entry |
| R19 | Conflict hints and fast backup (20 refusals → 1) | both | core |
| R20 | A late append below a snapshot, a late reply that moved progress back, a snapshot reply that credited the leader's own snapshot | slates bug 2026-09-28-a-late-append-could-land-compacted-entries-on-a-log | tests ported to the core |
| R21 | A commit rule that does not scan the backlog | slates bug 2026-09-29-a-leaders-commit-rule-scanned-its-backlog (5,000 entries: 20 ms → 96 ns) | slates' backlog bench run on the core; patch if it scales with backlog |
| R22 | Compaction by the thesis's size rule; the leader waits for followers while the log is within twice the threshold | slates `fold.rs`; BENCHMARKS "Consensus log compaction" (compacting at majority commit measured and rejected) | shell policy |
| R23 | ReadIndex with rounds given back when unconfirmed; follower reads with parked barriers; every asker of one read answered | mantle `replica.rs:157, 800`; focal 27 §5 "Follower reads", `653616e` (F63) | core (ReadIndex) + shell (rounds, parking) |
| R24 | Fast track | §2.4 | core: one algorithm, chosen by §3.8's evidence |
| R25 | MLRaft: one group's log divided into `n` logs with barriers | slates `multilog.rs`; explorer at 200 seeds × 3,000 steps; measured worse for slates' groups | a layer crate over the core; enabled by nobody until an owner shows a gain |
| R26 | Leader balancing across many groups (moves that lower the sum of squares of leaders per node) | focal 27 §5 "Multi-log synchronization" | a pure placement helper beside the shell; the decision stays with the owner |
| R27 | Unwind boundary `guarded_in` | focal `focal-consensus/src/lib.rs:33` | shell |
| R28 | Memory reserved before a transition, a page chosen before it is copied, allowances derived | focal `58c8141`, `1f87bfc` (F15, F16); `focal-raft/src/storage.rs:18-35` | core `Storage` contract + shell |
| R29 | Decoder fences for format evolution | focal `decoder.rs` | shell |
| R30 | Nonblocking drain with receipts; cross-group batching | focal `persistence.rs:1-31`; persistence tests (twelve groups) | shell |
| R31 | Messages a leader may send before its own write go out at once | mantle `replica.rs:928-936` | shell (focal gains it; audit §11.3) |
| R32 | Messages and ticks held while a Ready flushes, bounded by bytes and `2·election_tick` | mantle `replica.rs:113-124` | shell |
| R33 | A Ready refused for room waits whole | mantle `replica.rs:76-82` (audit S04) | shell |
| R34 | Protocol-aware repair with uncertainty marks and requested snapshots | mantle `replica.rs:690-760`; `crates/log` recovery | shell + log |
| R35 | When the commit needs its own fence | focal: always; mantle: never, restart repairs it from the engine (audit §11.3) | shell: a stated invariant over the state machine's contract (§3.8) |
| R36 | One flush per frame, persist record confirms, damage and restore | mantle `crates/log` | log |
| R37 | A moving base that cleans the shared log without rewriting other groups | focal `a8e95f7` (F14) | log: compared with mantle's slot reclaim (`raft-log.md` §5) under one benchmark; the better kept |
| R38 | Fast-track proposals persisted; the group's fast flag in its log | mantle `lib.rs:90-96`; focal `RecordKind::Proposal`, `RecordKind::FastTrack` | log |
| R39 | A stall covered in ticks; waits counted in the owner's periods | focal `c1af831`; slates `timing.rs:28-33` | timing |
| R40 | Timed simulation, safety explorer, exhaustive models, KIND succession | slates `support/timed.rs`, `explore.rs`, `support/exhaustive.rs`, `slot_model.rs`, `prefix_model.rs` | `x-check` and `x-sim` |
| R41 | Path model, disk faults, linearizability, power-loss property test | focal `focal-sim/src/path.rs`; mantle `mantle_disk::sim`, `range/tests/support/linear.rs`, `log/tests/log.rs` | `x-sim` and `x-check` |

---

## 3. Proposed shared crates

### 3.1 Principles

**Sans-io at every seam.** Every crate is a state machine fed `now`, input bytes and events, and
asked for output bytes, timeouts and events, as quinn-proto, focal-raft and slates' connection
and detector already are. A crate never spawns a thread or a task, never reads a clock, never
opens a socket or a file. The one exception is `x-log`, which owns its device writer thread, as
both existing logs do (one per device or node, bounded by hardware, not by groups). Completions
cross with `std::task::Waker`, which is in the standard library (`node.md` §1.3), so no crate
names a runtime.

**Budgets by trait.** Memory, disk and transport credit are reserved through small traits that
each project implements over its own accountant (`focal_memory::MemoryBudget`, slates'
`mem::budget`, mantle's admission authorities, `node.md` §2.5). A reservation is a value the
owner holds and gives back; no reference counting.

**One path per mechanism.** slates refuses a "just in case" second path or a mode switch (slates
`CLAUDE.md` §2 item 7), and mantle's rule 9 says code states what the system does now. Where the
three differ, the shared crate carries the one the evidence selects, and differences that are
genuinely per project (class sets, the durability medium, who computes priority) are type
parameters or owner inputs, not flags.

**The union of the rules.** The shared repository's lints are the strictest of the three:
slates' `indexing_slicing`, `string_slice`, `panic_in_result_fn`, `unwrap_in_result` on top of
mantle's and focal's no-panic set; `disallowed_types` for `Arc`, `Rc`, `Mutex`, `RwLock` with
the listed exceptions (rustls and quinn-proto configuration by signature); an unsafe budget that
only shrinks; every constant with a derivation or a citation.

**Arc exceptions, named.** quinn-proto's `EndpointConfig`, `ServerConfig` and `TransportConfig`
are held by `Arc` (`config/mod.rs:38-199`), and rustls's configs likewise. These are slates' D-8
exception 2 ("foreign APIs that take `Arc` by signature"). They are configuration, built once per
endpoint, never on a data path. Patching them out of the vendored copy is possible but would be
the largest single divergence from upstream, so this note leaves them as the named exception
(§6, open).

### 3.2 The crate map

```
x-check ── x-sim                    (test infrastructure; dev-dependencies of everything below)

x-timing                            (no dependencies)
x-quic   = vendored quinn-proto + patches  (rustls, AWS-LC)
x-transport ── x-quic, x-timing
x-datagram  ── (exporter secret from x-transport through a trait; AWS-LC)
x-swim      ── x-timing            (sends through a trait the datagram plane implements)
x-raft      ── x-timing            (prost derive, no protoc)
x-durable   ── x-raft, x-timing
x-log       ── x-durable's LogStore trait
x-multilog  ── x-raft               (MLRaft layer, R25)

adapters (thin, one per runtime, each in the repository of the runtime it serves or in the shared one):
  x-tokio  (focal; mantle's network runtime)    slates-rt adapter (in slates)    x-sim driver
```

### 3.3 `x-quic`: the vendored quinn-proto

quinn-proto is already sans-io: an `Endpoint` and `Connection` that take datagrams and `now`
and give transmits and timeouts. The fork is quinn-proto 0.11.18 as published, verified against
its crates.io SHA-256, with each local change listed in an `UPSTREAM.md` and marked in the source,
as mantle does for AWS-LC (`mantle vendor/UPSTREAM.md`). Patches, each offered upstream:

| Patch | From | Changes | Gate |
|---|---|---|---|
| Q1 pacing quantum | T30 | `BURST_INTERVAL_NANOS`, `MIN_BURST_SIZE`, `MAX_BURST_SIZE` (`pacing.rs:145-151`) become slates' law: 1 ms of the rate, floor two datagrams, cap derived (slates' 64 KiB) | slates' thin-link rows and focal's grid, pre-declared rule |
| Q2 controller pacing rate | T31 | the `Controller` trait may return a pacing rate, which the pacer uses when present | focal grid |
| Q3 adaptive reordering | T27 | packet and time thresholds widened on a spurious loss and restored after quiet recoveries, memory bounded by count and age (slates `reorder.rs`) | slates reorder-jitter scenario and both grids |
| Q4 PMTU refinements | T35 | each of slates' five rules where quinn's `mtud` differs | slates' path-MTU bench and the floor-path grid |
| Q5 assembler limit exposed | T33 | `MAX_CHUNKS` (`assembler.rs:361`) made public so the window is derived from it | focal's 100 Mbit/s closure scenario |
| Q6 no-panic hardening | §2.10 | each panic site reachable from peer input closed at its cause, found by fuzzing the frame and packet decoders under the shared simulator; until each is closed, every call into `x-quic` from `x-transport` runs inside one unwind boundary that turns an unwind into a closed connection | fuzzing corpus; the boundary's test |
| Q7 conditional patches | T2, T18, T19, T21 | only if the ported regression test fails on unpatched quinn-proto | the test |

**Recommendation (from note 30 §6.3, unchanged).** The wire is RFC 9000; slates' dialect is not
carried forward. What would overturn it is a measurement: slates' grid finding patched quinn
losing to the dialect on paths the projects serve.

### 3.4 `x-transport`: the application layer

mantle's application protocol in slates' shape (`node.md` §3): one connection per peer,
bidirectional exchanges of a request and a reply, classes with a credit reserve for the classes
above, absolute credits, typed refusals, streaming bodies through reservations, the class decided
by message kind and sender role, and TLS over TCP carrying the same protocol when UDP is blocked.
It is sans-io: it owns quinn-proto's `Endpoint` and connections and is driven by an adapter.

```rust
/// What a project tells the transport about its messages. A project's class set is its own
/// (slates: control, metadata, bulk; focal: its TrafficClass; mantle: control, replication,
/// request, bulk); the transport only orders them and keeps the reserve.
pub trait Classes {
    type Class: Copy + Ord;          // lower is more urgent
    type Kind: Copy;
    type Role: Copy;
    /// The class of a message of `kind` from a sender of `role`, or `None` when that role may
    /// not send that kind (audit §13.3, node.md §3.6).
    fn class_of(kind: Self::Kind, role: Self::Role) -> Option<Self::Class>;
    /// The largest frame a class carries; its bound is checked before any body byte is read.
    fn frame_bound(class: Self::Class) -> u64;
}

/// Bytes the node may hold for the transport; implemented over each project's accountant.
pub trait Budget {
    fn reserve(&mut self, bytes: u64, class: Lane) -> Result<Reservation, Refusal>;
    fn release(&mut self, reservation: Reservation);
}

pub struct Endpoint<C: Classes, B: Budget> { /* one per socket, one owner */ }

impl<C: Classes, B: Budget> Endpoint<C, B> {
    // Driven by the runtime adapter.
    pub fn handle_datagram(&mut self, now: Instant, from: SocketAddr, ecn: Option<Ecn>, bytes: &mut [u8]);
    pub fn poll_transmit(&mut self, now: Instant, out: &mut Vec<u8>) -> Option<Transmit>;
    pub fn poll_timeout(&mut self) -> Option<Instant>;
    pub fn handle_timeout(&mut self, now: Instant);
    pub fn poll_event(&mut self) -> Option<Event<C>>;   // Connected, Request, BodyReady, Reply,
                                                         // Writable, Refused, Closed, Unreachable
    // Used by the owner.
    pub fn connect(&mut self, now: Instant, peer: PeerId, address: SocketAddr) -> Result<(), Refusal>;
    pub fn open(&mut self, now: Instant, peer: PeerId, kind: C::Kind, header: &[u8],
                body: Option<u64>, deadline: Progress) -> Result<ExchangeId, Refusal>;
    pub fn write_body(&mut self, exchange: ExchangeId, from: &[u8]) -> Result<usize, Refusal>;
    pub fn read_body(&mut self, exchange: ExchangeId, into: &mut Reservation) -> Result<usize, Refusal>;
    pub fn reply(&mut self, exchange: ExchangeId, header: &[u8], body: Option<u64>) -> Result<(), Refusal>;
    /// A long-lived stream per (peer, lane), for replication: frames in order, always read
    /// (node.md §3.2).
    pub fn send_frame(&mut self, peer: PeerId, lane: LaneId, frame: &[u8]) -> Result<(), Refusal>;
    pub fn export_keying_material(&self, peer: PeerId, label: &[u8], context: &[u8],
                                  out: &mut [u8]) -> Result<Epoch, Refusal>;
    pub fn path(&self, peer: PeerId) -> Option<PathFacts>;   // rtt, rttvar, delivery rate,
                                                              // max datagram, cwnd
}
```

`Progress` is a progress-charged deadline (T39) built from `x-timing`; an exchange ends when a
period moves less than a datagram, never at a fixed wall time. Credits: each connection's receive
window is `min(BDP, share)` (`node.md` §3.3), set live with quinn's `set_receive_window`; the
class reserve (T16) is kept by the endpoint's own accounting, because quinn has one connection
window; the per-stream ceiling is derived from Q5's exposed limit. Lanes for consensus are as wide
as the core's window (T37). Admission (T9–T11) bounds pending handshakes, connections per identity
and streams per connection from the budget.

**How each project uses it.**

| | Uses | Gains | Gives up |
|---|---|---|---|
| mantle | node-to-node and client protocol (`node.md` §3–§5), HTTP/1.1 listener stays separate | everything; nothing to migrate | nothing; it has no transport |
| focal | replaces focal-wire's generic core (`transport.rs`, `peers.rs`, `admission.rs`, `frame.rs`, `round.rs`, `congestion.rs`); focal-wire keeps its domain envelopes, managed streams and validators | pacing, reordering and PMTU patches; class reserve; streaming bodies; windows from memory instead of 144 MiB per connection | its fixed timeouts and reference path (excluded anyway); a direct quinn API (it goes through the endpoint) |
| slates | replaces the session plane (`flight`, `keys`, `streams`, `flow`, `connection`, `conn`, `demux`, `pmtud`, `reorder`, `congestion`, `pacer`, `rtt`, `packet_number`, `handshake`, `session`, `stream`, `endpoint`), driven by its runtime through an adapter in slates | migration, path validation, IPv6, packet-byte accounting, an uncapped PTO, streaming exchanges, GSO/GRO, ECN, a standard wire | its own dialect and the code it wrote for it; two named `Arc` sites (quinn-proto and rustls configs) in place of one |

### 3.5 `x-datagram`: the sealed plane

Beside `x-transport`, not inside it: the plane has its own socket so control datagrams never share
a congestion window with bulk (25 §6), and it needs from the transport only an exporter secret per
connection epoch.

```rust
pub struct Plane { /* per-peer epochs, sealers, replay windows, pending packs */ }

impl Plane {
    pub fn new(limits: PlaneLimits) -> Result<Self, Refusal>;     // peers, epochs per peer, window
    /// A new connection to `peer` gives a new epoch: keys from HKDF-Expand-Label over the
    /// exporter secret, one per direction (D3). Old epochs overlap for one handshake round trip.
    pub fn install_epoch(&mut self, peer: PeerId, epoch: Epoch, secret: &ExporterSecret) -> Result<(), Refusal>;
    pub fn retire(&mut self, peer: PeerId, epoch: Epoch);
    /// Path facts from the transport: the largest datagram and the round trip (D9).
    pub fn set_path(&mut self, peer: PeerId, max_datagram: u16, rtt: Duration);
    /// Queues a message for the peer's next datagram; refused if it can never fit one.
    pub fn queue(&mut self, peer: PeerId, message: &[u8]) -> Result<(), Refusal>;
    /// Seals and hands out at most one packed datagram per peer per call.
    pub fn flush(&mut self, now: Instant, out: &mut dyn FnMut(PeerId, &[u8]));
    /// Runs the acceptance order (D4) and yields the messages of an authentic, fresh datagram.
    pub fn open<'a>(&mut self, now: Instant, datagram: &'a mut [u8],
                    fence: &dyn Fence) -> Result<Opened<'a>, Refusal>;
}

/// The owner's view of who may send: the sender's current epoch and term (D4's fencing).
pub trait Fence { fn admits(&self, sender: PeerId, epoch: Epoch) -> bool; }
```

The replay window starts at RFC 4303's 64 and widens to the reordering the plane measures,
within memory the plane's limits reserve (`node.md` §3.4). Raft control messages ride it when they
carry no entries and fit one datagram; anything larger goes on the replication stream
(`node.md` §3.1). A `FAST_PROPOSE` carries a payload, so it rides the plane only under the path MTU
(note 07 §7.3).

| | Gains | Gives up |
|---|---|---|
| mantle | the design of `node.md` §3.4 built once | — |
| focal | Raft control and probes no longer queue behind content on one connection (they share quinn's window today) | probes' current path through the peer pool |
| slates | its codec wired, a sliding window, fencing, keys that cannot reuse nonces | the enrolled control secret as the key source (its distribution half is owed anyway); RustCrypto `aes-gcm` and `hkdf` for AWS-LC |

### 3.6 `x-swim`: membership and failure detection

**Recommendation: its own crate.** It is used by all three, it is pure protocol, and its
decisions (suspect, dead, alive) are hints to each project's own committed membership (S17), so
it has no business inside the transport or the Raft crates. It is slates' sans-io detector
(the most complete and the only one free of a runtime), extended with focal's witnessed
extensions and the derivations both projects owe.

```rust
pub struct Detector { /* members, suspicions, gossip, coordinates, local health */ }

impl Detector {
    pub fn new(me: Member, limits: SwimLimits, seed: u64) -> Result<Self, Refusal>;
    /// One protocol period: the period is the owner's, at least three round-trip estimates
    /// (node.md §3.5). Returns the probes and relays to send.
    pub fn tick(&mut self, now: Nanos, out: &mut Vec<Outgoing>);
    pub fn on_message(&mut self, now: Nanos, from: Member, message: Message,
                      out: &mut Vec<Outgoing>) -> Result<(), Refusal>;
    pub fn observe_rtt(&mut self, peer: Member, sample: Nanos);
    /// The node's own lag: the delay between a probe's arrival and its handling (node.md §3.5).
    pub fn observe_self_lag(&mut self, lag: Nanos);
    /// A witness the owner cannot fake while stuck (S13), for asking an accuser for time.
    pub fn set_progress_witness(&mut self, witness: u64, overloaded: bool);
    pub fn admit(&mut self, member: Member, incarnation: u64) -> Result<(), Refusal>;
    pub fn remove(&mut self, member: Member);
    pub fn poll_event(&mut self) -> Option<Verdict>;   // Alive, Suspect, Dead { stood_for }, Refuted
}

pub mod codec { /* encode/decode with hostile-input checks (S15) */ }
```

Bounds (`SwimLimits`) come from the owner: members from its committed directory, inflight probes
and relays from the period and the path, gossip per message from `λ·ln(n+1)` and the datagram
size. focal's literals (`driver.rs:36-49`) are replaced by those derivations.

| | Uses | Gains | Gives up |
|---|---|---|---|
| mantle | within a cell, on the datagram plane | built once | — |
| focal | the liveness driver becomes an adapter over `Detector` | sans-io testability; slates' fixed-point suspicion and seed handling | its tokio-task structure and its fixed constants |
| slates | its detector moves out; `swim.rs` codec moves; its driver runs it on the datagram plane | focal's witnessed extensions; a sealed plane of its own | probes on the session plane |

### 3.7 `x-timing`

focal-timing's `ProgressDeadline`, `RoundBudget`, `DeadlineExtender`, `RoundWait`, `TickPace` and
`ExchangeRtt`, with slates' `ElectionTiming::derive`, `round_budget` and the pipelining window
`⌈2 × tail / heartbeat⌉`. **Open: the path estimator.** slates uses RFC 9002's smoothed estimator
and its `smoothed + 4·rttvar` tail; focal uses a median and MAD over the latest sixteen, so one late
answer does not set an election timeout. Both are reasoned; neither was measured against the other.
The decision is a timed-simulation run on slates' five-region matrix and the KIND profile, with
the successor time distribution and campaign count as the judged outputs (slates BENCHMARKS "A
leader loss across published inter-region round trips"), fixed before the run. The tail fed in
includes the durable-acknowledgement time (audit §11.7).

### 3.8 `x-raft` and `x-durable`

**The core** is focal-raft, unchanged at first (§5 step R-1), then:

- **Its own message types.** The `raft-proto` dependency is replaced by the same messages
  declared as Rust structs with `prost` derive, which needs no protoc and no build script, so the
  encoding is byte-identical (golden vectors for every message type prove it) and the second git
  source and the C++ build leave production. raft-rs stays a dev-dependency for the differential.
- **`Limits::derive(inputs)`** replaces `Limits::default()`: pending messages and entries from
  the byte window and entries per message from the frame bound; proposals and fast window from
  the path; vote bytes from the frame bound.
- **A byte-bounded pipeline window** set by the owner from `x-timing` (R16), replacing the count.
- **Out-of-order acknowledgement within a term** (R17), behind slates' sync-term rule.
- **slates' regression tests** for R4–R7, R15, R20, R21, each run on the core, each a patch only
  if it fails.

**The fast track (open; how to decide).** One algorithm. The candidate is focal's, for three
reasons, each with what would overturn it: (1) its log stays the classic log and no entry's bytes
change, so classic recovery and the raft-rs differential still apply to everything outside the
held proposals; (2) its leader takes the first entry it hears of, which focal recorded as costing
no more than the classic track at any loss (focal 27 §4.6), where slates' recorded crossover worsens
at 10 % loss; (3) it is already built under the shared core. The obligations it must meet before it
is the shared algorithm: slates' exhaustive search applied to it (its transition rules encoded
in `x-check`'s explorer at slates' scopes, including the three-index, four-term scope that found
slates' own loss), the reconfiguration and liveness that the TLA+ model lacks (note 07 §10.6),
slates' five-region crossover run on it, and mantle's admitted-request accounting (audit §11.5:
every admitted request committed once, displaced and retryable, or unknown pending recovery). If
it fails any, slates' window algorithm takes the same tests. No owner enables it until its own
application proof exists (focal 27 §4.6's owner table; audit §11.5).

**The shell** is mantle's Ready pipeline with focal's accounting:

```rust
/// What the shell asks of a state machine. `durable_applied` decides the commit fence (R35):
/// when the machine's own durable state covers an index, the commit at that index needs no
/// fence of its own and restart repairs the log's commit from it (mantle); when it reports
/// nothing, the commit is fenced before any output that depends on it leaves (focal).
pub trait StateMachine {
    type Answer;
    fn apply(&mut self, entry: &Entry, budget: &mut dyn Budget) -> Result<Self::Answer, Fatal>;
    fn durable_applied(&self) -> Option<u64>;
    fn snapshot(&mut self, at: u64) -> Result<SnapshotTicket, Fatal>;   // external, chunked
    fn install(&mut self, snapshot: SnapshotSource) -> Result<(), Fatal>;
}

/// Where a group's updates become durable: the shared device log, or a RAM publication.
pub trait LogStore {
    fn submit(&mut self, group: GroupId, update: Update, waker: &Waker) -> Result<Ticket, Refusal>;
    fn poll(&mut self, ticket: &Ticket) -> Poll<Result<(), LogError>>;
    fn view(&self, group: GroupId) -> Result<View, LogError>;     // bounds, terms, cached entries
    fn fetch(&mut self, group: GroupId, low: u64, high: u64, waker: &Waker) -> Result<Ticket, Refusal>;
}

pub struct Replica<L: LogStore, M: StateMachine> { /* core, staged ready, held inputs, rounds */ }

impl<L: LogStore, M: StateMachine> Replica<L, M> {
    pub fn tick(&mut self) -> Result<(), ReplicaError>;
    pub fn step(&mut self, message: Message, from: Authenticated) -> Result<(), ReplicaError>;
    pub fn propose(&mut self, entry: &[u8], lane: Lane) -> Result<(), ReplicaError>;
    pub fn propose_fast(&mut self, entry: &[u8], lane: Lane) -> Result<u64, ReplicaError>;
    pub fn read_index(&mut self, context: ReadContext) -> Result<(), ReplicaError>;
    pub fn transfer(&mut self, to: NodeId) -> Result<(), ReplicaError>;
    pub fn change(&mut self, change: &ConfChangeV2) -> Result<(), ReplicaError>;
    /// Takes the core's Ready, gives out what a leader may send before its own write, submits
    /// the update, applies what is already committed; returns with `persisting` set.
    pub fn begin(&mut self, log: &mut L) -> Result<Drive<M::Answer>, ReplicaError>;
    /// Finishes once the ticket is ready: the messages that waited, applies, advance.
    pub fn finish(&mut self, log: &mut L) -> Result<Drive<M::Answer>, ReplicaError>;
    pub fn compact(&mut self, keep: u64, log: &mut L) -> Result<(), ReplicaError>;
}
```

The log is passed in by the shard that owns the replica, never shared through `Arc`: a shard owns
its replicas and a handle to each device log's submission queue, and reads beyond a group's cached
entries go to the log's owner as a `fetch` ticket, which the core's existing
`StorageError::LogTemporarilyUnavailable` (`focal-raft/src/error.rs:18-19`) is for. That removes
mantle's `RwLock<State>` and `Arc<Log>`; whether the core resumes cleanly after such a fetch is an
obligation to test, not a fact this note verified. focal's `guarded_in`, memory reservations before
a transition (R28), decoder fences (R29) and checkpoint images come in around this pipeline;
focal's conservative hold of a leader's appends becomes mantle's early send (R31).

**How each project uses the Raft crates.**

| | Core | Shell | Log | Gains | Gives up |
|---|---|---|---|---|---|
| mantle | `x-raft` (replaces the git pin) | `Replica<DeviceLog, RangeMachine>`; `RangeMachine` is mantle's engine and layer | `x-log` (its own, moved) | focal's budgets, unwind boundary, decoder fences; slates' pipelining window, out-of-order acks, learner rounds, compaction rule | its log's locks and condition variable (a gain under its own rules) |
| focal | `x-raft` (its own core, moved) | `Replica<DeviceLog, …>` replacing `DurableNode`'s drain | `x-log`, after a one-way WAL conversion | one flush per commit instead of two fences; early leader sends; PAR repair; slates' enhancements | focal-log and its `CURRENT` rename protocol; `DurableNode`'s API as is |
| slates | `x-raft` (replaces the hecate core) | `Replica<RamPublication, ConfigurationFold>` | none: a RAM `LogStore` over its publication, keeping R1 | joint consensus v2, the raft-rs differential, typed limits, budgets; one core to keep correct instead of two | its wire format for Raft, its fast-track algorithm unless §3.8's tests choose it, a 6,262-line core it measured carefully |

### 3.9 `x-log`

mantle's per-device log (§2.3) with three changes the rules require: an owner thread that holds
all state (no `RwLock`, `Mutex` or `Condvar`), `Waker` tickets in place of the mpsc `Pending`, and
entry bytes owned by the frame buffer and handed out by copy into the caller's reservation or by
the fetch ticket instead of `Arc<[u8]>`. Its `BlockFile` trait (`mantle crates/disk/src/block.rs:8-27`,
four methods) moves into the crate; mantle's device file and its simulated file implement it there
and in `mantle-disk`; focal and anyone without hardware detection get a portable implementation over
`std::fs` with the platform's full flush. focal's moving base (R37) and mantle's slot reclaim are
compared under one benchmark (segments, groups and checkpoint cadence from mantle's
`bench_log.rs`) before one is kept.

### 3.10 `x-sim` and `x-check`: shared test infrastructure

| Piece | From | Use |
|---|---|---|
| Path model: Gilbert–Elliott, bottleneck queue, MTU, NAT expiry | focal `focal-sim/src/path.rs`, `network.rs` | transport grids, election timing |
| Seeded UDP fabric with interface MTU refusal | slates `rt/src/sim.rs` | quinn-proto endpoints under virtual time |
| Disk with crash, faults and bit flips | mantle `mantle_disk::sim` | `x-log`, `x-durable` |
| Timed Raft simulation on published matrices | slates `cluster/tests/support/timed.rs` | crossover, pipelining, leader loss |
| Safety explorer over real nodes | slates `cluster/tests/explore.rs` | the core under adversarial schedules |
| Exhaustive search with symmetry reduction | slates `cluster/tests/support/exhaustive.rs` | fast-track models, multilog |
| Differential against raft-rs | focal `focal-raft/tests/differential.rs` | the core's classic track |
| Linearizability checker (WGL + Lowe) | mantle `range/tests/support/linear.rs` | every project's history checks |
| Congestion, class and PMTU grids with pre-declared rules | slates examples, focal `tests/congestion.rs` | every `x-quic` patch |
| Recorded-run replay | new | equivalence checks in §5 |

focal-sim's `history.rs` stays in focal (it depends on `focal_model`). `x-sim` is test
infrastructure, held to the production lints as focal requires of `focal-sim` (focal `CLAUDE.md`
§1).

---

## 4. Where they live and how they are versioned

### 4.1 Options

| Option | For | Against |
|---|---|---|
| **(a) A new repository under `github.com/hyper-light`, its own workspace** | neutral ground; its rules are the union; no consumer's domain churn reaches it; each consumer pins independently | a fourth repository to keep; CI must build three consumers |
| (b) Inside focal | focal-raft, focal-timing, focal-sim already live there | slates would depend on focal's repository and its toolchain (1.94.1) and tokio-coupled crates in one workspace; focal's domain commits move every pin |
| (c) Inside slates | slates' rules are the strictest | slates bans tokio and git dependencies of its own; the tokio adapter could not live there; slates' churn (277 bug records in its ledger) moves every pin |
| (d) A workspace inside mantle's `vendor/` | mantle already runs a vendored workspace as a gate | focal and slates would depend on mantle's repository |

**Recommendation: (a).** Its `CLAUDE.md` is the union of the three rule sets (§3.1), its toolchain
is pinned exactly at 1.98.0 (slates' and mantle's), edition 2024. Gates: mantle's seven gates,
slates' `xtask` structural checks (literals, unsafe budget), focal's production-policy script, and
a downstream job (§4.3).

### 4.2 How consumers pin it

- **mantle.** `deny.toml` already requires `required-git-spec = "rev"` and lists allowed git
  sources (`deny.toml` `[sources]`); the shared repository is added and, once `x-raft` owns its
  message types, `https://github.com/hyper-light/focal` and `https://github.com/tikv/raft-rs`
  leave production (raft-rs remains only as a dev-dependency of the shared repository, not of
  mantle). The vendored quinn-proto lives in the shared repository's `vendor/`, so mantle consumes
  it through the git dependency; mantle's own `vendor/` keeps AWS-LC, which the shared crates use
  through the same `[patch.crates-io]`.
- **focal.** `deny.toml` today allows only `tikv/raft-rs` (`focal deny.toml:23-24`); it gains the
  shared repository with `rev`. focal moves to toolchain 1.98.0.
- **slates.** Today it has no git dependency and no `deny.toml`. A git dependency pinned by `rev`
  is its first; whether that needs explicit authorization under slates `CLAUDE.md` §2 is the
  owner's call (§6). An alternative that keeps slates free of git sources is a vendored snapshot of
  the shared crates in slates' tree, refreshed by a script that records the source revision and
  SHA, as mantle records AWS-LC.

### 4.3 Moving in lockstep without breaking each other

1. **Pins, never branches.** Each consumer pins an exact revision. A shared change reaches a
   consumer only when that consumer bumps; nothing breaks a consumer by landing.
2. **A downstream job.** The shared repository's CI checks out each consumer at its current
   head, overrides the pin to the candidate revision (`[patch]`), and runs that consumer's own
   gates. A change that breaks a consumer is visible before it lands, and the consumer's owner
   decides whether to bump.
3. **Wire and disk compatibility are tested, not assumed.** Golden vectors for every wire message
   and record; a mixed-revision simulation (members at the old and new revision in one group,
   connections between old and new endpoints) before any bump that changes a format; the ALPN
   carries the application protocol's version so incompatible endpoints refuse each other with a
   typed error.
4. **Releases are tags.** A tag names a revision every consumer's downstream job passed. Consumers
   may pin between tags; tags are for the owner's convenience.

---

## 5. Migration plan

Each step names its gate and how it is undone. "Equivalence" means a recorded run replayed through
the old and new code with the outputs compared: for a core or shell, the sequence of Readies
(messages, entries, hard states, committed entries) per seed; for a transport, the per-seed
exchange latencies and bytes of the grids.

### 5.1 Order across the repositories

```
R-1 core moved        → R-2 own message types → R-3 slates' core enhancements, one at a time
L-1 log moved         → L-2 owner thread and wakers
D-1 shell extracted from mantle → D-2 focal's shell onto it
Q-1 quinn-proto vendored, zero patches → Q-2 patches one at a time
T-1 application layer from focal-wire's core → T-2 mantle builds on it
P-1 datagram plane → S-1 SWIM
F-1 focal's WAL conversion (one-way)
X-1 slates' consensus onto x-raft (one-way for its running groups) → X-2 slates' session plane onto x-transport
```

Strongest-first: each piece moves from the implementation the evidence ranks highest (§1 item 5),
so the first consumer of each shared crate is the project it came from, and its own suite is the
first gate.

### 5.2 Steps

**R-1. Move focal-raft.** Copy `crates/focal-raft` with history into the shared repository as
`x-raft`, unchanged. focal and mantle point their dependency at it. Gate: focal-raft's own tests,
the raft-rs differential, focal's full suite, mantle's `crates/range` suite including `sim.rs`.
Equivalence: none needed (the same bytes compile). Undo: revert the pins.

**R-2. Own message types.** Replace `raft-proto` with prost-derived structs. Gate: golden vectors
equal for every message and entry type; the differential unchanged (raft-rs on its own types, the
core on the new ones, compared by encoded bytes); focal's WAL replay of a recorded data directory.
Undo: revert.

**R-3. slates' core enhancements.** One commit each, in this order, because each later one is
measured by harnesses the earlier ones need: R6 and R7 regression tests (expected to pass); R4,
R5, R20, R21 tests (patch on failure); R16 byte window; R17 out-of-order acknowledgement; R13
learner rounds; R22 compaction rule. Gate for each: the slates test or bench that motivated it, run
against `x-raft` and showing slates' recorded improvement; the raft-rs differential, with any
intended divergence added to focal-raft's divergence table (focal 27 §4.5) and its own test; mantle
and focal suites. Undo: revert the commit.

**L-1. Move mantle's log** to `x-log`, `BlockFile` with it. Gate: `crates/log/tests/log.rs`,
`fairness.rs`, the range simulation. Equivalence: bytes on the simulated device identical per seed.
Undo: revert.

**L-2. Owner thread and wakers.** Replace `Arc<Shared>`, `RwLock`, `Mutex`, `Condvar` and the mpsc
`Pending` with an owner-held state, a bounded submission queue and `Waker` tickets, and entry
fetches by ticket. Gate: L-1's suite; a test that a completion wakes only its submitter (note 26
§5); mantle's log benchmark against its recorded baseline (`crates/mantle/src/bench_log.rs`).
Equivalence: frame contents per seed identical (only the wake path changes). Undo: revert.

**D-1. Extract the shell from mantle's replica.** Move the Ready pipeline (`begin`, `drive`,
staged parts, held inputs, rounds, repair, compaction) into `x-durable`, leaving
`RangeMachine: StateMachine` in mantle. Gate: `crates/range/tests/group.rs`, `sim.rs` (every
invariant in its header, linearizability per key). Equivalence: recorded simulation seeds replay
to identical Ready sequences and identical histories. Undo: revert.

**D-2. focal's shell onto `x-durable`.** Bring in `guarded_in`, reservations before transitions,
decoder fences, checkpoint images and the nonblocking receipts; `DurableNode` becomes a focal
wrapper over `Replica<FocalLog, …>` while focal still runs focal-log behind the `LogStore` trait.
Gate: focal-consensus's tests (`persistence_tests` twelve-group barrier, `raft_safety_tests`,
`sim_election_tests`, `sim_fast_tests`, `staging_peaks`, `allowances`) and focal's real-process
suites. Equivalence: focal's recorded simulation seeds replay with identical committed output;
message timing changes where leader appends now go before the leader's own write, which is the
intended difference and is checked by the safety explorer, not by equality. Undo: revert.

**F-1. focal's WAL onto `x-log`** (one-way). A conversion tool reads a focal-log directory
(`FOCALW01`) and writes an `x-log` device file with the same groups, entries, hard states,
proposals and checkpoint images; focal opens either until the conversion is done on every node,
then the old reader is deleted. Gate: conversion of recorded focal data directories, read back
equal; focal's restart and restore runbooks on real processes. Undo: possible only while the old
files are kept; the step is declared complete, and `focal-log` deleted, only after a release in
which nothing reads the old format.

**Q-1. Vendor quinn-proto 0.11.18 with zero patches.** focal's `quinn` resolves to it through
`[patch.crates-io]`. Gate: quinn-proto's own suite, focal's `tests/congestion.rs` grid
equal to its recorded rows. Undo: drop the patch line.

**Q-2. Patches one at a time** (Q1–Q7 of §3.3). Gate for each: focal's grid and slates' congestion,
class and PMTU grids ported onto `x-sim`, each judged by its own pre-declared rule; the patch lands
only if no row regresses outside the measured noise band (slates BENCHMARKS records that band:
per-scenario median ratios 0.79–1.22 between disjoint seed sets). Undo: revert the patch.

**T-1. The application layer.** Port focal-wire's generic core into `x-transport` as sans-io, with
the changes of §3.4; write the tokio adapter. focal-wire keeps its domain layer over it. Gate:
focal-wire's tests (`adversarial`, `peer_registry`, `congestion`, `custody_contract`, …) and focal's
real-process suites. Equivalence: the grid's per-seed latencies within the noise band (the
transport code changes, so byte equality is not expected). Undo: revert focal's pin.

**T-2. mantle builds its transport** on `x-transport` (`node.md` §10's build order). Nothing to
migrate.

**P-1. The datagram plane.** Port slates' `seal.rs`, `accept.rs`, `schedule.rs` onto AWS-LC; add
exporter keys, the sliding window, fencing and packing. Gate: slates' golden vector reproduced
exactly (D7), its hostile and plane tests, a restart test proving no nonce repeats across epochs.
Then each project moves its control messages onto it (focal and slates) or builds on it (mantle).
Undo: each project's control messages go back to their QUIC lane.

**S-1. SWIM.** Move slates' detector and codec; add focal's extensions and the derived bounds.
Gate: slates' `tests/swim.rs`; focal's `swim_tests.rs` and `algorithm_tests.rs` ported onto the
shared detector; a timed simulation with a starved node (slates bug
2026-09-13-swim-fixed-probe-deadline-kills-a-starved-live-peer as the scenario). focal's liveness
driver becomes an adapter; slates' driver moves its probes onto the datagram plane. Undo: revert
each project's pin.

**X-1. slates' consensus onto `x-raft`** (one-way for running groups). slates' council and root
groups hold configuration only, so a running fleet moves by re-founding each group from its
committed configuration at a barrier: the old group commits a final entry naming the new group's
founding state; the new group starts from that state as its snapshot; the old one stops. This is
focal-ranges' fenced intent, seed, catch-up, barrier, activate pattern (note 07 §7.2 D). Gate:
slates' explorer, conformance, prevote, priority, wan_election, pipelining and fast_track suites
retargeted at `x-raft`; the prefix and slot models re-encoded for whichever fast track §3.8
selects; the KIND succession lane (its six-trial median within its recorded spread); slates'
N=1 differential (rule R8). Undo: before the re-founding, revert; after, a re-founding back is the
only way, and it is the same procedure.

**X-2. slates' session plane onto `x-transport`.** Write the slates-rt adapter in slates. Gate:
every transport regression test from slates' bug records (T1–T22, T34, T35 rows) passing against
the shared stack; slates' class-latency, congestion and PMTU benches showing the shared stack no
worse than the dialect's recorded rows outside the noise band; `slates-server` fleet and
`slates-cli` real-process tests. A fleet moves by rolling restart, each node speaking the new ALPN
and refusing the old with a typed error, so a mixed fleet partitions cleanly rather than
misinterpreting; the procedure needs a slates fleet-level plan the owner approves (§6). Undo:
revert slates' pin before the roll.

### 5.3 What is deleted, and when

| Repository | Deleted | After |
|---|---|---|
| focal | `crates/focal-raft`, `crates/focal-timing`, the generic parts of `crates/focal-sim` | R-1 and the timing move |
| focal | `crates/focal-consensus` drain internals; `DurableNode` reduced to a wrapper | D-2 |
| focal | `crates/focal-log` | F-1, one release later |
| focal | `focal-wire`'s `transport.rs`, `peers.rs`, `admission.rs`, `frame.rs`, `round.rs`, `congestion.rs` (generic parts) | T-1 |
| focal | `focal-node/src/liveness/{suspicion,gossip,health,coordinates,wire}.rs` | S-1 |
| mantle | `crates/log` | L-1 |
| mantle | the generic half of `crates/range/src/replica.rs`; `tests/support/linear.rs` | D-1 |
| mantle | `focal-raft` and `raft-rs` from `deny.toml`'s production sources | R-2 |
| slates | `cluster/src/raft.rs`, the Raft half of `raft_wire.rs`, `timing.rs`, `multilog.rs` (moved to `x-multilog`), `fold.rs` (moved to the shell) | X-1 |
| slates | `cluster/src/{detector,swim,coordinates,fixed}.rs` | S-1 |
| slates | `transport/src/{flight,keys,streams,flow,connection,conn,demux,pmtud,reorder,pacer,rtt,packet_number,handshake,session,stream,endpoint}.rs`, `congestion/` | X-2 |
| slates | `transport/src/{seal,accept,schedule}.rs` | P-1 |

slates' measured-and-rejected records and bug records stay where they are; each moved mechanism's
module header in the shared crate cites them by path and revision, as slates' and mantle's rules
both require.

---

## 6. Risks and open decisions for the owner

**Decisions this note cannot make.**

1. **Names** of the repository and crates (`x-` is a placeholder).
2. **The fast-track algorithm** (§3.8): accept the proposed tests as the decision procedure, or
   decide now.
3. **The path estimator** (§3.7): RFC 9002 EWMA or median/MAD, by the proposed run.
4. **Lifeguard's local-health multiplier beside mantle's measured self-lag** (S4): keep both
   (they measure different things: the multiplier counts missed and refuted probes, the lag
   measures handling delay), or only the measured lag as `node.md` §3.5 states for mantle.
5. **slates' first git dependency** (§4.2): a git pin, or a vendored snapshot.
6. **tokio in the shared repository.** The tokio adapter is a separate crate that slates never
   links; slates `CLAUDE.md` §2 item 2 bans tokio "anywhere". Whether "anywhere" covers a sibling
   crate in a shared repository that slates does not depend on is the owner's reading.
7. **TLA+ in the shared repository's CI.** slates bans it from CI and from the owner's machine
   (§2 item 13); focal checks `FastTrack.tla` in CI. Options: TLC in the shared CI only, never on
   the owner's machine; or the TLA+ model kept as a document and the fast track checked by
   `x-check`'s Rust explorer alone.
8. **`Arc` at quinn-proto's and rustls's configuration signatures** (§3.1): accept as the named
   exception (slates D-8 exception 2, which needs per-site authorization under slates `CLAUDE.md`
   §2 item 1), or patch quinn-proto's configs to owned values, which is the largest divergence from
   upstream and makes every upstream merge harder.
9. **slates' fleet migration** (X-1, X-2): re-founding running council and root groups and a
   rolling ALPN change on a fleet. slates is pre-release; if no fleet must survive, both become
   restarts.
10. **Release cadence and ownership.** Who may land in the shared repository, and whether a change
    may land when one consumer's downstream job fails (proposed: it may, since pins protect the
    consumer, but the failure is recorded and the consumer's owner decides).

**Decided by the owner, 2026-09-30.**

- Item 1: the repository is `github.com/hyper-light/hyper-raft`. The crates take the `hyper-`
  prefix in place of `x-`: `hyper-quic`, `hyper-transport`, `hyper-datagram`, `hyper-swim`,
  `hyper-timing`, `hyper-raft`, `hyper-durable`, `hyper-log`, `hyper-multilog`, `hyper-sim`,
  `hyper-check`, plus the adapter crate `hyper-tokio`.
- Item 5: slates, mantle and focal vendor a snapshot of the shared crates for now. Each snapshot
  records the hyper-raft revision it was taken from, so it can later become a git pin.
- Item 6: the tokio adapter is a separate crate in that repository. The core crates stay free of
  any runtime, and slates never depends on the adapter.
- Item 7: TLC runs in hyper-raft's CI only, never on the owner's machine and never in slates' CI,
  with a fixed worker count and an explicit state and time budget. `x-check`'s exhaustive Rust
  explorer runs beside it, so item 2's fast-track decision has both checks as evidence.
- Item 8: neither option. quinn-proto and rustls are both vendored and conformed to the
  minimal-`Arc`, no-panic rules, removing panics where removal makes sense. No `Arc` exception is
  taken at their configuration signatures. The upstream-merge cost this note names under item 8
  is accepted.
- Item 9: slates has no release (its README says so, and its version is 0.1.0), so no running
  fleet has to survive the change. X-1 and X-2 re-found groups on a fresh start; no in-place
  conversion of on-disk state and no rolling ALPN change is built. The new ALPN is still
  versioned, so later changes can roll.
- Item 10: the shared crates are now maintained across all three projects as one body of work.
  A change lands in hyper-raft only once every consumer's suite passes against it. A consumer
  updates its vendored snapshot in its own gated commit, under that repository's own rules.

**Risks.**

- **Three fast-moving projects behind one gate.** slates' ledger holds 277 bug records; focal
  closes audit findings daily (§2.13's citations). A shared repository whose downstream job runs
  three full suites is slow; on the owner's machine it is also constrained by disk space and by the
  standing limit of two agents and bounded cargo jobs. Mitigation: the downstream job runs in CI,
  not locally; consumers bump on their own schedule.
- **The no-panic rule and quinn-proto.** About 550 textual panic sites (§2.10, DERIVED) sit in a
  dependency that parses peer bytes. The unwind boundary is the fence; the causes have to be closed
  one by one (Q6), and until they are, a panic closes a connection rather than a process. focal
  already runs quinn-proto without this boundary.
- **AWS-LC for slates.** slates moves from `ring` to AWS-LC, which mantle and focal both vendor.
  aws-lc-sys builds C sources; whether its build needs a tool slates' "non-Rust tooling" rule
  forbids (slates `CLAUDE.md` §2 item 13) is UNVERIFIED here; `ring` also compiles C and assembly,
  so the rule's line is the owner's reading.
- **Divergent requirements that should not be forced together.** slates' data-plane fenced
  registers are not Raft and stay in slates (banned item 10: no consensus per write). focal's
  domain envelopes, its parallel materializer and its ranges stay in focal. mantle's engine, range
  split and merge, chunk paths and HTTP/1.1 listener stay in mantle. MLRaft moves as a layer but is
  enabled by no one (R25). Class sets differ by project and stay a type parameter (§3.4). The
  durability medium differs and stays behind `LogStore` (§3.8). A shared crate that encoded any of
  these would carry a mode switch for each.
- **Licensing.** All three are MIT, "Copyright (c) 2026 Hyperlight" (each `LICENSE`); quinn-proto
  is MIT or Apache-2.0; AWS-LC is already vendored by two of the three. No conflict was found;
  `cargo deny check licenses` in the shared repository is the gate.
- **Stale sibling documents.** focal 27 §2 and §8.2 understate slates' core; note 07 §10.9 lists
  further drift; several slates module headers still call built work owed (audit §11.8). The
  migration's evidence is the code and the ledgers, never a header.

---

## 7. Stepped complexity

| Step | What it newly needs from the shared crates | What it does not yet need |
|---|---|---|
| One laptop | `x-raft` and `x-durable` at one voter (the same code at `f = 0`, slates rule R8); `x-log`; `x-transport` for local clients (loopback path skips nothing but the socket) | `x-datagram` and `x-swim` (one member); congestion patches beyond safety on loopback |
| A cell | `x-datagram` for control; `x-swim`; PMTU to the interface MTU (Q4); derived election timing | adaptive reordering beyond the defaults' safety (it moves only after a spurious loss, T27) |
| A region over WAN | Q1–Q3; pipelining windows from tails (R16); priority by measured quorum round trip (R10) | the fast track |
| The global fleet | the full grid; election timing that stretches with tails; the fast track only where an owner's application proof and the crossover favour it (§3.8) | — |

---

## 8. What remains unknown

- Whether quinn-proto 0.11.18 already handles T2, T18, T19 and T21 as slates' fixes require; each
  is a test before it is a patch.
- Whether focal-raft's commit rule scales with the backlog (R21) and whether its pre-vote lease has
  slates' yielding-voter defect (R4); both are slates tests to run on it.
- Whether the core resumes cleanly after a `LogTemporarilyUnavailable` fetch (§3.8).
- The fast-track and estimator decisions (§6 items 2–3) and their runs.
- Whether aws-lc-sys's build needs a tool slates' rules forbid.
- The cost of the downstream job in CI time.
