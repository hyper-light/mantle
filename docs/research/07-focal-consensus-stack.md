# 07 — focal's consensus stack: what mantle can take, and what it would cost

Research note for mantle's range-partitioned, multi-Raft metadata layer. Source:
`/Users/adalundhe/Projects/focal` (read only; nothing was built or run except
`cargo metadata --no-deps`). Every path below is relative to that repository unless it
starts with `/`. Line numbers are those of the working tree on 2026-09-28.

<!-- SUMMARY -->

## 0. Provenance: which revision to pin

| Fact | Evidence |
|---|---|
| The checked-out branch is `r10-r11-windows-ci`, HEAD `bd5e54b18df87ceda2bc4b548f0ecf11ad10a5b9` (2026-09-28), pushed as `origin/r10-r11-windows-ci` | `git rev-parse HEAD`, `git branch -a` |
| HEAD is **106 commits ahead of `origin/main`** (`626b1b5`, 2026-09-10). `focal-raft`, `focal-timing` and `focal-platform` do not exist on `main` at all | `git ls-tree origin/main crates/` vs `git ls-tree HEAD crates/` |
| The Copa congestion controller is **uncommitted**: `crates/focal-wire/src/congestion.rs` and `crates/focal-wire/tests/congestion.rs` are untracked, and `focal-wire/{Cargo.toml,src/lib.rs,src/message.rs,src/transport.rs,src/tests.rs}` carry uncommitted edits | `git status --short` |
| raft-rs types come from an unmerged upstream PR head, pinned by `rev` | `Cargo.toml:19-23`, `docs/dependencies/raft-upstream.md:5-11` |

Consequence: a mantle git dependency must pin `rev = "bd5e54b…"` on that branch (not
`main`), and cannot get Copa until the owner commits it. focal's own `deny.toml` would
also have to be taught the extra git source (section 8).

---

## 1. focal-raft — the sans-io core

### 1.1 Shape

- A state machine "with no clock, no disk and no network": told time (`RawNode::tick`)
  and arrivals (`RawNode::step`); says what to persist, send and apply
  (`RawNode::ready`) (`crates/focal-raft/src/lib.rs:13-27`, `README.md:3-6`).
- Dependencies: `raft-proto` and `thiserror` only; `raft` + `slog` are
  **dev**-dependencies used by the differential tests (`crates/focal-raft/Cargo.toml:8-16`).
  No workspace-internal dependency at all.
- Rules: nothing unwinds; every growing structure has a bound (`Limits`,
  `MAX_MEMBERS = 1024`); a run is its seed (`README.md:19-25`, `src/lib.rs:50-53`).
- Size: ~7.3k lines of source, ~2.4k lines of integration tests and harness.

### 1.2 Public API

Re-exports (`src/lib.rs:42-53`): `Change, Changed, Configuration, ConfigurationError`,
`Error, Result, StorageError`, `LightReady, RawNode, Ready, SnapshotStatus`,
`Quorum, Tally`, `Config, FastStats, Limits, Precedence, Raft, SoftState, StateRole`,
`ReadState`, `InitialState, Storage`, `type NodeId = u64`, `MAX_MEMBERS = 1024`.
Public modules: `configuration, error, fast, log, node, progress, proto, quorum, raft,
read, storage`; `track` is private (`src/lib.rs:29-40`).

**Storage trait** — read-only view of what is durable; the core never writes
(`src/storage.rs:1-36`):

```rust
pub struct InitialState { pub hard_state: HardState, pub configuration: ConfState,
                          pub proposals: Vec<Entry> /* fast-track self-approved entries */ }
pub trait Storage {
    fn initial_state(&self) -> Result<InitialState, StorageError>;
    fn entries(&self, low: u64, high: u64, max_bytes: u64, into: &mut Vec<Entry>) -> Result<(), StorageError>;
    fn term(&self, index: u64) -> Result<u64, StorageError>;
    fn first_index(&self) -> Result<u64, StorageError>;
    fn last_index(&self) -> Result<u64, StorageError>;
    fn snapshot(&self, request_index: u64, to: u64) -> Result<Snapshot, StorageError>;
}
```

**`RawNode<S: Storage>`** (`src/node.rs:189-544`), one `Ready` outstanding at a time:

| Input | Signature | Line |
|---|---|---|
| construct | `new(&Config, S) -> Result<Self>` | 201 |
| time | `tick() -> Result<bool>`; `ping()` (leader heartbeats now) | 237, 326 |
| network | `step(Message) -> Result<()>` — refuses local kinds (`StepLocalMessage`) and answers from non-members (`StepPeerNotFound`); fast-track kinds bypass the enum read | 310-324 |
| proposals | `propose(context, data)`, `propose_fast(context, data) -> Result<u64>`, `propose_conf_change(context, &ConfChangeV2)` | 243, 261, 264 |
| membership apply | `apply_conf_change(&ConfChangeV2) -> Result<ConfState>`, `apply_conf_change_v1(&ConfChange)` | 287, 295 |
| elections / leadership | `campaign()`, `transfer_leader(NodeId)`, `set_priority(i64)` | 240, 353, 234 |
| reads | `read_index(context)` → answer arrives in a later `Ready::read_states` | 360-374 |
| feedback | `report_unreachable(NodeId)`, `report_snapshot(NodeId, SnapshotStatus)`, `request_snapshot()` | 338-351 |
| output | `has_ready()`, `ready() -> Result<Ready>` | 394, 413 |
| persistence ack | `advance_append(Ready) -> Result<LightReady>`, `advance_apply_to(u64)`, `advance(Ready)` | 483, 527, 534 |

While a `Ready` is out, every mutating call returns `Error::Invariant("an operation while
a ready is out")` (`src/node.rs:225-233`).

**`Ready`** (`src/node.rs:78-179`) is how persistence is requested:
`hard_state()` (persist when changed), `entries()` (persist, *replacing* storage from the
first of them), `snapshot()` (persist before the entries), `proposals()` (fast track:
persist beside the log), `displaced()` (fast-track entries that lost their index),
`read_states()`, `committed_entries()`, `messages()` (send now — leader only),
`persisted_messages()` (send only after this Ready is durable — every non-leader;
`after_persisting = state != Leader`, line 478), and `must_sync()` (false when only the
commit moved). This is Ongaro §10.2.1: a leader may send before its own write
(`src/node.rs:5-8`). **`LightReady`** (`src/node.rs:52-76`) carries what follows
`advance_append`: a moved `commit_index`, more committed entries, more messages.

**`Config`** (`src/raft.rs:89-187`): `id, election_tick (20), heartbeat_tick (2),
applied, max_size_per_msg, max_inflight_msgs (256), max_uncommitted_size (u64::MAX),
max_committed_size_per_ready, check_quorum (false), pre_vote (false), priority,
precedence (Log), fast (false), skip_bcast_commit, seed (= id), limits`. `validate()`
refuses zero ids, `election_tick <= heartbeat_tick`, zero windows and zero bounds.

**`Limits`** (`src/raft.rs:31-69`), defaults: `pending_messages 65_536`,
`pending_reads 4_096`, `unstable_entries 65_536`, `entries_per_message 16_384`,
`proposals 256`, `proposal_bytes 8 MiB − 64 KiB`, `fast_window 256`,
`vote_bytes 64 MiB`. Reaching one is `Error::Capacity(..)`.

**Errors** (`src/error.rs:1-57`): three classes — a *refusal* changed nothing
(`ProposalDropped`, `NotPromotable`, `Capacity`, `Settings`, `Configuration`,
`StepLocalMessage`, `StepPeerNotFound`, `RequestSnapshotDropped`, `Storage(..)`); a
*violation* is a peer message contradicting local state (`Violation`), dropped; a
*fatal* error (`Invariant`, `Memory`; `Error::is_fatal`, line 53) means the replica must
stop and be reopened from disk. `StorageError` = `Compacted | Unavailable |
SnapshotTemporarilyUnavailable | LogTemporarilyUnavailable | Other`.

### 1.3 Features and where they live

| Feature | Present | Where |
|---|---|---|
| Pre-vote | yes (`Config::pre_vote`) | `src/raft.rs:1113-1147` (`campaign`), `1264-1267` (asking moves no term) |
| Check-quorum + leader lease against disruptive candidates | yes | step-down `src/raft.rs:1397-1403`; vote refused while leader heard within election timeout `1249-1263` |
| Election priority | yes, with `Precedence::{Log (default), Length (raft-rs rule)}`; not in force at term 0; never judges a transfer | `src/raft.rs:71-87`, `293-365`, `634-651`; 27 §4.5 table |
| Learners | yes | `src/configuration.rs:1-8`, `Configuration::learns` |
| Joint consensus (ConfChangeV2, auto-leave) | yes; both halves must win | `src/configuration.rs:321-340`, `src/quorum.rs:11-12,78-85`, `src/proto.rs:70-140` |
| Leader transfer | yes (`MsgTransferLeader`, `CAMPAIGN_TRANSFER` context skips pre-vote and the lease); abandoned after one election timeout | `src/raft.rs:1021-1025`, `1184-1213`, `src/proto.rs:21-22` |
| Leader that loses its vote hands over and follows (raft-rs leads on) | yes | 27 §4.5 table; `tests/group.rs:140` |
| Inflight window, probe/replicate/snapshot progress, conflict hints | yes | `src/progress.rs` (`Inflights`, `ProgressState`), `src/raft.rs:1524-1582` |
| ReadIndex (quorum-confirmed, no clock lease) | yes; a leader answers only after committing in its term | `src/read.rs:1-3`, `src/raft.rs:1460-1523` |
| Snapshots (send, restore, request) | yes | `src/raft.rs:371-393`, `1773-1905` |
| Fast track (Fast Raft) | yes, per group | `src/fast.rs`, `src/track.rs` — section 1.4 |
| Proposal forwarding follower→leader | core forwards `MsgPropose`; **the shell refuses it** (section 2.2) | `src/raft.rs:1700-1703`; `focal-consensus/src/lib.rs:921-925` |
| Seeded election randomness | yes (`Config::seed`) | `src/raft.rs:119-120`, `1990-2008` |

### 1.4 The fast track, as exposed

Algorithm (header docs, `src/fast.rs:1-21`, `src/track.rs:1-26`; design 27 §4.6):

1. A non-leader proposes an entry for "the index after what it holds" **to every voter**,
   not to the leader: message type `FAST_PROPOSE = 100` (`src/fast.rs:29-33`,
   `src/track.rs:74-164`). A leader calling `propose_fast` simply proposes classically
   (`src/track.rs:96-107`).
2. A voter that holds nothing at that index holds the entry "approved by itself"
   **beside** the log, never in it (`Proposals`, `src/fast.rs:57-188`), and only once that
   is durable (`Ready::proposals` → `on_persist_proposals`) tells the leader with
   `FAST_VOTE = 101` (`src/track.rs:188-248`).
3. The leader decides each next index in order, taking **the first entry it hears of**
   (from proposer or voter) and stamping it with its own term (`decide`/`take`,
   `src/track.rs:358-410`). It commits by whichever comes first: a **fast quorum**
   holding that entry (`fast_commit`, `src/track.rs:411-452`) or the classic quorum.
4. Fast quorum = ⌈3M/4⌉ computed as `M − ⌊M/4⌋` (`src/quorum.rs:3-9, 31-32`): 3 of 3,
   4 of 5.
5. A new leader recovers, for each index above its log, the entry most held among its
   electors, or a no-op where none is held (`recover`, `src/track.rs:475-524`).
6. Fast commits happen only when no configuration change is pending and the
   configuration is not joint (`src/track.rs:413-421`); configuration entries and empty
   entries never take the fast track (`proposable`, `src/track.rs:37-44`).
7. Bounds: `Limits::proposals`, `proposal_bytes`, `fast_window` (256 above commit),
   `vote_bytes` (`src/raft.rs:46-54`); overflow degrades to the classic track rather
   than failing (`src/track.rs:336-341`).

API surface: `Config::fast` (group-wide, identical at every member);
`RawNode::propose_fast(context, data) -> Result<u64>` (the index proposed for);
`Ready::proposals()` (persist), `Ready::displaced()` (re-propose these);
`InitialState::proposals` (give back on open); `Raft::fast_stats() -> FastStats`
(`proposed, displaced, held, taken, committed, recovered`, `src/raft.rs:189-204`).

Consequences an owner must accept (27 §4.6): the **term of a committed entry is not the
same at every member** in a fast group, so an owner may derive nothing from an entry's
term; a displaced proposal must be re-proposed and the owner must deduplicate retries.
**No production owner in focal uses the fast track today** (27 §4.6, last paragraph;
27 §6 stage E). It helps only non-leader proposers, and with 3 voters one slow voter
closes it (27 §4.3).

### 1.5 The raft-proto dependency

- `focal_raft::proto` re-exports raft-rs's prost-generated `eraftpb` types
  (`ConfChange*`, `ConfState`, `Entry`, `EntryType`, `HardState`, `Message`,
  `MessageType`, `Snapshot`, `SnapshotMetadata`) and `protocompat`
  (`src/proto.rs:1-13`). It keeps raft-rs's wire and log bytes so mixed old/new members
  form one group (27 §4.5).
- Pinned: `raft-proto = { git = "https://github.com/tikv/raft-rs", rev =
  "8e4cef172421bf77b2ae1c26628a9531b0be41f0", default-features = false, features =
  ["prost-codec"] }` (`Cargo.toml:19-23`). That rev is the head of **unmerged** upstream
  PR #578, not a release (`docs/dependencies/raft-upstream.md:5-11`).
- It pulls `prost 0.11.9`, `prost-build`, `protobuf-build 0.15.1` and
  **`protobuf-src` (builds protoc from C++ sources at build time on Unix)**
  (`docs/dependencies/inventory.tsv:102-107`, `raft-upstream.md:13`).
- Generated enum accessors unwind on unknown values a peer can choose; they are banned by
  `clippy.toml` `disallowed-methods` and the core reads enums as `Option`
  (`clippy.toml:1-15`, `src/proto.rs:24-29`). Fast-track kinds 100/101 are raw
  `msg_type` values outside the enum, so a raft-rs member treats them as unknown.

### 1.6 TLA+ model

`docs/models/FastTrack.tla` with `FastTrack.cfg`, `FastTrackFive.cfg` and
`FastTrackWrong.cfg` (the last encodes the rule the core does *not* follow, which the
checker must refuse), checked by `scripts/check-model.sh` in CI (27 §4.4;
`crates/focal-raft/README.md:35`). Details of what is modelled and how it runs are in
section 8.3.

### 1.7 Test harnesses

`tests/support/mod.rs` (1063 lines) + `tests/support/cluster.rs` (518 lines):

- **`Seeded`** — SplitMix64; "the schedule of a run is its seed" (`support/mod.rs:14-38`).
- **`Store(Rc<RefCell<Disk>>)`** — an in-memory disk the harness can crash
  (`Disk::reopen`) and compact (`support/mod.rs:64-178`).
- **`Replica` trait** implemented by **`Old`** (raft-rs driven exactly as focal's shell
  drove it), **`New`** (focal-raft) and **`Either`** (mixed groups); every replica
  reports `Output`/`View` in the same words so they compare for equality
  (`support/mod.rs:1-4, 419-453, 491, 736, 980`). `Settings::{shell, focal, fast}` fix
  the production-like knobs (`election_tick 10, heartbeat 2, max_size_per_msg 4 MiB+1 KiB,
  inflight 128, uncommitted 32 MiB, check_quorum, pre_vote`) (`support/mod.rs:372-417`).
- **`Cluster<R>`** — "a network the schedule owns": deliver, lose, repeat, hold back
  (bounded at `NETWORK = 2048`), block/heal links, restart, compact, change membership,
  transfer, read, set priority (`Op`, `cluster.rs:14-39`); a `Mix` gives percentages
  (`cluster.rs:50-79`). Safety is checked on **every** report: the same entry
  (by content, not term) committed at each index (`chosen`), and at most one leader per
  term (`cluster.rs:166-219`). Liveness: `settles(budget)` heals everything and requires a
  proposal applied by every member (`cluster.rs:456-517`).
- **Differential test** (`tests/differential.rs`): raft-rs and focal-raft on one schedule,
  compared after every step; a failure names seed and step (`differential.rs:1-15`).
  `campaign()` runs `FOCAL_RAFT_SEEDS` (default 96) × `FOCAL_RAFT_STEPS` (4000) from
  `FOCAL_RAFT_SEED` (0), and asserts coverage floors (terms, ≥8 commits per seed, reads,
  transfers, refused votes) so a vacuous schedule fails (`differential.rs:299-340`). A
  15,000-schedule run compared 80.8 M steps equal (27 §6 stage D).
- `tests/group.rs` (7 tests: safety + settling for groups of this core and of both cores;
  the 27 §4.5 decisions), `tests/fast.rs` (9 tests incl. a seeded-schedule campaign with
  fast proposals), `src/tests.rs` (15) and unit tests in `log.rs` (11), `progress.rs` (7),
  `configuration.rs` (5), `quorum.rs` (4), `proto.rs` (4), `fast.rs` (3), `read.rs` (1).
- `benches/replicate.rs` reuses the harness (`#[path = "../tests/support/mod.rs"]`) to
  time ns per entry committed by every member, raft-rs vs focal-raft, in-memory storage and
  a lossless network (`benches/replicate.rs:14-17, 35-61, 68-76`).

---

## 2. focal-consensus — `DurableNode`, the durable shell

### 2.1 Responsibilities

"Disk-durable Raft with an injected clock and transport-independent events"
(`crates/focal-consensus/src/lib.rs:13-20`). The shell owns: persistence of every `Ready`
to the shared WAL before any output is released; the second commit fence; application
checkpoints; decoder fences; the unwind boundary; memory accounting; validation of peer
input before the core; membership-change policy; restore-from-image. It owns **no
clock, no network, no thread, and no inbound queue** (`README.md:9`).

Dependencies: `focal-log`, `focal-memory`, `focal-raft`, `focal-timing` (re-exported as
`focal_consensus::timing`, used only by tests), `getrandom`, `postcard`, `serde`,
`thiserror`; dev: `focal-sim`, `tempfile` (`Cargo.toml:8-24`, `src/lib.rs:29-30`).
**No focal domain crate.**

### 2.2 API

Construction (`src/lib.rs:292-630`): `open(config, dir)`, `open_in(config, dir,
&parent_budget)`, `open_on_wal(config, SharedWal)`, **`open_on_wal_in(config,
SharedWal, &parent_budget)`** (the multi-group constructor), `restore_in` /
`restore_on_wal_in(.., RestoredLog)` (begin an empty logical log from an image; refuses a
populated log, `349-439`).

`NodeConfig` (`src/lib.rs:65-143`): `node_id, cluster_id: [u8;16], group_id: [u8;16],
voters, learners` (bootstrap only), `election_tick (10), heartbeat_tick (2),
max_entry_bytes (4 MiB, ≤ 8 MiB), max_uncommitted_bytes (32 MiB),
max_inflight_messages (128, ≤ 65,536), fast (serde-skipped)`; `single(..)` and
`joining(..)` constructors. The shell **forces `check_quorum: true, pre_vote: true`**,
`max_committed_size_per_ready = 16 MiB`, `max_size_per_msg = max_entry_bytes + 1 KiB`,
and a random election seed from `getrandom` (`src/lib.rs:559-578, 1460-1475`).

| Operation | Signature | Line |
|---|---|---|
| propose (leader only; else `NotLeader{leader}`) | `propose(Vec<u8>)`, `propose_in(data, BudgetLane)`, `propose_borrowed_in(&[u8], lane)` | 638, 677, 687 |
| fast track | `propose_fast(data) -> u64`, `propose_fast_in`, `fast()`, `fast_stats()` | 650-676 |
| peer input | `step(Message)`; **`step_authenticated(peer_node_id, &[u8])`** decodes with bounded prost and binds `message.from` to the authenticated peer | 710, 1011-1034 |
| time | `tick()`, `beat()` (heartbeat now, for a stretched tick period) | 734, 791 |
| reads | `read_index(context)` (leader only, context ≤ 1 KiB, ≤ `max_inflight` outstanding); answer in `NodeEvents::read_states` | 739, 1042-1058 |
| membership | `propose_conf_change(ConfChangeV2)`; `propose_membership(expected, change, context)`; `membership_configuration()` | 744, `membership.rs:172-202` |
| leadership | `campaign()`, `transfer_leader(node)`, `set_priority(i64 ≥ 0)`, `transferring()` | 632, 809, 771, 1228 |
| output | **`try_drain() -> Option<NodeEvents>`**, `drain() -> NodeEvents`, `has_ready()`, `persistence_pending()` | `persistence.rs:36-64` |
| checkpoint | `checkpoint(index, data)`, `begin_checkpoint`, `begin_checkpoint_funded`, `try_finish_checkpoint`, `finish_checkpoint`, `cancel_unadmitted_checkpoint` | 862; `checkpoint.rs:22-207` |
| status | `status()`, `peer_progress()`, `peer(node)`, `has_committed_current_term()`, `published_term(index)`, `snapshot_index()`, `failed()` | 1163-1255, 868, 1412 |
| disk | `disk_available_bytes()`, `disk_budget()`, `shared_wal()` | 1154-1162; `persistence.rs:33` |
| tests | `inject_fault_once(FaultPoint)`, `set_randomized_election_timeout` (feature `test-support`) | 1319, 814 |

`NodeEvents` (`src/lib.rs:210-229`): `messages`, `committed: Vec<CommittedEntry{index,
term, data}>`, `membership: Vec<AppliedMembership>` (before/after configurations +
caller context), `read_states: Vec<ReadBarrier{index, context}>`, `displaced`,
`snapshot: Option<AppliedSnapshot>`, `applied_index`, plus an owned memory permit
(`take_allocation()`).

`ConsensusError` (`src/lib.rs:145-181`): `Log, Raft, Protobuf, Encoding, Configuration,
Corruption, NotLeader{leader}, Capacity, Failed, PersistencePending,
DecoderUnconfirmed, DecoderMismatch, DependencyFailure, CheckpointIndex, LearnerBehind,
MalformedMessage, LeaderLeaving`. `PersistencePending` and `Capacity` are retryable;
`Failed`/`DependencyFailure` mean reopen.

Policy the shell adds over the core: peer `MsgPropose` is refused ("proposals must enter
through the leader's application admission", `src/lib.rs:921-925`); appended sequences,
unknown enums, undecodable changes and `u64::MAX` counters are rejected before the core
(`897-972`); fast messages ≤ 256 entries, normal, non-empty (`975-1007`); a leader may not
propose its own removal/demotion (`LeaderLeaving`), a voter is added only once its
learner progress has reached the commit (`LearnerBehind`), joint changes must be entered
and left in separate committed changes (`1059-1109`).

### 2.3 How it persists — the drain state machine

`persistence.rs:1-396`. One outstanding durable `Ready` per group; other groups stay
runnable.

1. `poll_drain` reserves staging memory (a refusal leaves the replica untouched and is
   retryable) and creates a `PendingDrain` (`persistence.rs:66-118`).
2. `Phase::Start`: `raw.ready()`; encode `Snapshot`, each `Entry`, each fast-track
   `Proposal`, and the `HardState` as `focal_log::Record`s of the group's
   `LogicalLogId(group_id)`; `validate_append`; **prepare** storage (reserve replacement
   entries/snapshot in `RamLog`) without publishing (`persistence.rs:161-224`).
3. `Phase::Ready`: `WalLease::append_async_in(records, Completion)` returns a
   `WalAppend` receipt; `try_drain` returns `None` here so the owner can go queue other
   groups (`225-247`). When the receipt completes: `publish(prepared)`, record hard state,
   then release **both** `messages()` and `persisted_messages()` — i.e. the shell releases
   even a leader's appends only after the leader's own fsync (`265-316`).
4. `advance_append` → `LightReady`. If the commit moved, a **second WAL write** of the
   hard state is queued (`Phase::Light`), and nothing — messages, read barriers,
   committed entries, applied index — is released until that fence is durable
   (`317-381`; `README.md:11`).
5. When `has_ready()` is false the complete durable prefix is returned as `NodeEvents`
   (`137-160`).

While any of this is pending, every mutation — `step`, `tick`, `propose`, `read_index`,
membership, transfer, checkpoint — returns retryable `PersistencePending`; the **host**
must retain or requeue its input (`README.md:9`, `persistence_tests.rs:138-180`).

### 2.4 Checkpoints and snapshots

- The in-memory log is `RamLog`: a `VecDeque<Entry>` of every retained entry, each with
  its memory charge, plus hard state, configuration, snapshot and held proposals
  (`storage.rs:10-36`). **The whole retained Raft log lives in RAM** and is rebuilt from
  the WAL on open. It shrinks only when the application checkpoints.
- `begin_checkpoint(index, data)` requires `index == delivered_index` (the published
  prefix), no outstanding Ready, and **`data.len() ≤ 8 MiB`**
  (`checkpoint.rs:58-72`). It builds identity + fast-track + decoder records + a
  `Snapshot{index, term, conf_state, data}` + every retained entry and proposal above the
  index + the hard state, and submits them as a WAL **rewrite**
  (`rewrite_checkpoint_async_in`, `checkpoint.rs:90-164, 226-270`). Only after that
  fence is durable is `RamLog` compacted.
- The Raft snapshot sent to a lagging peer is that same application checkpoint, as one
  `MsgSnapshot`; inbound snapshots are capped at 8 MiB of data and 9 MiB of message
  (`src/lib.rs:904-912`, `1648-1662`). Restore images are also ≤ 8 MiB (`359`).
- "This component does not autonomously checkpoint the application"; application limits
  and checkpoint policy must bound retained history (`README.md:5`).

### 2.5 Decoder fences

An irreversible per-group **decoder floor** (32-byte fingerprint of the application's
compiled decoder) and at most one registered successor, persisted as WAL records
`DecoderFloor` ("FOCALDF1") and `DecoderTransition` ("FOCALDT1", 74 bytes)
(`decoder.rs:1-12`). `confirm_decoder(hash)` / `confirm_decoder_pair(..)` register what
the binary can decode; `begin_decoder_floor(hash)` / `begin_decoder_transition()` stage
the write; `try_finish_decoder_floor`/`try_drain` await the fsync; only
`decoder_floor_ready(hash)` authorizes advertising the capability
(`decoder.rs:36-215`). A recovered floor blocks elections, votes, peer processing,
reads, proposals and replay output until the application confirms exactly that
fingerprint (`README.md:25`). Because new `RecordKind` ordinals are appended, an older
binary refuses the entire physical WAL (`focal-log/src/lib.rs:56-69`,
`focal-log/README.md:74-81`). The fast-track flag uses the same mechanism
(`RecordKind::FastTrack`, payload `FOCALFT1`, `src/lib.rs:1485-1494`).

### 2.6 The unwind boundary — `guarded_in`

`src/lib.rs:1322-1399`. Every Raft-participating entry point runs through
`guarded_in(incoming, new_members, lane, op)`: check not failed / decoder confirmed /
no membership rebuild / no persistence pending; reserve staging memory; run `op` inside
`catch_unwind(AssertUnwindSafe(..))`; on a fatal core error or any unwind set
`failed = true` and return `DependencyFailure`; otherwise shrink the reservation to what
the core retains. Construction (`RawNode::new`, line 596), `poll_drain`
(`persistence.rs:119-130`) and checkpoints (`checkpoint.rs:80, 213`) are wrapped the same
way. It is "the last fence, never the fix": it requires `panic = "unwind"` and cannot
contain aborts or allocator OOM (`SECURITY.md:16-26`, `CLAUDE.md` rule 1).

### 2.7 Memory accounting

- Each group gets a child `MemoryBudget` of **512 MiB with a 128 MiB completion
  reserve** under the caller's parent — ceilings, not preallocation; a quiet
  single-voter group accounts ≈ 12 KiB (`src/lib.rs:457-459`, `README.md:3-5`).
- Every retained payload and vector capacity carries an owned `Allocation`
  (`storage.rs:10-26`); `NodeEvents` carries its output permit, which must move with its
  buffers (`README.md:13`).
- Before each operation the shell reserves conservative headroom
  (`memory::staging_bytes`, `memory.rs:128-150`): 2 × retained history +
  (batch + snapshot) × (members + 2) + incoming × (members + 8) + 6 × core-resident
  bytes + members × (8 × inflight + 4 KiB) + 64 KiB. This is proportional to the retained
  log, so groups that checkpoint rarely make every `tick` reserve a lot.
- Ordinary vs completion lanes (`BudgetLane`): ordinary admission for new proposals;
  completion for Ready persistence, peer processing and output, so admitted work can
  finish under pressure (`README.md:5`).

### 2.8 Many groups on one WAL and one pool

- A node opens **one** `SharedWal` (e.g. `<root>/wal` in `crates/focal-node/src/network_service.rs:451-461`) and
  every group calls `open_on_wal_in(config, wal.clone(), &budget)`, which takes an
  exclusive `WalLease` on `LogicalLogId(group_id)` (`src/lib.rs:472`; a second instance
  of the same group gets `LogicalLocked`, `focal-log/src/writer.rs:953-955`).
- On open, only that group's frames are replayed, via the writer's in-memory frame index
  (`focal-log/README.md:54-60`, `writer.rs:1181-1200`).
- The single disk-writer thread coalesces queued appends from all groups into one
  covering fsync; `single_owner_queues_many_groups_into_one_covering_flush` proves 12
  groups → one group commit (`persistence_tests.rs:138-200`, assertion at 184).
- Production owners: the root/partition **control groups** (`ControlReplica`,
  `crates/focal-control/src/replica.rs:168-178`) and **session ledgers**
  (`crates/focal-ledger/src/session.rs:222-376`). Session replicas either get a thread each
  (`focal-replica-{node}`, `crates/focal-node/src/fleet.rs:652-655`) or share one worker
  with tenant-fair scheduling (`focal-node-sessions-{node}`,
  `crates/focal-node/src/fleet_group.rs:1-3, 265-269`, using
  `focal_directory::FairScheduler`). 27 §1 states "root, partition and session groups share
  one WAL and one pool; no leader balancing" (the balancer has since been built in
  focal-node/focal-directory, 27 §5, §6 stage F).
- The connection pool is `focal-wire`'s (section 4).

### 2.9 How a state machine plugs in, and domain coupling

- **There is no state-machine trait.** The only trait in the three crates is
  `focal_raft::Storage`. An application *owns* a `DurableNode` and drives it:
  propose bytes → `drain()`/`try_drain()` → apply `committed` / `membership` /
  `snapshot` → publish → `checkpoint(applied_index, bytes)`. Examples:
  `ControlReplica` (`focal-control/src/replica.rs:534-1124`) and `Session`
  (`focal-ledger/src/session.rs:852, 970, 991`).
- Domain coupling of focal-consensus itself is **cosmetic**: comments and one error
  string mention sessions (`src/lib.rs:220` "never a SessionSeq", `1592` "unexpected
  session Raft record", `1158-1159`), and `RestoredLog` follows doc 26's custody design.
  Types are generic: `[u8;16]` cluster/group ids, `u64` node ids, `Vec<u8>` payloads.
- Coupling that *does* matter is in hard-coded policy: 8 MiB entries/snapshots, 9 MiB
  messages, 16 MiB committed per ready, 512 MiB/128 MiB per-group budget, forced
  pre-vote/check-quorum, ≤ 1 KiB read contexts, 1024 members, leader-only proposals.

### 2.10 Fit for a mantle KV-range state machine

Usable generically — nothing in its API is focal-domain — but these properties would
shape mantle's design:

| Property | Effect on a range-partitioned KV metadata store |
|---|---|
| Snapshot = application checkpoint bytes, ≤ 8 MiB, sent as one `MsgSnapshot` | A range's state cannot be snapshotted through Raft unless ranges stay tiny. mantle needs out-of-band range state transfer (checkpoint = small manifest of an LSM/SST set) and a patch to DurableNode for "external" snapshots |
| Whole retained log in RAM (`RamLog`) until checkpoint | Frequent small checkpoints per range, or a patch that reads cold entries from the WAL |
| Every checkpoint rewrites the **entire physical WAL** (all groups) — section 3.5 | With thousands of ranges per node, checkpoint write amplification is O(groups × WAL); this is the main multi-Raft scaling gap |
| Two WAL fences per commit (entries, then commit hard state), each = segment fsync + fence-file fsync + rename + directory fsync | Commit latency ≈ 2 × (3 flushes) on the leader plus follower flushes; group commit amortizes across ranges but not within one write |
| Leader appends released only after the leader's fsync | No leader-write/replicate overlap; a latency cost on every write |
| `PersistencePending` gate; no inbound queue | mantle's per-range router must buffer (bounded) and retry inbound messages and client proposals |
| Leader-only proposals; peer `MsgPropose` refused | mantle must route writes to range leaders (or use the fast track from followers) |
| `try_drain` is non-blocking and per group | Fits a scheduler that drives many ranges from a small thread pool; focal's production owners mostly use blocking `drain` on dedicated threads |
| Per-group identity/decoder/fast records, `LogicalLogId` = 16-byte group id, ≤ 65,536 logical logs per WAL | Range ids fit; a split creates a new logical log (restore image path exists: `restore_on_wal_in`) |
| No split/merge support in consensus | Range split/merge (new group bootstrap from parent state, key-span fencing) must be built in mantle |

---

## 3. focal-log — the shared physical WAL

### 3.1 On-disk format

Directory (`crates/focal-log/src/lib.rs:170-266`): `LOCK` (exclusive OS lock via
`focal_platform::try_lock_exclusive` → std `File::try_lock`), `CURRENT` (durability
fence), `CURRENT.tmp`, `INITIALIZED` (sentinel: a missing fence after first init is
corruption, not a fresh log), and segments `wal-{generation:020}-{segment:020}.seg`
(`535-537`).

- **Segment header, 72 bytes** (`create_segment`, `572-601`): `"FOCALW01"`, version
  `u32 = 1`, cluster `[u8;16]`, node `u64`, stream `u32`, generation `u64`, segment `u64`,
  starting sequence `u64`, previous frame CRC `u32`, CRC32 of the preceding 68 bytes.
  All little-endian. Segments roll at `segment_bytes` (default 64 MiB), fsyncing the full
  one first (`454-475`).
- **Frame, 20-byte header + payload** (`476-513`): `len u32`, `sequence u64`
  (monotonic across the generation), `previous CRC u32`, `CRC32(header[0..16] ‖ payload)`.
  Each CRC covers the previous frame's CRC, so frames form a **CRC chain** across segments.
  Checksum: `crc32fast` (CRC-32/IEEE).
- **Payload** = postcard-encoded `Record { log: LogicalLogId([u8;16]), kind: RecordKind,
  index: u64, term: u64, payload: Vec<u8> }` (`38-79`). Kinds, append-only ordinals:
  `Entry, HardState, Configuration, Snapshot, Identity, Checkpoint, DecoderFloor(6),
  DecoderTransition(7), FastTrack(8), Proposal(9)` (`48-70`). Raft payloads inside are
  protobuf (`focal-consensus/src/lib.rs:1445-1459`).
- **Fence** (`install_fence`/`read_fence`, `603-657`): `"FOCALF01"` + postcard
  `Fence{version: 1, identity: WalIdentity, position: DurablePosition{generation, segment,
  byte, sequence, checksum}}` + CRC32, ≤ 1 KiB.
- Limits (`WalOptions::new`, `81-98`): record ≤ 16 MiB, batch ≤ 64 MiB, segment 64 MiB.

### 3.2 Write path and group commit

- `SharedWal::open` starts **one bounded disk-owner thread** (2 MiB stack) fed by a
  `std::sync::mpsc::sync_channel` (`writer.rs:18-20, 76-111`; `README.md:3-10`).
- Callers submit through a per-group `WalLease`: blocking `append`/`append_in`, or
  `append_async`/`append_async_in` returning a `WalAppend` receipt that is a `Future`,
  pollable with `try_complete()`, or waitable with `wait_blocking()`
  (`writer.rs:113-182, 750-860`). Dropping a receipt cancels interest, not the write.
- **Group commit without a timer**: on waking, the writer takes the first `Append` and
  greedily drains further queued `Append`s up to `max_batch_requests` (64) and
  `max_batch_bytes` (64 MiB), writes all frames, then performs **one** `finish_append`
  (`writer.rs:908-937, 1019-1130`). Defaults: 64 queued data requests + 4 control slots,
  65,536 logical groups (`writer.rs:48-63`; `README.md:27-29`).
- `finish_append` (`lib.rs:517-524`): `active.sync_all()` (segment data) →
  `install_fence` = write `CURRENT.tmp`, `sync_all`, `atomic_replace` to `CURRENT`,
  `sync_dir` → only then are receipts completed. So each group commit is **two file
  flushes + one directory flush + a rename**.
- Admission: memory is reserved from the WAL's budget before enqueue ("RAM pressure
  rejects before enqueue"), and disk bytes from a `DiskBudget` promise that is committed
  once behind the fence (`README.md:27-37`, `writer.rs:1108-1117`).

### 3.3 OS durability primitives

focal-log calls `std::fs::File::sync_all` everywhere (never `sync_data`) and
`focal_platform::sync_dir` for directories:

| OS | File flush (`sync_all`) | Directory flush (`sync_dir`) | Fence rename |
|---|---|---|---|
| Linux | `fsync(2)` | `open(dir)` + `fsync` | `rename(2)` |
| macOS | `fcntl(F_FULLFSYNC)` | `open(dir)` + `F_FULLFSYNC` | `rename(2)` |
| Windows | `FlushFileBuffers` | no-op (a directory handle cannot be flushed) | `MoveFileExW(MOVEFILE_REPLACE_EXISTING \| MOVEFILE_WRITE_THROUGH)` |

Evidence: std 1.94.1 `library/std/src/sys/fs/unix.rs:1381-1393` (`F_FULLFSYNC` under
`target_vendor = "apple"`, else `fsync`) and `sys/fs/windows.rs:400-401`
(`FlushFileBuffers`) in `/Users/adalundhe/.rustup/toolchains/1.94.1-aarch64-apple-darwin/lib/rustlib/src/rust/`;
`crates/focal-platform/src/lib.rs:136-152` (`sync_dir`), `src/fs.rs:257-277`
(`atomic_replace`), `src/windows.rs:324-339` (`MoveFileExW`). No `fdatasync`, no
`O_DIRECT`, no preallocation. The crate states it "does not claim protection against a
drive that lies about flushes" (`lib.rs:21-22`).

### 3.4 Recovery, torn tails, corruption

- Open validates the **entire durable prefix** up to the fence: every segment header,
  every frame's length, sequence, predecessor CRC and CRC, every record's decoding, and
  finally that the last frame matches the fence's sequence and checksum
  (`scan_indexed`, `lib.rs:679-784`).
- **Torn tail**: bytes after `fence.byte` in the active segment are truncated
  (`set_len` + `sync_all`, `lib.rs:249-255`); segments of other generations, or beyond the
  fence's segment, are deleted (`cleanup_segments`, `830-860`). Nothing before the fence
  is ever guessed at (`lib.rs:15-19`).
- **Corruption before the fence is fatal**: `LogError::Corruption{path, offset,
  reason}`; there is no repair or skip (`116-131`). A missing `CURRENT` with
  `INITIALIZED` present is corruption (`214-216`).
- **Any write/flush/fence failure poisons the writer** (`failed = true`) until reopen;
  every receipt in the affected batch fails and admission stops (`lib.rs:293-297`,
  `writer.rs:1120-1128`, `README.md:39-43`). Pre-write per-batch errors fail only that
  batch (`writer.rs:1042-1050`). Fault points `AfterAppend`, `AfterDataSync`,
  `AfterFenceInstall` exist for crash tests (`lib.rs:159-165, 517-524`).

### 3.5 Compaction and GC

- There is no segment-level GC of live data: the only way records leave the WAL is a
  **generation rewrite**. `rewrite_checkpoint` rewrites everything; per group,
  `rewrite_log_checkpoint`/`rewrite_log_encoded` creates generation `g+1`, **streams every
  other group's records forward** from the old generation, writes this group's retained
  records, installs the fence, re-scans, and deletes the old generation
  (`lib.rs:300-394`). The writer reserves disk for "the whole current log" as transient
  headroom (`writer.rs:1131-1172`).
- So **each group's checkpoint costs a copy of the entire physical WAL**. focal lives
  with it because it has few groups per node; mantle, with many ranges per node, would
  need a different compaction design (per-group segment chains, or segment GC by
  per-group low-water marks) before adopting focal-log unchanged at scale.
- Startup cost: the full-prefix validation scan plus a bounded in-memory frame index;
  per-group replay then seeks only that group's frames (`README.md:54-60`).

### 3.6 Numbers that matter for mantle

- Per commit on a leader: Ready fence (entries + hard state) and LightReady fence
  (commit) — up to 2 × (2 `sync_all` + 1 directory sync). On macOS each is an
  `F_FULLFSYNC`.
- One disk thread per `SharedWal`; ≤ 64 requests and ≤ 64 MiB per group commit.
- `benches/append.rs` measures ns/append, records/s and MiB/s for batch × payload in
  {(1,64), (16,64), (256,64), (1,4096), (16,4096)} on the `Wal` directly
  (`benches/append.rs:1-77`); recorded results are in section 9.

---

## 4. focal-wire — QUIC transport (and the UDP layer that is not there)

### 4.0 Committed vs uncommitted

`git diff crates/focal-wire` shows that Copa, `TrafficClass`, per-stream priorities and
`STREAM_WINDOW_CEILING` are **working-tree only** (`src/congestion.rs` and
`tests/congestion.rs` untracked; `Cargo.toml` adds `quinn-proto = "0.11.18"`;
`src/lib.rs`, `message.rs`, `transport.rs`, `tests.rs` modified; 27 §7 and the doc-09 entry
at `09:11057-11210` also uncommitted). At HEAD `bd5e54b` focal runs quinn's default
**CUBIC**, a ~10 MiB stream window, quinn-proto **0.11.17** (which closes connections with
"too many gaps", 27 §7 `:421-429`), and only `Operation::Raft` has elevated priority.

### 4.1 quinn configuration

- quinn 0.11.11, quinn-proto 0.11.18 (working tree), rustls 0.23.45, **ring** provider
  (`crates/focal-wire/src/transport.rs:105, 143`; `Cargo.lock`).
- `pub fn quic_transport(limits: &WireLimits) -> Result<quinn::TransportConfig, WireError>`
  (`transport.rs:64-97`): bidi streams = `streams_per_connection + 2`, no uni streams;
  `stream_receive_window = min(max_frame + 16, STREAM_WINDOW_CEILING)` with
  `STREAM_WINDOW_CEILING = 1 MiB` (`:58, 70-74`); connection windows = stream window ×
  streams; idle timeout `min(request_timeout, 10 s)` (`:44, 88-89`), keep-alive idle/4;
  `congestion_controller_factory(CopaConfig::default())` (`:95`). MTU and datagram
  settings are quinn defaults (initial MTU 1200, PMTUD on).
- Node-to-node limits: 10 MiB frames, 40 MiB cost, 1024 items, 128 connections, 16 streams
  per connection, 30 s timeout (`crates/focal-node/src/fleet.rs:616-622`,
  `control_host.rs:461-467`, `message.rs:765-784`) → 18 bidi streams, 1 MiB stream window,
  18 MiB connection window, 10 s idle, 2.5 s keep-alive.
- **TLS** (`transport.rs:99-157`): TLS 1.3 only; server requires client certificates via
  `WebPkiClientVerifier` against the cluster roots; client verifies server roots and
  `server_name` and presents its certificate; 0-RTT disabled on both sides
  (`max_early_data_size = 0`, `enable_early_data = false`); ALPN `focal/1`
  (`message.rs:12`).
- **Identity**: a cluster CA (`BootstrapAuthority`, rcgen, path length 0) issues node
  certificates from CSRs with client-owned keys; requested subjects/SANs are ignored and
  the SAN is the assigned server name (`crates/focal-enrollment/src/pki.rs:34-200`).
  Enrollment runs on the same endpoint under ALPN `focal-enroll/1`
  (`crates/focal-enrollment/src/lib.rs`; `crates/focal-node/src/network_listener.rs:79-121,
  283-323`).
- **Authorization after the handshake** (`transport.rs:272-299`; `auth.rs:36-192`): the
  leaf certificate's BLAKE3 derive-key fingerprint (`"focal.transport.peer-certificate.v1"`)
  maps through `PeerRegistry` to a `PeerGrant{principal, tenants, role}`; the registry is
  re-checked on every stream so revocation reaches open connections (`transport.rs:369-371`);
  `Operation::Raft` requires `PeerRole::Node`. `DurableNode::step_authenticated` then
  requires the protobuf `from` to equal the certificate's node id
  (`focal-consensus/src/lib.rs:1011-1034`).
- **Admission** (`admission.rs`, 27 P5): `AdmissionLimits::for_connections(128)` → 32
  pending handshakes, 4 connections per node identity, 16 per participant; over the bound,
  the identity's least-recently-used connection is replaced (close code 3) (`:36-52,
  143-242`).
- An application `Hello{versions, max_frame_bytes, max_items}` / `HelloReply` negotiation
  follows the TLS handshake (`transport.rs:311-353, 537-563`). quinn/tokio calls are wrapped
  in `catch_unwind` (`lib.rs:63-89`).

### 4.2 Connection pool

- One `PeerConnectionPool` per node, shared by the root group, directory, **every ledger
  group's replication driver** and the evidence driver
  (`crates/focal-node/src/network_service.rs:645-675, 1389-1414`). A node uses two UDP
  sockets: the listen port (server endpoint) and an ephemeral client endpoint.
- Defaults (`crates/focal-wire/src/peers.rs:48-72`): 4096 routes, 128 connections, 256
  total inflight, **2 inflight per peer (validated to 1..=2)**, 16 probe slots, 2 attempts,
  5 s timeout, 10 ms retry backoff, 2 s unreachable cooldown.
- One connection per target node, shared by all groups; LRU eviction of idle slots only;
  one detached dial per slot racing the announced address and (after 100 ms) up to 4
  resolved addresses; failed dials mark the peer unreachable for the cooldown
  (`peers.rs:146-180, 788-1060`). Routes are replaced by monotonic revision; a changed
  endpoint retires its slot and estimators (`:326-370`).
- Estimators: `PathRtt` fed by answered probes only; `ExchangeRtt` by non-Raft exchanges
  with doubling per abandoned exchange (≤ 6); `round_budget(targets, period)` (`:212-264,
  374-421, 703-756`).
- **No internal queue**: over the permits a send is `Busy` immediately and "Raft must
  retransmit" (`:438-449`). Errors: `PeerSendError{Configuration, InvalidRequest, Busy,
  NoRoute, StaleRoutes, RouteChanged, Lost, Rejected, Closed}` (`:85-104`).

### 4.3 Copa (uncommitted)

`src/congestion.rs` ports slates' `crates/transport/src/congestion/copa.rs` (doc
`:1-25`): target rate 1/(δ·d_q) with d_q = RTTstanding − RTTmin (RTTstanding = min over
srtt/2, RTTmin = min over 10 s); window moves v/(δ·cwnd) per ACK, velocity doubles after
3 RTTs in one direction; slow start until first decrease; competitive mode (1/δ +1 per RTT,
halved on loss) when the queue has not nearly emptied over 4·srtt; loss otherwise not a
signal; persistent congestion resets to 2 datagrams; initial window 10 datagrams;
`DEFAULT_INV_DELTA = 2` (δ = ½) (`:33-46`). Plugged in as
`impl quinn::congestion::ControllerFactory for CopaConfig { inv_delta }` (`:397-487`);
pacing stays quinn's. 9 unit tests; `tests/congestion.rs` (6 tests) runs two sans-io
quinn-proto endpoints over `focal_sim::path` with a 1-BDP drop-tail bottleneck, comparing
NewReno/CUBIC/BBR/Copa by a selection rule fixed before any run (`tests/congestion.rs:10-36,
225-260, 645-708, 906-957`). Results in section 9 / 27 §7: Copa geomean p99 1.130× best
and 0.987 of best throughput, never stalls; CUBIC 1.193× / 0.455, stalls on 5 paths.

### 4.4 Traffic classes, windows, framing

- `TrafficClass { Bulk = -10, Exchange = 0, Control = 10 }` → `SendStream::set_priority`
  on both sides (`message.rs:231-250`; `transport.rs:373, 632`). Control: Raft, Probe,
  Control, PeerControl, NodeContact, EnrollmentControl, ManagedSupport, PlacementControl,
  SessionSign, RangeControl, SessionControl; Bulk: Upload, Download, Custody; the rest
  Exchange (`message.rs:308-341`). **Priorities on one connection per peer pair, not
  separate connections.** `QuicRemote` also reserves a 2-stream control lane for
  `Operation::Raft` (`transport.rs:580-583, 621-626`).
- `STREAM_WINDOW_CEILING = 1 MiB` exists because quinn closes a stream holding > 1,024
  unmerged spans and a 10 MiB window overshoots badly on loss; its cost is that one stream
  carries ≤ 1 MiB per RTT (73% of 100 Mbit/s at 100 ms, 25% at 300 ms)
  (`transport.rs:45-58`; 27 §7 `:421-435`).
- Frame (`frame.rs`): 16-byte header — magic `FOCALQ01`, u16 version, u16 kind
  (Hello/HelloReply/Request/Response), u32 big-endian length — then a postcard payload;
  hard cap 16 MiB, negotiated limit checked before allocating; **no checksum** (QUIC AEAD
  provides integrity); one request and one response per bidi stream, `require_end`
  rejects trailing bytes (`frame.rs:5-7, 59-73, 116-173, 204-210`).
- `RequestEnvelope{protocol, ledger: LedgerId, route_epoch, request_epoch, request_id,
  operation}` (postcard, `deny_unknown_fields`, `message.rs:365-372`);
  **`Operation::Raft { group: [u8; 16], message: Vec<u8> }`** (`message.rs:96-99`, tag 4 at
  `:272`) is how a Raft message is addressed to a group. Dispatch: `RequestHandler`
  (`handler.rs:52-72, 121-238`).

### 4.5 `gather` (fan-out)

`pub async fn gather<T, F>(asks: impl IntoIterator<Item = (u64, F)>, budget: RoundBudget,
enough: impl FnMut(u64, T) -> bool) -> Result<Round, RoundError> where F: Future<Output =
Option<T>>` (`round.rs:49-109`), ≤ `MAX_ROUND_PEERS = 1024` (`:18`). A biased `select!`
consumes replies in hand before `RoundWait::judge`; the round ends `Enough`,
`AllReported` or `Expired` (on a stall past the lookahead, with bounded extensions while
answers arrive); outstanding exchanges are dropped. **It is used for placement
signature quorums** (`crates/focal-node/src/placement_agent.rs:2671-2699`), **not for Raft
replication**.

### 4.6 How Raft messages travel today

- Egress (`crates/focal-node/src/fleet.rs:2830-2877`): each `eraftpb::Message` is protobuf
  encoded into `Operation::Raft{group, message}` inside a `RequestEnvelope`, charged to the
  budget and `try_send`-ed onto a bounded queue — **dropped if the queue is full**;
  messages over `max_frame − 128` (large snapshots included) are dropped and the snapshot
  reported failed ("require the chunked snapshot transport"). A replication task sends
  with up to 256 concurrent `pool.send` calls (`crates/focal-node/src/replication.rs:111-151`).
- On the wire: **one QUIC bidi stream per Raft message**, no batching across messages or
  groups, all groups sharing one connection, **2 in flight per peer** and the 2-stream
  Raft lane.
- Ingress: `DataService` routes by `group` to the root owner, a directory host, or the
  ledger fleet (`network_service.rs:207-260`); the fleet checks the sender is a member or
  admitted node (`fleet.rs:2035-2045`); `step_authenticated` binds sender to certificate;
  the `PeerAccepted` reply is held until the receiver has persisted the resulting Ready
  (`fleet.rs:2044-2047, 2897`; `control_host.rs:1134-1146`).
- Consequence for multi-Raft: across *all* groups a peer gets about 2 Raft messages per
  (RTT + remote persist); more are `Busy`-dropped and left to Raft retransmission. This is
  fine for focal's handful of groups and would throttle thousands of ranges.

### 4.7 The "additional UDP connection layer for FastRaft" — **not in focal**

- focal has **no** separate UDP layer and **no QUIC datagram use**: no `send_datagram`,
  `read_datagram` or datagram configuration anywhere; the only UDP sockets are quinn's two
  endpoint sockets (`crates/focal-node/src/network_listener.rs:15-47, 174-184`,
  `network_service.rs:548-554, 645`); `docs/network-startup.md:24` describes "one UDP/QUIC
  port for authenticated peer traffic and invitation redemption".
- Fast Raft messages (`FAST_PROPOSE = 100`, `FAST_VOTE = 101`) are ordinary
  `eraftpb::Message`s in `Operation::Raft` on QUIC streams at Control priority; there is no
  fast-track-specific path, and no focal-node owner sets `NodeConfig::fast`
  (`focal-raft/src/fast.rs:29-33`; `focal-consensus/src/lib.rs:913-917`).
- **Where the idea lives.** The hecate reference spec vendored in focal:
  `docs/archictecutre/reference/hecate/docs/specs/PROTOCOL.md:17-50` — "There is no TCP
  anywhere in the mesh … QUIC + UDP over TCP" (i.e. QUIC and UDP *instead of* TCP), with
  two planes: **§1.1 a control plane of stateless, header-encrypted bare-UDP datagrams**
  ("Raft rides this plane — protocol-sound because Raft is loss-tolerant … nothing here is
  ever retransmitted by the transport"; cleartext prologue `sender_id ‖ key_epoch`, then
  AES-256-GCM over the full envelope) and **§1.2 a reliable QUIC session plane** for
  everything else. slates adopts the same two planes in
  `/Users/adalundhe/Projects/slates/docs/wip/fleet-transport.md:50-60`.
- **What exists in slates.** `crates/transport` implements the control-datagram codec
  (`ControlDatagram { sender, key_epoch, envelope, body }`, `src/lib.rs:87-100`), the seal
  (AES-256-GCM, prologue as AAD, strictly increasing counter nonces, `src/seal.rs`), a
  per-(sender, epoch, direction) HKDF key schedule (`src/schedule.rs`), an acceptance order
  (length → prologue → keyring → AEAD → replay; `src/accept.rs`) and a membership keyring
  (`src/enrollment.rs`). It is exercised only by `tests/plane.rs` and `tests/hostile.rs`;
  **no crate outside `crates/transport` references `ControlDatagram` or the seal**
  (grep of `crates/server`, `crates/cluster`, `crates/client`). In production slates
  carries its config-group Raft over its own QUIC dialect on a record plane, at Control
  priority (`crates/server/src/fleet.rs:4282-4300`, `crates/cluster/src/raft_wire.rs:396-432`),
  and it has no fast track (focal 27 §2).
- focal explicitly chose **not** to port slates' transport protocol (27 §3.2, "Leave":
  one exchange per session, IPv4 only, no idle timeout/close/reset/migration/key update,
  several packet copies — "quinn covers all of these").
- Net: the owner's two-plane design (reliable QUIC for stateful transfers, a separate UDP
  datagram plane for Fast Raft) is **specified (hecate §1.1) and partly coded (slates'
  unwired codec) but not built anywhere**. mantle would be the first implementation. Note
  also that the owner's phrase "QUIC over TCP" matches the spec's "QUIC + UDP over TCP" —
  a preference, not layering; QUIC runs over UDP.

### 4.8 Coupling

`focal-wire` depends on `focal-model` and `focal-stream` (domain), `focal-memory`,
`focal-timing`, `focal-platform`; dev `focal-sim` (→ `focal-model`) (`cargo metadata`).
Envelope/response/error types are domain-bound (`LedgerId`, `Command`, `Claim`,
`SessionSeq`). Domain modules: `native`, `validators`, `reconcile`, `list`, `traversal`,
`selection`, `peer_mutations`, `managed`, `monitor`, `summary`, most of `message` and
`auth`'s capability checks. Generic pieces worth extracting: `frame.rs` and
`congestion.rs` (no focal deps), `round.rs` (needs focal-timing), `admission.rs` (swap the
principal/role types), the config builders in `transport.rs`, `QuicServer`/`QuicConnector`/
`QuicRemote` made generic over an envelope, `PeerRegistry` + `certificate_fingerprint`,
the `peers.rs` pool logic, and the `tests/congestion.rs` harness — roughly 3–3.5k lines.

---

## 5. focal-timing, focal-sim, focal-memory

### 5.1 focal-timing — derived pace and progress-charged waits

No dependencies at all, std only (`crates/focal-timing/Cargo.toml:1-9`). Nothing in it
reads a clock; callers pass time in (`src/round.rs:16-17`). Constants:
`ELECTION_MARGIN = 10`, `GRANULARITY_NS = 1_000_000` (RFC 9002 kGranularity),
`PATH_WINDOW = 16` (`src/lib.rs:45-53`).

| Type | What it is | API / formula | Where |
|---|---|---|---|
| `PathRtt` | per-path RTT estimate from **answered liveness probes**; median/MAD of the last 16 samples (not EWMA: fewer than 8 late samples cannot move it) | `on_sample(ns)`, `smoothed_ns()` = median, `variation_ns()` = MAD, `tail_ns()` = median + max(4·MAD, 1 ms) | `src/lib.rs:55-157` (module doc at `:9-11` still calls it RFC 9002 — stale) |
| `ExchangeRtt` | RFC 9002 / RFC 6298 smoother for request/response exchanges | srtt ← ⅞srtt + ⅛rtt, rttvar ← ¾rttvar + ¼\|srtt−rtt\|; `tail_ns()` = srtt + max(4·rttvar, 1 ms) | `src/lib.rs:163-216` |
| `TickPace` | the Raft tick period derived from the slowest voter path | `derive(configured, ceiling, election_tick, paths)`: period = clamp(⌈10·max tail / election_tick⌉, configured, ceiling), so `election_tick × period ≥ 10 × tail`; `election_timeout(ticks)` | `src/lib.rs:219-273` |
| `RoundBudget` | a fan-out round's deadline, lookahead, extensions and stall window | `hard(d)`; `derive(period, tail, ceiling)`: deadline = min(max(period, tail), ceiling), lookahead ¾, extension = period, ≤ 10 extensions, stall window = min(max(2·period, tail), ceiling) | `src/round.rs:26-85` |
| `RoundWait` (+ `ProgressWitness`, `DeadlineExtender`, `Verdict`) | judges a round: ends when all reported, extends while a quorum is still filling, expires on a stall | `begin(&budget, now)`, `judge(gathered, now) -> bool`, `next_judgement_ns()` | `src/round.rs:90-199` |
| `ProgressDeadline` / `Spent` | a test/owner wait charged in **owner periods**, not wall time; the charge is the least advance of any owner; `Frozen` is the only wall-clock bound | `begin(counters, budget, frozen)`, `periods(allowance, period)`, `check(counters) -> Result<(), Spent>` | `src/progress.rs:15-100` |

Interlock with consensus: `focal-raft` and `DurableNode` have no clock; they count ticks
(`focal-consensus` only re-exports `focal_timing`, `src/lib.rs:29-30`). The host derives
the period: `PeerConnectionPool` keeps a `PathRtt` per peer, fed only by probes answered
on an already-open connection (Karn's rule) (`crates/focal-wire/src/peers.rs:277, 411-421,
747-756`); a 1 s pacer loop in focal-node collects `pool.path(voter)` for every voter and
calls `TickPace::derive` for the root group and each hosted session
(`crates/focal-node/src/network_service.rs:1517-1554`, `control_host.rs:592-606`,
`fleet.rs:787-798`); owner threads tick at that period (default tick 100 ms, ceiling 2 s,
`control_host.rs:26-30, 48-49, 783-810`) and, when the period is stretched, a leader still
heartbeats at the configured cadence through `DurableNode::beat()`
(`control_host.rs:813-829`; `focal-consensus/src/lib.rs:787-797`). Only the election
timeout stretches, never the heartbeat (27 §3.1 P2). `RoundBudget`/`RoundWait` drive
`focal_wire::gather` (section 4) — used for placement/session-fact fan-out, **not** for
Raft replication.

### 5.2 focal-sim — real code in virtual time

Dependencies: `focal-model` (domain) and `thiserror` (`crates/focal-sim/Cargo.toml:11-13`);
the domain import is only in `history.rs` (`src/history.rs:4`). Consumers take it as a
dev-dependency only.

- `Seeded`: SplitMix64 with rejection sampling (`src/lib.rs:21-66`).
- `Network<M>`: an explicitly scheduled, bounded message queue with partitions
  (`src/network.rs:10-122`).
- **`path.rs`** (27 §3.1 P7, all tables bounded by `FabricLimits`: 4096 flows, 64 links,
  1024 NATs, 65,536 link messages; `src/path.rs:244-261`):
  - `Loss`: per-flow Gilbert–Elliott channel — `NONE`, `random(ppm)`,
    `bursty(enter, leave, burst_loss)` (`:25-71`).
  - `Link { rate_bits_per_second, queue_bytes }`: bottleneck with serialization delay and
    drop-tail queue, shareable by several paths (RFC 5166 dumbbell) (`:73-125`).
  - `Path { one_way_ns, jitter_ns, reorders, loss, mtu, link }`, profiles `LAN`
    0.2 ± 0.1 ms, `REGIONAL` 80 ± 20 ms, `GEOGRAPHIC` 500 ± 100 ms one way; an over-MTU
    message is black-holed (RFC 8899) (`:127-201`).
  - `Nat { idle_timeout_ns }` with `rebind(node)` (RFC 4787) (`:203-210, 362-367`).
  - `Fabric<M>`: `set_path`, `set_pair_path`, `add_link`, `set_link`, `set_nat`,
    `partition`, `send(from, to, msg, bytes) -> Fate`, `receive()`, `advance_to`,
    `next_arrival`, `stats()`; replays exactly from its seed (`:263-492, 710-725`).
- `disk.rs`: live vs durable file bytes and directory names, `crash()`, injected failure
  at an operation ordinal; no torn writes or lying fsync. Used only by focal-ledger
  backup tests — focal-log and focal-consensus test against real files
  (`src/disk.rs:22-161`).
- `history.rs`: checks ordered publication events from the real state owner against
  caller-visible history (not a black-box linearizability search); written over
  focal-model types (`src/history.rs:1-214`).

**How real code runs in virtual time.** There is no deterministic executor and no clock
trait; the core being sans-io is what makes it work. The consensus harnesses
(`crates/focal-consensus/src/sim_election_tests.rs:17-213`, `sim_fast_tests.rs:15-154`)
are hand-written discrete-event loops over real `DurableNode`s on real WALs in temp
directories: advance the `Fabric` clock to min(next tick, next arrival), `step` delivered
messages, `drain()`, `fabric.send` the output, `tick` due nodes. Per-replica periods come
from `TickPace::derive` over `PathRtt`s fed simulated probes (`sim_election_tests.rs:45-72`).
fsync takes real time that virtual time does not count. The QUIC congestion test drives
quinn-proto's sans-io `Endpoint`/`Connection` over a `Fabric` with a synthetic
`Instant = began + virtual ns` (`crates/focal-wire/tests/congestion.rs:213-291, 306-592`);
`gather` tests use tokio's paused clock (`crates/focal-wire/src/round.rs:154-266`).

### 5.3 focal-memory — budgets (and a generic range engine)

Dependencies: optional `serde` only (`crates/focal-memory/Cargo.toml:8-12`); no IO, no
clock, no tasks (`src/lib.rs:13-24`).

- **`MemoryBudget`** (`src/budget.rs`): `Clone` handle over atomic counters (`:182`);
  `new(limit, completion_reserve)`, `child(limit, reserve)` (charged to the child and every
  ancestor, all-or-nothing, ≤ 8 levels, strictest wins) (`:211-240`), `funded_child`
  (prepaid pool, `:254-312`), `elastic_funded_child` (`src/budget_elastic.rs:57-247`);
  `reserve(kind, lane, bytes) -> Result<Reservation, MemoryError>` via CAS
  (`:314-327, 467-477`); `Reservation::commit() -> Allocation`; `Allocation` is RAII with
  `absorb`, `split_off`, `shrink_to` (`:483-583`); `is_within`, `stats()`.
  `BudgetLane::{Ordinary, Completion}` — ordinary work can never spend the completion
  reserve (`:20-23`); 14 `BudgetKind`s for reporting (`:27-43`). Error
  `MemoryError::Capacity{requested, available}` et al. (`src/lib.rs:69-109`).
- **`DiskBudget`** (`src/disk.rs:25-336`): kinds `Wal, Checkpoint, Content, Archive,
  Staging`; headroom 64 MiB, completion reserve 16 MiB, free-space sample every 32
  admissions via `focal_platform::available_space`; a reservation is committed only once
  bytes are behind the durable fence.
- How consensus charges it: section 2.7 (per-group child budget; `RamLog` payloads,
  index slots, snapshot and proposals; `guarded_in` staging; decode scratch; replay;
  `NodeEvents` permits). Measured admission cost: 7.7 ns uncontended, 26.8 ns through a
  child (section 9).
- **Range engine (generic, reusable)**: `KeySpan<K>`, `RangeDescriptor<K, M>` and
  **`RangeMap<K, M>`** — a gap-free, overlap-free, epoch-fenced range directory with
  `route`/`routed` and `replace(sources, replacements, limits)` expressing move (1→1),
  split (1→N) and merge (N→1) with generation rules (`src/range_map.rs:14-313`);
  **`RangeStore<K, V>`** — an in-RAM copy-on-write, prefix-versioned ordered KV store with
  `prepare_batch`/`publish` (swap after the durability decision), leased snapshot reads,
  and `split_with`/`merge_with` that share whole pages and copy only the boundary page
  (`src/range.rs:85-1035`, `src/range_split.rs:22-240`, `src/range_directory.rs`); plus
  hydration, preflight and envelope helpers. No persistence engine: durability comes from
  the Raft log plus checkpoints.

---

## 6. focal-ranges — ranges *within* one Raft group, not multi-Raft split

- Dependencies: `focal-model` (domain), `focal-memory`, `serde`, `postcard`, `blake3`
  (`crates/focal-ranges/Cargo.toml:8-13`). Used by focal-ledger and focal-node (its README
  saying it is "not connected to the node" is stale, `README.md:3-6`).
- It partitions the state of **one session ledger — one Raft group —** into key-span
  members keyed by `StorageKey { affinity: [u8;16], family: u16, object: [u8;16],
  slot: u64 }` (`src/map.rs:8-38`), reusing `focal_memory::{KeySpan, RangeDescriptor}`
  (`:40-44`), with `Placement { owner: Voters | Replica(node, generation), readers }`
  (`:49-96`) and a blake3 digest of the map (`:138-211`).
- It "never creates an independent range or cross-session transaction decision"
  (`src/lib.rs:13-15`): every range decision is a record in the session's own Raft log;
  holders are materializer replicas serving reads, every voter still holds every member
  (doc 25 §6, `25-parallel-materialization-and-ranges.md:378-461`).
- Movement protocol: `RangeCoordinator` applies ordinal-ordered `RangeOperation`s —
  `Begin(intent) → Snapshot → Barrier → SourceSealed → Ready → Activate` (or `Abort`),
  then `Cleanup` — with `relayout` for split/merge and proof-based authority through the
  `RangeVerifier` trait whose proofs are `LedgerId`/`SessionSeq`/Raft index+term/content
  hashes (`src/coordinator.rs:46-71, 208-860`; `src/types.rs:91-173`). Doc 04 §11 gives
  the intent → seed → catch-up → barrier → activation order, epoch-fenced
  (`04-storage-and-distribution.md:438-473`).
- `RangeReplica` (a `RangeStore<StorageKey, Vec<u8>>` with arm/seal/activate/
  checkpoint), `RangeStager` (hash-verified blocks against a `SeedManifest`), `PinRegistry`
  (leased read cursors) (`src/replica.rs`, `src/pins.rs`). Limits: 64 ranges, 64 MiB
  checkpoint, 1 MiB blocks, etc. (`src/types.rs:46-62`).

**Verdict.** It is not a TiKV/CockroachDB-style region split where each range is its own
Raft group, and it is domain-typed throughout. For mantle: reuse the *pattern* (a fenced,
logged move/split state machine with seed + catch-up + barrier + activation), and reuse
`focal_memory::RangeMap`/`KeySpan` directly as the range directory type. Splitting a range
into a new Raft group (bootstrap the child group from the parent's state at a barrier
index, fence the key span by epoch) is not implemented anywhere in focal.

<!-- SECTION7 -->

---

## 8. Engineering rules and gates (for mantle to adopt)

### 8.1 Rules (`CLAUDE.md`)

They apply to every crate, including test-support crates; anything compiled outside
`#[cfg(test)]` or `tests/` is production (`CLAUDE.md:3-5`).

1. **No panics in production** (`:7-23`): no `panic!`, `unwrap`, `expect`,
   `unreachable!`, `todo!`, `unimplemented!`, `assert!` family, slice/map indexing,
   unchecked arithmetic or `as` narrowing — use `checked_*`, `saturating_*`, `try_from`,
   `.get()`, typed errors. Poisoned locks, closed channels and ended tasks are errors. A
   dependency that can panic runs behind `DurableNode::guarded_in` *and* the path to the
   panic is closed at its cause. Never `#[allow]` these in production.
2. **Nothing grows without a bound** (`:25-37`): every collection, queue, cache, map,
   log, journal, retry loop and wait has a stated bound; reaching it is a typed
   `Capacity` refusal or a stated eviction. Peer/caller-keyed maps are bounded and pruned
   when the key leaves the configuration. Loops end on a counted budget or deadline.
   Memory is charged to `focal_memory::MemoryBudget`, disk to the disk budget.
3. **Root causes, no shortcuts** (`:39-44`): never raise a limit, lengthen a timeout or
   retry past a failure; tests wait on facts via `focal_timing::ProgressDeadline`, never
   on wall-clock guesses.
4. **Gates** (`:46-57`), on Linux, macOS and Windows CI:
   ```
   cargo fmt --all --check
   python3 scripts/check-contracts.py
   bash scripts/cargo.sh clippy --workspace --all-targets --locked -- -D warnings
   bash scripts/check-production.sh
   cargo deny check advisories bans licenses sources
   bash scripts/cargo.sh test --workspace --locked -- --test-threads=4
   ```

Doc 10 adds: `deny` rather than `forbid` (derive macros conflict), lints are a guardrail
not proof, typed error classes (capacity, invalid input, stale handle, corruption,
unavailable), no saturating arithmetic for sequence numbers, offsets, epochs or charges;
a persistence/consensus invariant failure stops the owner from acknowledging later work
(`docs/archictecutre/10-ownership-and-failure-policy.md:3-17, 24, 41, 58`).

### 8.2 Mechanisms

| File | Content |
|---|---|
| `rust-toolchain.toml:1-4` | channel `1.94.1`, profile minimal, `rustfmt`, `clippy` |
| `Cargo.toml:1-65` | edition 2024, `rust-version = "1.94"`, resolver 3; `[workspace.lints.rust] unsafe_code = "deny"`, `unused_must_use = "deny"`; `[workspace.lints.clippy]` denies `dbg_macro, todo, unimplemented, panic, unwrap_used, expect_used, unreachable, indexing_slicing, arithmetic_side_effects, disallowed_macros, disallowed_methods`; release `lto = "thin"`, `codegen-units = 1`, **`overflow-checks = true`**; dev/test `debug = 0`, `incremental = false` |
| `clippy.toml:1-21` | `disallowed-methods`: the seven raft-proto enum accessors that unwind (`Message::get_msg_type`, `Entry::get_entry_type`, `ConfChange{,Single}::get_change_type`, `ConfChangeV2::{get_transition, enter_joint, leave_joint}`); `disallowed-macros`: std/core `assert*`, `debug_assert*`, `print*`, `eprint*` |
| crate roots, e.g. `crates/focal-raft/src/lib.rs:1-12` | `#![cfg_attr(test, allow(clippy::panic, clippy::unwrap_used, clippy::expect_used, clippy::unreachable, clippy::indexing_slicing, clippy::arithmetic_side_effects, clippy::disallowed_macros))]`; integration tests and benches use a file-level `#![allow(..)]` |
| `scripts/check-production.sh:6-18` | `clippy --workspace --lib --bins --locked` (no test targets) with `-D warnings` and explicit `-D` for panic, unwrap_used, expect_used, unreachable, indexing_slicing, arithmetic_side_effects, disallowed_macros, disallowed_methods, todo, unimplemented, dbg_macro |
| `scripts/check-contracts.py` | (1) every `crates/*/Cargo.toml` has `[lints] workspace = true` (`:13-15`); (2) relative markdown links in `docs/archictecutre/*.md` resolve (`:16-32`); (3) sha256 of imported reference sources unchanged (`:34-41`); (4) `unsafe` appears under `crates/*/src/**` only in `crates/focal-platform/src/windows.rs` (`:43-59`; `tests/` not scanned); (5) domain vocabulary codes match `focal-model` (`:61-70`) |
| `scripts/cargo.sh` | `exec cargo "$@"` from the repo root; notes protoc build via `protobuf-build` (`:1-7`) |
| `scripts/check-model.sh` | TLC model check (section 8.3) |
| `deny.toml:1-24` | graph targets aarch64-apple-darwin + x86_64-unknown-linux-gnu; `yanked = "deny"`, `unmaintained = "all"`, nothing suppressed; license allow-list (MIT, Apache-2.0, Apache-2.0 WITH LLVM-exception, BSD-2/3, ISC, Unicode-3.0, Zlib, CC0-1.0, Unlicense; confidence 0.93); `multiple-versions = "warn"`, `wildcards = "deny"`; unknown registry/git denied, `allow-git = ["https://github.com/tikv/raft-rs"]`, **`required-git-spec = "rev"`** |
| `docs/dependencies/README.md`, `inventory.tsv`, `raft-upstream.md` | `Cargo.lock` is the authority; a TSV row (`name version license source`) per external package, drift-checked at release (`scripts/release/notices.py`); written provenance and re-audit procedure for the raft-rs pin |
| `.github/workflows/ci.yml` | `dependencies` (cargo-deny 0.20.2), `model` (3-voter config + `wrong`), `check` on ubuntu-24.04 and macos-15 (contracts, fmt, clippy, check-production, tests, release build) (`:1-62`) |
| `.github/workflows/nightly.yml` | black-box histories, adversarial inputs, crash-cut matrix, seeded workloads, `FOCAL_RAFT_SEEDS=2000 FOCAL_RAFT_STEPS=6000` release consensus schedules, fast-track latency, congestion grid (uncommitted), five-voter TLC run (350 min timeout) (`:3-88`) |
| `.github/workflows/windows.yml` | only a build and a subset of tests (`focal-platform`, `focal-wire`, `focal-enrollment`, `focal-node --lib`, two native gates) — **not** the full gate set that `CLAUDE.md:48` claims (`:24-62`) |

What mantle must replicate to "adopt the same standards": the toolchain pin; the two
workspace lint tables plus `[lints] workspace = true` in every crate (with a contract
check); `clippy.toml` — **including the raft-proto accessor bans if mantle uses
focal-raft types, because `clippy.toml` is per-workspace and does not travel with a
dependency**; the crate-root test opt-out block; `check-production.sh`; `deny.toml` with
`allow-git` extended to `https://github.com/hyper-light/focal` (and kept for
`tikv/raft-rs`, transitive through raft-proto); `inventory.tsv` + drift check; a protoc or
C++ toolchain for raft-proto's build; `overflow-checks = true`; one audited `unsafe`
file; dependency-free `harness = false` benches; per-progress test deadlines.

### 8.3 TLA+ checking

`docs/models/FastTrack.tla` models the fast track as built: servers, values, terms,
logs, `held` (self-approved entries), commit, votes/acks/grants and a ghost `chosen`;
actions Hold, Say, Take, FastCommit, ClassicCommit, Replicate, Campaign, Grant, Lead
(recovery by `MostHeld`, or `LeastHeld` under the wrong rule); loss/reorder as "what was
said stays said"; invariants `TypeOK, Agreement, Committed, LeaderHolds, OneLeader`,
symmetry `Alike`. **No liveness properties and no configuration changes are modelled.**
Configs: 3 servers/2 values/MaxTerm 3/MaxLen 1 (`FastTrack.cfg`); 5 servers/MaxTerm 2
(`FastTrackFive.cfg`); 5 servers with `Rule = "least"` (`FastTrackWrong.cfg`, must fail).
`scripts/check-model.sh` downloads `tla2tools.jar` v1.7.4, verifies its sha256, runs
`tlc2.TLC -workers auto -deadlock -config <cfg>`; `wrong` passes only if TLC exits 12
with "Invariant LeaderHolds is violated". CI runs the 3-voter and wrong configs on every
push; the five-voter config runs nightly. Recorded: 3 voters — 8,278,749 states,
1,219,562 distinct, depth 26, no violation; wrong rule — violation at depth 17 after
26,212,234 states (`09-implementation-status.md:10946-10951`). No five-voter result is
recorded.

---

## 9. Recorded performance numbers

All are macOS arm64 (Apple Silicon, 18 logical CPUs) —
`docs/qualification/performance/2026-09-12-macos-arm64.md:3`. **No Linux/NVMe numbers
exist.**

| What | Number | Where |
|---|---|---|
| Replication CPU cost, in-memory storage, lossless net (ns per entry committed by all), raft-rs vs focal-raft | 3 members ×1 × 64 B: 3,333 vs 3,323; ×16 × 64 B: 1,738 vs 1,721; ×1 × 4 KiB: 16,841 vs 16,026; ×16 × 4 KiB: 15,186 vs 15,004; 5 members ×1 × 64 B: 6,608 vs 6,626; 5 × 16 × 1 KiB: 8,612 vs 8,510; 3 × 1 × 256 KiB: 906,984 vs 864,550 (ratio 0.95–1.00) | `09-implementation-status.md:10873-10888`; `crates/focal-raft/benches/replicate.rs` |
| Differential run vs raft-rs | 5 campaigns × 3,000 schedules × 6,000 steps, release, 77 s: 80,847,287 steps, all equal | `09:10816-10833` |
| Fast-track schedules | 2,000 × 6,000 steps, 7 s: 331,509 fast proposals, 284,247 held, 7,973 taken on arrival, 86,941 taken at election, 1,913 committed by the fast quorum, 73,142 displaced | `09:10929-10937` |
| Fast-track latency (non-leader proposer, propose→applied at itself, virtual time; **real WAL on disk but fsync time not counted**) | regional 3 members 0% loss: classic 320.9 ms mean vs fast 243.4 ms (0.76); regional 5 members 10%: 499.6 vs 283.9 (0.57); LAN 3 members 0%: 0.8 vs 0.6 ms; LAN 3 members 5%: 14.6 vs 17.9 (1.22, the one case where fast is slower on the mean); p99 always lower or equal | `09:10953-10985`; `crates/focal-consensus/src/sim_fast_tests.rs:1-5, 157-200, 322-341` |
| WAL durable append (`Wal::append`, one F_FULLFSYNC each) | ~12.6 ms/append (batch 1 × 64 B, 80 rec/s); ~13.4 ms (16 × 64 B, 1,198 rec/s); ~14.7 ms (256 × 64 B, 17,357 rec/s, 1.1 MiB/s); ~12.7 ms (1 × 4 KiB, 79 rec/s); ~13.6 ms (16 × 4 KiB, 1,175 rec/s, 4.6 MiB/s) | `docs/qualification/performance/2026-09-12-macos-arm64.md:62-83`; `capacity-envelope.md:28-40`; `09:9811-9820` |
| End to end, single node | 200 claims at ~33 ops/s, p50 ~30 ms, bound by F_FULLFSYNC | `09:9891-9894` |
| Memory budget admission | reserve/commit 7.7 ns uncontended, 26.8 ns via child budget; shared envelope 130.8 / 237.0 / 270.5 ns at 2 / 4 / 8 threads | `performance/2026-09-12-macos-arm64.md:11-18` |
| Request codec | ~40 ns fixed each way, decode ~2 GiB/s | same file `:45-54` |
| Elections over modelled paths | assertion bounds, not measurements: a leader within 8 election timeouts at 0.2/80/500 ms one-way; at 1.2 s one-way only the derived pace elects | `09:10617-10638` |
| KIND scale (focal:0.1.10) | 25 pods Ready in 108 s; zone of 12 pods back in 74 s; claim committed at t = 7 s during a zone outage; liveness suspect→dead ≈ 35 s | `09:10254-10311` |
| Transport / Copa (uncommitted) | section 4 | `09:11056-11210`; 27 §7 |

No multi-group WAL throughput number is recorded — only the test that 12 groups share one
covering flush. No commit-latency number on a real network with real fsync exists.

<!-- SECTION10 -->
