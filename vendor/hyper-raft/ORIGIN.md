# Where hyper-raft came from

- **Source.** focal (`github.com/hyper-light/focal`), `crates/focal-raft`, at
  `a8e95f71496461ebc8984926a65666746e946473` (`origin/slates-port`, 2026-09-30).
- **Why.** mantle note 32 (`docs/research/32-shared-transport-and-raft.md`) §3.8 and §5.2 step R-1: the
  shared Raft core is focal's, moved unchanged in behaviour. The owner's decisions in that note's §6 name
  it `hyper-raft`.
- **History.** The eight focal commits that touched `crates/focal-raft` were split out
  (`git subtree split --prefix=crates/focal-raft a8e95f7`), moved under `crates/hyper-raft` with an index
  filter, and merged into this repository unchanged (merge `a24589b`). `git log --follow` on any file
  reaches focal's first commit of it.

  | focal | here | |
  |---|---|---|
  | `b92492c` | `d5a222e` | focal's own consensus core under the durable shell, compared with raft-rs step for step |
  | `f929ea0` | `13df4de` | the fast track, modelled and measured |
  | `bd5e54b` | `817a7ce` | leadership that returns, a zone that outranks, preferred leaders spread |
  | `c1af831` | `3906c6a` | a stall covered in ticks |
  | `078d9ac` | `0a130e7` | the allocation audit's counting benches |
  | `653616e` | `352864b` | Raft acknowledgments admitted beside the participants (focal audit F56, F63) |
  | `58c8141` | `7c5e32f` | a page is chosen before it is copied (focal audit F15, F16) |
  | `1f87bfc` | `7026c0b` | every fixed allowance of the consensus pricing is derived (focal audit F16) |

  The tree at `7026c0b` equals focal's `a8e95f7:crates/focal-raft` byte for byte.

## Changes since

Each change below preserves behaviour. "Proof" at the end says how that was shown.

### Name

- Package `focal-raft` is now `hyper-raft`, and the library `hyper_raft`.
- Tests and the bench import `hyper_raft::` (45 lines).
- The schedule variables are `HYPER_RAFT_SEEDS`, `HYPER_RAFT_STEPS` and `HYPER_RAFT_SEED`, in place of
  `FOCAL_RAFT_*` (14 lines, README included).
- Printed labels say `hyper-raft` (5 lines).
- `Settings::focal()` keeps its name: it is the settings focal runs the core with.

### Manifest

- The crate inherits `version`, `edition`, `rust-version`, `license` and `repository` from the
  workspace, and `[lints] workspace = true`.
- `raft-proto`, `thiserror`, and the dev-dependencies `raft` and `slog`, are workspace dependencies here,
  at focal's pins: raft-rs `8e4cef172421bf77b2ae1c26628a9531b0be41f0`, no default features,
  `prost-codec`. raft-rs is a dev-dependency only: the differential's oracle.
- `Cargo.lock` was seeded from focal's at `a8e95f7` and pruned to this workspace, so every package
  resolves to the version focal tested.
- This workspace declares `rust-version = "1.98"`. focal pins 1.94.1.

### The lint wall

This repository's wall adds to focal's: `missing_docs`, `cognitive_complexity` (threshold 10), the cast
lints, `string_slice`, `panic_in_result_fn`, `unwrap_in_result`, `undocumented_unsafe_blocks` and
`disallowed_types`. Shipped code meets all of them with no `#[allow]`.

- **`missing_docs`.** 208 public items were documented: 124 methods, 33 enum variants, 19 struct fields,
  12 associated functions, 8 structs, 5 enums, 5 functions, 1 type alias and 1 trait. Each doc comment
  states what the code does; none changes it.
- **`cognitive_complexity`.** One shipped function was over the threshold: `Raft::step`, at 13/10.
  - It is split along the lines it already had, and the moved bodies are unchanged.
    - `counts_beyond_bound` is the check for a term or index at `u64::MAX`.
    - `Raft::step_term` holds a message's term against the member's.
    - `step_term` passes a newer term to `Raft::step_newer_term` and an older one to
      `Raft::step_older_term`.
    - The private enum `TermChecked` says whether the message goes on to its kind's handler.
  - Every branch and early return keeps its order.
- **Casts, `string_slice`, `panic_in_result_fn`, `unwrap_in_result`, `undocumented_unsafe_blocks`.**
  Shipped code had no finding. The crate has no `unsafe`.
- **`disallowed_types`.** Shipped code had no finding.
- **`clippy.toml`.** focal's seven `disallowed-methods` came with the core. These are raft-proto's
  enumeration accessors, which unwind on an unknown value.
- **Test code.** Tests keep the opt-out pattern the rules allow, so that they stay the unchanged proof:
  - `src/lib.rs`'s `cfg_attr(test, allow(..))` adds `cognitive_complexity`,
    `cast_possible_truncation` and `unreachable_pub`. These cover 19 test functions over the threshold,
    7 casts and 4 `pub` helpers in the in-crate test modules.
  - The crate-root `allow` of `tests/differential.rs`, `tests/fast.rs`, `tests/group.rs` and
    `benches/replicate.rs` adds the three cast lints, `cognitive_complexity` and `unreachable_pub`.
    These cover 30 casts, 9 test functions over the threshold and 58 `pub` helpers in `tests/support`.
  - No test code opts out of `disallowed_types`. The harness's disk had been an `Rc<RefCell<Disk>>`;
    see the next section.

### The harness's disk has one owner

In focal, the harness kept each member's disk as an `Rc<RefCell<Disk>>`, shared by the member's node
and by the cluster. This repository denies `Rc` in test code too. The harness now gives each disk one
owner at a time. This is a separate commit, so that the commit before it keeps the tests unchanged as
its proof.

- `Store` is the `Disk` itself.
- A running member's node owns its store, and the harness reaches it through the node: raft-rs's
  `mut_store`, or this crate's `store_mut`.
- `Cluster` holds each member as `Member::Up(node)` or `Member::Down(store)`.
  - `Cluster::stop` takes the store out of the node it drops.
  - `Cluster::restart` opens the member on what was stopped.
  - `Cluster::disk` reads a member's disk, running or stopped.
- The tests stopped a member by writing `group.nodes[i] = None`; they now call `group.stop(i)`
  (5 sites in `tests/fast.rs` and `tests/group.rs`). They read a configuration through
  `group.disk(id)` in place of `group.stores[i].0.borrow()` (2 sites in `tests/group.rs`). No assertion
  changed.
- The proof is the same as for the commit before. Every seed-fixed count the tests print is identical to
  focal `a8e95f7`'s, at 96 seeds and at 1,000 seeds from seed 1,000.

### Constants

Every value is unchanged. These doc comments now state where each value comes from:

- `Config::new` gives raft-rs 0.7's `Config::default` values, so the two cores compare under one setting.
- `CAMPAIGN_TRANSFER` is raft-rs's bytes.
- `approximate_bytes`'s twelve is raft-rs's `entry_approximate_size`, with its derivation.
- `FAST_PROPOSE` and `FAST_VOTE` are wire identifiers outside raft-proto's `MessageType` range (0 to 18).

Two kinds of value are literals, not derivations, and are marked so:

- `Limits::default`: mantle note 32 §2.10. `Limits::derive` replaces it in R-3.
- `MAX_MEMBERS`.

They are open against the no-arbitrary-numbers rule until R-3.

### Removed

- `benches/allocs.rs`, focal's allocation-count bench. It includes focal-memory's counting global
  allocator (`focal-memory/benches/support/alloc_count.rs`, 833 lines) by path. That allocator needs
  `unsafe` (`GlobalAlloc`) and a `std::sync::Mutex` site table. This repository allows `unsafe` only in
  files its contract script lists, and it has no such script yet. It also denies `Mutex` everywhere.
  - The counting allocator is shared test infrastructure, so it belongs with `hyper-sim` (note 32
    §3.10). It returns there, without the lock, as that crate's own work.
  - It was still run as evidence for this move; see Proof.
- The README's row for the TLA+ model. The model is still in focal (`docs/models/FastTrack.tla`), and it
  moves here with the fast-track decision (`docs/raft.md`).

### Proof

All runs used `--test-threads=4`.

- **The tests.** focal-raft's tests at focal `a8e95f7`, and this crate's tests changed only by the
  renames above, both pass:
  - 53 unit tests;
  - 6 differential tests;
  - 9 fast-track tests;
  - 7 group tests.
- **96 seeds.** Every count the tests print is identical at the default 96 seeds. In the differential
  that is, for each of the six mixes, the schedules compared, the steps compared, where each run ended,
  and the `Reached` totals. In the fast-track and group tests it is the terms led, the entries committed
  and the `FastStats`.
- **1,000 seeds.** The same holds at 1,000 seeds from seed 1,000 (`*_SEEDS=1000 *_SEED=1000`). The six
  differential mixes compared 22,275,363 steps and gave identical totals.
- **The one line that varies.** In `a_group_of_both_cores_is_safe_and_settles`, raft-rs draws its own
  election timeouts from `rand::thread_rng`, so the terms each core led vary from run to run. Three runs
  at focal `a8e95f7` gave 525/313, 497/340 and 494/282. Three runs here gave 496/318, 498/337 and
  484/316.
- **Allocations.** focal's `allocs` bench was run in a scratch clone of focal, on `a8e95f7`'s sources and
  then with this crate's `src/` in their place. Per committed entry, these are identical for every
  configuration:
  - allocations;
  - reallocations;
  - bytes requested;
  - peak and live growth;
  - the size histograms.

  Only the "moved by the allocator" column differs, and it differs for the unchanged raft-rs rows too:
  it records where the system allocator placed blocks.

## After R-1: the law of measurement (branch `raft-law`)

`CLAUDE.md` §1a: allocations, reallocations and page faults are measured on every hot path and
driven down, and the crate is benchmarked against each core it replaces. The measurements are in
`docs/benchmarks.md`; the counting allocator is `crates/hyper-measure` (it replaces the
`benches/allocs.rs` this move removed, without its lock, its `unsafe` in two files the contract
script lists). Two changes to this crate followed from them. Each decides exactly what the
crate decided before.

### Entries move into the log uncopied

- A leader's proposals were copied into the log not yet durable, through a staging vector, and a
  follower copied every entry of an append out of the leader's message the same way.
- They now move in: `Log::append_owned` and `Log::append_after_owned` take the entries by value,
  and when the log holds nothing not yet durable the incoming vector becomes its own.
- `Log::append` and `Log::append_after` keep their borrowed signatures and copy, as before.
- One count rose: a batch of appends to a log that holds nothing grows the adopted vector from
  its exact length, one reallocation more per batch than growing a fresh one. Each such batch
  saves one allocation and every entry's copy, so allocator calls fell in every workload
  (`docs/benchmarks.md`, "Optimisations").

### A `Ready` given in place

- `RawNode::ready_in_place` decides exactly as `RawNode::ready` and copies nothing the owner can
  read where it is: the owner writes from `RawNode::to_persist`, applies the range
  `Ready::committed_range` names from its own storage (`Log::next_range_since` chooses it by the
  rule `Log::next_entries_since` pages by), and keeps the entries and snapshot the member gives
  up at `RawNode::advance_append_keeping`.
- `RawNode::ready` and `RawNode::advance_append` are unchanged.

### A page's entries are read once

- A leader's page held what storage gave to the byte rule three times: storage's own count, a
  running total for what followed it, and a final cut over the whole page. It now counts each
  entry once, in the walk that also sums what the page's buffers hold, which the message queue's
  accounting had walked the entries again for (`Log::slice`, `Outbox::send_page`,
  `proto::message_bytes_with`).
- A storage that gives more than the rule admits is still cut by the rule (a unit test holds it).

### Proof

- All tests pass: 55 unit tests, 7 differential tests, 10 fast-track tests, 8 group tests.
- The differential runs every mix twice, with `Ready`s copied and given in place. `fast.rs` and
  `group.rs` run their schedules both ways and assert equal results.
- At 1,000 seeds from seed 1,000 every seed-fixed count the tests print is identical to focal
  `a8e95f7`'s (R-1's record, above), and each in-place mix's counts equal its copying twin's: the
  six mixes compared 22,275,363 steps each way.

## Ports from focal

focal's own session changes the core in its copy, `crates/focal-raft`, and each change is ported
here in a commit of its own, adapted to this crate's names and its optimisations (above). Each
keeps the raft-rs differential unchanged: the differential runs the rule raft-rs has wherever the
port adds one of its own.

### F43: reads asked together share one round of heartbeats (focal `6af1c6b`)

- A leader no longer sends a round of heartbeats as each read is asked. `Raft::ask_reads` sends
  one when the member is next asked for a `Ready` (`RawNode::ready` and `RawNode::ready_in_place`
  alike), carrying the context of the last read asked; a quorum's answer confirms it and every read
  before it (Ongaro's thesis §6.4).
- `ReadOnly::asked` marks how many reads the last round sent asks for; a round never confirms a
  read asked after it left (`ReadOnly::advance` takes the confirmed reads off the asked ones).
  `RawNode::has_ready` is true while a read is unasked.
- `Config::read_rounds` is `ReadRounds::Shared` by default. `ReadRounds::Each` keeps raft-rs's
  round per read, and the differential runs with it (`Settings::shell`).
- Tests: three unit tests from focal; the schedule harness checks every answered read against
  the highest index committed when it was asked (`Cluster::report`), and this core's schedules ask
  reads in bursts (`Mix::bursts`, `Op::Reads`); the directed safety test
  `a_round_confirms_no_read_asked_after_it_left`.
- Changed from focal's form: the round is asked in `RawNode::ready_given`, the one path both
  `Ready` forms take, so the in-place form sends it too; the schedule count of
  `schedules_of_this_core` returns the reads answered as well as the terms, so that the in-place
  twin is held to answer the same reads; the doc comments the lint wall asks for.

### F41: a member is sent no more bytes ahead of its answers than its path carries (focal `052ae4a`)

- `Inflights` holds each message's last index and the bytes of its entries, and is full at
  `cap` messages or at its byte bound. One entry larger than the bound is sent, alone; a bound is
  never zero.
- `Config::max_inflight_bytes` (default `u64::MAX`, no bound of its own; zero refused) seeds every
  member's bound, and `RawNode::set_inflight_bytes` sets one member's as its owner learns the
  path. `Tracker::new` takes the bound; `Progress::sent(last, bytes)` charges the window;
  `Progress::page_bytes` cuts a page to the window's room while entries are sent ahead of their
  answers; `Raft::check_accounting` checks every window's count of bytes.
- The differential runs with no byte bound (`Settings::shell`). This core's schedules run with
  256 bytes (`Settings::focal`, and so `Settings::fast`) and change bounds while they run
  (`Mix::windows`, `Op::Window`).
- Changed from focal's form: focal walked the page a second time after cutting it, summing
  `proto::encoded_bytes`, to charge the window. Here the bytes are counted in the walk that chooses
  the page (`Page::bytes`), which the byte rule makes anyway wherever it cuts: storage's page held
  to the rule, and the tail not yet durable. Where it does not count them (a storage that cut the
  page itself, or a page with no bound), the bytes are counted in the walk that already counts the
  page's buffers, and only for a leader's page (`Log::page`); `Log::slice`, which pages what is
  applied, counts nothing more than before. A unit test holds `Page::bytes` to the sum of the
  encodings on every path, against a storage that pages by the rule and one that gives too much.

### F42: a peer that answers nothing holds its own lane (focal `4bf7b64`)

Taken from focal's patch of `crates/focal-raft` `7ab57e0..04a45f6`, after focal gated `4bf7b64`
(`origin/slates-port`); the patch is byte for byte focal's diff `052ae4a..4bf7b64` of the crate.

- `HeartbeatAnswers::Position` (the default): a member's heartbeat answer carries its last index
  and that entry's term. Where the entry is of the leader's term, the answer is taken as an
  append's answer for everything through it (not in a fast group, whose terms differ by member), so
  lost answers are made good exactly and a full window gives back what the member holds and nothing
  more. A member whose window is full and that answers for none of it through a beat of the
  leader's ticks (`Progress::stalled`, counted by `Progress::tick` on the leader's ticks, never by
  heartbeat answers) is probed; a probe is sent again when told lost (`MsgUnreachable`) or once a
  beat has passed. `HeartbeatAnswers::Bare` keeps raft-rs's rule (answers say nothing; a full window
  frees its first message at every answer), and the differential runs with it.
- `Raft::settle_priority` puts no priority in force for a member that is not promotable: a member
  that applied its own removal refused, for good, the voter that remained. The directed test is
  `a_member_that_left_refuses_no_one_for_priority`. The harness's emulation of priority for
  raft-rs (`Old::settle`) takes the same rule, so the differential still agrees step for step; its
  `Reached` totals moved in the two mixes with priorities, `shell` (votes refused 2,217 → 2,215)
  and `plain` (terms 5,036 → 5,025, committed 76,316 → 75,998), and reverting the two lines of the
  rule restores them exactly.
- `Cluster::settles` proposes again a proposal whose leader was deposed before it committed.
- focal's documentation that the fast track is not safe as built (`fast.rs`, README) came with
  it; the next change below mends it.
- Changed from focal's form: the handling of a heartbeat's answer by `HeartbeatAnswers` moved
  unchanged into `Progress::heard_heartbeat`, since `Raft::handle_heartbeat_response` with it was
  over the lint wall's cognitive-complexity threshold (11/10); the harness reads a configuration
  through `Cluster::disk`, since its disks have one owner here.

## The fast track's safety fix

Not a port: this repository's own change, after the ports above. focal found the defect and
documented the fast track as not safe as built (F42); the design, the literature it cites and the
evidence are in `docs/raft.md`, "The fast track's election defect, and its fix".

- **First rule** (`Raft::fast_commit`): a member that holds the entry beside its log counts toward a
  fast quorum only once the leader knows its log holds an entry of the leader's term. Without it, an
  election committed a second entry at an index that held a committed one (seed 9843 of 40,000 from
  seed 3,000, reproduced on `e1e292c` before any change; directed test
  `an_election_never_commits_a_second_entry_at_a_committed_index`).
- **Second rule** (`Raft::fast_quorum_of_the_term`): a fast quorum counts only where it is one of the
  voters the leader was elected under and of the one other set of voters a change in its term named;
  after a change that names a third, none until the next term. Without it, a member counting by the
  configuration before a change it had not applied was elected and committed a second entry (seeds
  54104 and 203544, found once the first rule was in; directed test
  `a_member_that_counts_by_the_configuration_before_commits_no_second_entry`).
- The fast-track and README notes that it was not safe as built are replaced by the rules.
- Evidence: 160,000 fast schedules from four base seeds pass; the raft-rs differential, which runs
  no fast group, is unchanged; allocation counts in `docs/benchmarks.md`.

## R-2: its own types and its own wire format

The owner's decision (2026-10-01): hyper-raft speaks its own protocol. `raft-proto` is gone from
production; the format is `docs/raft.md` §3.1, written and read by `src/wire.rs`.

- **The types** (`src/proto.rs`) are plain structs with typed kinds: `MessageType`, `EntryType`,
  `ConfChangeType` and `ConfChangeTransition` are enums held as themselves, so a kind no value
  names is refused when bytes are read and never held. The fast track's two kinds, which were
  numbers past raft-proto's range (100 and 101), are `MsgFastPropose` and `MsgFastVote`.
  raft-rs's `deprecated_priority`, `sync_log` and the change `id` are not carried: hyper-raft never
  read them. `HardState` is `Copy`.
- **The format**: a record is a version, a kind, a fixed-width little-endian body with
  variable-length bytes after the fixed fields, and a CRC-32C. Every count and length is checked
  against the bytes left before anything is taken or allocated; an unknown version, kind, flag or
  presence bit, a checksum mismatch and trailing bytes are each refused with a typed
  `wire::DecodeError`.
- **An entry's bytes** are counted by the format: `proto::encoded_bytes` is the entry's length in a
  message (its 25 fixed bytes, its data and its context), and the unpersisted-entry count
  (`approximate_bytes`, raft-rs's twelve-byte estimate before) is the same length.
- **The empty change** has one encoding: no data. A leader's own leave entry always had none; a
  proposal of the empty change is now written the same way, and `Plan::of_entry` reads an entry
  with no data as the empty change (protocol buffers read empty bytes as the default message; this
  format refuses a record cut short, so the case is explicit). Found by the differential.
- **`apply_conf_change_v1`** builds the joint form of a single change directly (`proto::joint`)
  instead of encoding the change into an entry and reading it back.
- **The differential against raft-rs** (`tests/differential.rs`) compares the two cores as values
  through `tests/support/convert.rs`, which converts field by field and re-encodes a change carried
  in an entry's data between the formats. raft-rs counts an uncommitted change by its own encoding;
  the adapter replays raft-rs's rule (reset on taking the lead, counted past the tail, taken off on
  commit while leading, floored at zero) for the difference, so the counts compare in this
  format's bytes. Four settings compare step for step as before (the shell's, without pre-vote and
  check-quorum, a window of two and one entry a message, a network that loses nothing, each with
  readies copied and in place). Where a bound counts bytes (pages of 100 and 64 bytes, a ready's
  committed bytes, the uncommitted bound), the two cores cut at different entries by design, so
  raft-rs is no oracle there: this core runs those schedules alone and is held after every step to
  its bounds as it measures them (`alone`, `bounded`).
- **Tests of the format** (`src/wire.rs`): every layout written out field by field, every type
  round-tripped, every truncation, one-byte extension and single-bit flip of every record refused,
  counts past the bytes refused with a valid checksum, and 140,000 arbitrary bodies under valid
  checksums decoded without a panic.
