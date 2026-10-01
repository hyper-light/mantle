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
