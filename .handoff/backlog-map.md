# mantle backlog map (PARTIAL: paused 2026-11-17 for a machine move)

Source: worktree `/Users/ada.lundhe/Projects/.worktrees/integrate`, branch `integrate` at `c3b16c42`
(origin/lean-doors). Read only: no builds, no tests run, no file changed in any repository.
Inputs read so far: `docs/STATUS.md` in full (Implemented, In progress, Goal stack checked,
Planned 1–28 and the three frames); `docs/design/node.md` §2.3, §2.5–§2.8, §4.5, §9–§11;
`docs/design/gateway.md` §2–§6; `docs/design/metadata.md` §2 "Operations", §4–§6;
`docs/design/measurement.md` §10; plus targeted greps and reads of the code cited below.

## Coverage

| Reached, with code evidence | Not reached (no mapping yet) |
|---|---|
| 1, 3, 6, 7, 8, 9, 12, 13 | 11, 14 (only STATUS text, no code check), 15–28 |
| 2, 4, 5, 10: marked held by the other session from STATUS text only, not read in code | The three frames: S3 gateway profile, multi-machine, cells |
| | membership.md §10 and repair.md §8 (the owner decisions they name) not read |

Whoever resumes: start at item 11 and work down; then the frames; then read membership.md §10,
repair.md §8, storage-classes.md, architecture.md §4–§8, chunk-store.md §9.1, durability.md §5,
replica.md §7, and `scripts/arbitrary-open.txt` (the guessed constants the contracts gate still
lists, e.g. line 198 `crates/node/src/derive.rs gateway:window`), which is a second source of
remaining work beside the backlog.

## Item by item

### 1. No broadcast wakes: DONE (STATUS's "on macOS" qualifier probably stale)

- The log's `Room` hands room one-to-one: test
  `each_answer_wakes_only_the_waiter_it_admits_and_a_fence_wakes_each_once`
  (`vendor/hyper-log/src/room.rs:251`); `a_waker_is_woken_once_for_each_answer`
  (`vendor/hyper-log/tests/log.rs:4000`). `workers.rs` is gone (no file of that name in the tree).
- Every `Condvar`/`notify_all` left under `crates/` is test-only: `crates/chunk/src/volume.rs:1067`
  (inside `mod tests`, :1060), `crates/engine/src/branch/point.rs:218` (`mod tests`, :205),
  `crates/engine/src/shard_db.rs:5891` (`#[cfg(test)]`, :5876), `crates/engine/src/trunk/relay.rs`
  and `trunk/growth_asker.rs` (both `#![cfg(test)]`). `crates/engine/src/ranges.rs:1205`'s
  `Barrier` is a stop/checkpoint ask, not `std::sync::Barrier`.
- Stale: STATUS says "done on macOS (2026-09-30)". The tests live in hyper-log, whose suite is a
  gate (`cargo test --manifest-path vendor/Cargo.toml`) that CI runs on every target, so the item is
  likely done on all targets. Not confirmed against a CI run here.

### 2. Depth without a thread per transfer: HELD BY THE OTHER SESSION

(hyper-block threads/issuer, crates/disk measurement.) Not read in code. Commit `b543fb0c`
("a device's depth costs no thread on Linux") and the memory note that Linux uses native AIO, not
io_uring, suggest STATUS's "io_uring remaining" wording is stale; the other session owns the check.

### 3. Logical clients as records: DONE, two small remainders

- Built: `crates/mantle/tests/clients.rs:35`
  (`bench_log_runs_the_same_threads_at_ten_and_a_hundred_times_the_replicas`),
  `crates/mantle/src/bench.rs:1265` (`clients_cost_no_threads`), open loop from a Poisson process
  with lateness reported (`crates/mantle/src/bench_gateway.rs:22`, `:448`, `:523`;
  `crates/mantle/src/bench.rs:7–12`, `:530`).
- Remaining: (a) resident memory per client at two client counts (measurement.md §10, last bullet):
  no `resident`/RSS reporting in any `crates/mantle/src/bench*.rs`; (b) the replica ladder's bound
  derived from the idle engine instance's cost (node.md §11, still open); (c) chunk reads blocking a
  thread each: held by the other session (item 5).

### 4. One issuer per physical device: HELD BY THE OTHER SESSION

### 5. The device dispatcher: HELD BY THE OTHER SESSION

Item 6's device half (start-tag dispatch, `D_b`, `D_n`, `u`) sits here too.

### 6. Charges and fair queues: NOT BUILT (a different, per-node mechanism is)

- No SFQ, start tags, virtual time, mClock/dmClock, Aequitas, Space-Saving, DRF or count-min
  anywhere under `crates/` (grep; the only "start tag" hits are XML in `crates/s3/src/xml.rs`).
- What exists instead is node.md §2.8 (owner's goal-16 rule, 2026-11-12): one learned admission
  limit per node, the point of greatest power found by BBR-style probing
  (`crates/node/src/admit.rs:1–25`, `PHASE = 267`), byte-charge admission
  (`derive::Charges`, `host::fitting`), requests parked at the door in arrival order, native
  exchanges from a pool. It is one FIFO authority with no per-principal fairness.
- Principals with their own keys now exist (built 2026-11-14, `docs/design/principals.md`), so the
  SFQ key (the authenticated principal) is available. Tenants and contracts are not stored
  (metadata.md §6), so per node.md §2.7 "with no contract configured, every request is one class".
- Splits into: gateway/admission half (hierarchical SFQ over principals, bounded by admitted work;
  Space-Saving table; per-range command SFQ, node.md §2.3; admission level on answers) = workable
  now; device half (DRF charges from calibrated per-device cost models, dispatcher) = held by the
  other session; contract classes (mClock reservation, Aequitas admit probability) = need an owner
  decision on tenants/contracts first.
- Done condition (node.md §9.1 "the owner's scenario") needs the many-principal simulation, which
  `crates/node/tests/sim.rs` does not yet have ("Still to build: many ranges per node, the clients
  in their millions, and the faults and loads listed above", node.md:3021).

### 7. Overload as sojourn, shed by hashed principal: NOT BUILT

- No CoDel/sojourn test anywhere. `amz-sdk-request` (the attempt header) is never parsed in
  production code: it appears only in fixtures (`crates/mantle/src/bench_http.rs:63`,
  `crates/http/tests/allocations.rs:41`). No hashed-principal shedding.
- Built (node.md §4.5 "as built"): `503 SlowDown` before any body for a stopping node; a connection
  past the listener's budget answered `503 SlowDown` from a reserve of ten, then closed; idle
  connections let go after 60 s (`bounds::STOCK_PATIENCE`); door parking means a burst waits rather
  than being refused (§2.8).
- node.md §4.5 open list: the descriptor limit the budget does not read; reclaim of the
  least-progressing connection; the shared authority's sojourn test (a request now waits at the
  door as long as the operations ahead take). Whether to send `x-amz-retry-after` is open
  (node.md §11, last bullet).
- Conflict: item 7 lands in the same admission path as item 6's gateway half
  (`crates/node/src/admit.rs`, `crates/node/src/door.rs`, `crates/node/src/node/host.rs`), so the
  two are one stream of work, not two parallel packages.

### 8. Fair-share windows: PARTLY BUILT; STATUS's "Open" paragraph is stale

- Built 2026-11-13 (commit `6d95f931`): a PUT's window learned by Little's law, block
  time-to-recorded times the body's pace over a block's bytes, each smoothed with RFC 6298's gain
  (`crates/node/src/window.rs:1–60`; `Put::take_committed`, `Put::take_filled`), admitted and
  charged at that window (`derive::Charges::write`).
- Remaining: (a) the entitled-rate term `R = min(body rate, principal's fair share)` (needs
  item 6); (b) a GET's window is still one block (`gateway:window` listed as guessed in
  `scripts/arbitrary-open.txt:198` and `docs/design/constants.md:1209`; node.md §2.8 says it is
  open until the get's rate and chunk read time are measured); (c) the hedge quantile `T_q`
  (hedging not built, gateway.md §3 as-built); (d) the done test (lone upload reaches
  `min(client rate, device plateau)`, window shrinks and regrows across a spike).
- Stale: the item's "Open" paragraph (a replica's chunk put waiting one 3.07 s idle timeout; the
  client's first run waiting until the node states its write time). STATUS's own In progress
  section says progress notes (research/77 D1) and carried deadlines (D2) are built and on main at
  `17eb8032` (STATUS lines 571–584). Only "held vs moved in a note for a no-deadline asker" remains
  open there.

### 9. Operation identity: PARTLY BUILT (carried end to end, not recorded)

- Built: the client draws 128 random bits per mutation (gateway.md §6, as built); the native
  request head carries it (`crates/protocol/src/object.rs:14`); the S3 listener derives it from
  `amz-sdk-invocation-id` scoped by the authenticated principal, else draws one
  (`crates/node/src/listener/request.rs:442–466`); the host passes it into the Name `Put`'s `id`
  (`crates/node/src/node/host.rs:2190`, built at `:4722–4726`).
- But the Name range uses `id` only to mark a write with no file (an empty object)
  (`crates/meta/src/name.rs:266–270`, `:737–745`). A retry of a write with a file draws a new file,
  so in a versioned bucket it makes a second version. STATUS line 410 and gateway.md:335 say so.
- Remaining (metadata.md §2 "Operations", lines 344–363; key row `(OPERATIONS, bucket, key,
  principal, identity)` at metadata.md:35): the operation row committed with the version, part or
  completion; later attempts answered from it with nothing applied and their file released; the
  horizon capped by the server's maximum; removal by the collector at range time; carried by
  splits with its key; `Expired` natively past the horizon; the orphan-sweep simulation extended
  (`crates/meta/tests/orphan_sweep.rs`) with each broken variant failing; the real-process test
  (node killed between commit and answer in a versioned bucket, retry through another node, one
  version).
- Possible owner decision: the server's maximum horizon is "the bytes the operator gives the
  operation rows over that rate: a budget, not a picked period" (metadata.md:357–358). No such
  operator statement exists in `mantle serve`'s configuration today; either derive it from the
  node's memory division (`derive.rs`) or ask the owner.

### 10. The device plan by class: HELD BY THE OTHER SESSION

### 11. The derived scrub period: NOT REACHED

The scrubber is `crates/chunk/src/scrub.rs` (held). Whether the period can be derived in
`crates/node/src/derive.rs` and passed in without touching crates/chunk is unchecked.

### 12. Native protocol, client library and CLI: PARTLY BUILT

- Built (gateway.md §6 as built, 2026-10-26 and 2026-11-14): S3's operations natively, streamed in
  bounded memory (`crates/client/tests/heap.rs`), client admitted by certificate, typed refusals
  (`Status`), the run plan from the node, retries after RFC 6298 doubling with equal jitter at most
  three attempts, the CLI's journal of open uploads (`crates/mantle/src/journal.rs`, CRC-checked,
  flushed, renamed, directory flushed, at most 1,000), pooled stream credit (node.md §2.8).
- Not built (gateway.md:748–750): the journal in the library and its power-cut tests; credits and
  the admission level on responses; path adaptation and migration on interface change; per-block
  GET streams with CRC-64/NVME; the validated caches; the TLS-over-TCP route.
- TLS session resumption: the S3 listener turns it off explicitly
  (`crates/node/src/listener/mod.rs:173–174`: `NoServerSessionStorage`, `send_tls13_tickets = 0`).
  The vendored hyper-tls is rustls-derived and has tickets (`vendor/hyper-tls/src/ticketer.rs`), and
  hyper-quic validates resumption parameters (`vendor/hyper-quic/src/transport_parameters.rs:199`),
  so the listener half looks like mantle-only configuration. The QUIC half was not checked.
- Migration likely needs hyper-quic/hyper-rt-transport rebinding: unverified; flag as possibly
  "needs hyper-raft".

### 13. Resumable native uploads: PARTLY BUILT

- Built (gateway.md §2.1 as built 2026-10-26; `crates/mantle/tests/native.rs`): Open/Append/
  Offset/Finish, runs of whole blocks cut by the node (`name::run_length`), progress frames, resume
  from the node's offset, MD5 state carried across runs through the vendored aws-lc-rs export and
  import, `BadDigest` on a changed source, kills at a run boundary and mid-run of client and node.
- Remaining: the interval's measured terms (`g/r_meta`, Young's `g·sqrt(2h/λ)`); two runs in flight
  (the next run streaming while the last commits); block hashes besides MD5 (other digests' state);
  the Open's source identity, resume horizon and checksum algorithms beyond one CRC; SSE-C on the
  native protocol; the gateway closing a stalled run at its last whole segment; the operation
  identity's record (item 9: an Open asked again after a lost answer opens a second, empty upload).
- Depends on item 9 for the Open's record; touches the same Name rows (`crates/meta/src/name.rs`).

### 14. The HTTP/1.1 listener's resilience: NOT CHECKED IN CODE

From STATUS only (In progress, lines 817–826; node.md §4.5 open list): ListParts and
ListMultipartUploads are served (STATUS line 392); the lifecycle worker that would apply
`AbortIncompleteMultipartUpload` is not built (STATUS lines 295–296); open-upload bytes reporting,
completion's early `200` near client read timeouts, the connection budget growing past ten with
memory and reading the descriptor limit, slowest-connection reclaim, and ceph s3-tests' multipart
suite (s3-tests not yet run at all, node.md §9.2 as built) all remain. Pinned resumed downloads
(ranged GET with `versionId` or strong `If-Match`) not checked.

### 15–28 and the three frames: NOT REACHED

Only incidental facts gathered: hedging is not built (gateway.md §3 as built; items 8, 22); the
repair/rebalancing section of STATUS (lines 964–1006) lists the multi-machine frame's built parts
(repair by margin, reintegration, drains, spreading onto joiners) and remaining parts (deleting
chunks no block names, moves for disk space, sizing a machine's volumes, trimming kept places,
re-coding wide blocks, keeping rebuild time across leader change, repair priority, the default
repair time, end-to-end scenarios on Linux and Windows). Membership's remaining parts (STATUS lines
146–163): a range's voters a subset of the cell, certificate rotation, owner's decisions in
membership.md §10 (not read).

## Work packages proposed so far (from reached items only)

Conflict hotspot: `crates/node/src/node/host.rs` (5,752 lines) is on the path of items 6–9 and
12–14. Packages below that touch it are marked; run at most one of them at a time, or split them so
only one edits host.rs.

| ID | Builds | Crates and modules | Done when | Machines | Blockers |
|---|---|---|---|---|---|
| P1 Operation records | item 9 | meta: `name.rs`, `key.rs`, `record.rs`, `collector.rs`, `wire.rs`, `tests/orphan_sweep.rs`; node: `node/host.rs` (answer from the row); mantle: a real-process test | orphan-sweep sim holds the five obligations with every attempt delivered at any step across gateways, splits and lagging clocks, each broken variant failing; a node killed between commit and answer in a versioned bucket leaves one version after a retry through another node | Mac; CI all targets | owner: the server's maximum horizon budget (or a derivation in `derive.rs`); touches host.rs |
| P2 Principal fair queues | item 6, admission half | node: new fair-queue module, `admit.rs`, `door.rs`, `node/host.rs`; meta range command queue (node.md §2.3) | unit/property tests: SFQ state bounded by admitted work under a new principal per request; Space-Saving error within N/m; laptop-scale owner's scenario for principals (needs a sim harness extension in `crates/node/tests/sim.rs`) | Mac; CI | owner: tenants/contract classes before mClock and Aequitas; device half held by the other session; touches host.rs |
| P3 Overload by sojourn | item 7 | same as P2 plus `listener/request.rs` (`amz-sdk-request`) | open-loop steps to 1.5x and 10x capacity, corrected for coordinated omission, leave overload once the trigger stops, retries shed before first attempts | Mac; Linux for the network matrix later | sequence after P2 (same files); `x-amz-retry-after` open (node.md §11) |
| P4 GET window | item 8 (b) | node: `window.rs`, `derive.rs` (`gateway.window`), `node/host.rs` get path; gateway: `get.rs`; docs: constants.md row; `scripts/arbitrary-open.txt` line removed | GET window = ceil(R·T/B) from measured read rate and chunk read time, constants gate shows it derived | Mac; CI | touches host.rs; the entitled-rate term waits on P2 |
| P5 Library journal | item 12 (journal + power-cut tests) | client: `src/lib.rs` + a journal module; mantle: `src/journal.rs` moved under the library | library killed and its storage cut at every journal write and protocol step: every operation answered, resumed with no committed run resent, or provably restarted | Mac; CI | needs P1 for "answered from the server's record" of non-upload operations |
| P6 TLS resumption on the S3 listener | item 12 / STATUS listener list | node: `listener/mod.rs`, `listener/io.rs`; tests in `crates/node/tests/listener.rs` | a stock client's reconnect resumes without a full handshake, no early data accepted; ticket key rotation stated | Mac; CI | none found; QUIC-side resumption unchecked (may need hyper-raft) |
| P7 Client memory per client | item 3 (a) | mantle: `bench.rs`, `bench_gateway.rs`, `bench_log.rs` | benches report resident memory at two client counts and the slope S (measurement.md §10) | Mac | none |
| P8 Resumable uploads, rest | item 13 remainder | client `lib.rs`; protocol `object.rs`; node `node/host.rs`; meta `name.rs` | two runs in flight; the interval's measured terms; other digests' state; Open carries source identity and horizon | Mac; CI | after P1 (same Name rows) and P5 (same client file); touches host.rs |

Held by the other session: items 2, 4, 5, 10 (device parts), item 6's device dispatcher half,
item 3 (c), and probably item 11's scrubber side.
