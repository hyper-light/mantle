# 91 — Snapshots made when a member needs one, not at compaction

Status: research note, 2026-11-20, for docs/design/replica.md §4. It gathers what the primary
sources say about when a Raft replica should make the image it sends a member behind its log's
start: at log compaction, or only when the core asks for one. Quotations from code were read in
the sources' own text on 2026-11-20 at the branches named (line by line, as the files state
them); the dissertation is paraphrased, its section given. Analysis is marked as such.

## 1. Sources

- **[RAFT-RS]** tikv/raft-rs, branch `master`: `src/storage.rs` (trait `Storage`, `fn snapshot`)
  and `src/raft.rs` (`prepare_send_snapshot`). hyper-raft keeps raft-rs's wire and storage
  contract (research/32).
- **[ETCD]** etcd-io/raft, branch `main`, `storage.go` (`ErrSnapshotTemporarilyUnavailable`,
  `Storage.Snapshot`): the contract raft-rs ported.
- **[TIKV]** tikv/tikv, branch `master`, `components/raftstore/src/store/peer_storage.rs`
  (`PeerStorage::snapshot`, which `impl Storage for PeerStorage` calls).
- **[DISS]** D. Ongaro, "Consensus: Bridging Theory and Practice", PhD dissertation, Stanford
  University, 2014, ch. 5 ("Log compaction"); §5.1.2's rule is quoted in hyper-durable's
  `src/compaction.rs`.

## 2. The storage contract: a snapshot may be temporarily unavailable

[RAFT-RS] `Storage::snapshot(&self, request_index: u64, to: u64)`:

> Returns the most recent snapshot.
> If snapshot is temporarily unavailable, it should return SnapshotTemporarilyUnavailable,
> so raft state machine could know that Storage needs some time to prepare
> snapshot and call snapshot later.
> A snapshot's index must not be less than the `request_index`.
> `to` indicates which peer is requesting the snapshot.

[RAFT-RS] `prepare_send_snapshot`, on that error: it logs at debug, "failed to send snapshot to
{} because snapshot is temporarily unavailable", and returns `false`. Nothing is sent and the
member's progress is left as it was, so the leader tries again at its next send to that member
(Analysis, from the same function).

[ETCD] states the same: `ErrSnapshotTemporarilyUnavailable` "is returned by the Storage interface
when the required snapshot is temporarily unavailable", and `Snapshot()` "returns the most recent
snapshot. If snapshot is temporarily unavailable, it should return
ErrSnapshotTemporarilyUnavailable, so raft state machine could know that Storage needs some time
to prepare snapshot and call Snapshot later."

hyper-raft keeps the contract: `Outbox::snapshot` (`vendor/hyper-raft/src/raft.rs`) answers
`Ok(false)` on `StorageError::SnapshotTemporarilyUnavailable`, and a member's next answer to a
heartbeat sends to it again (`handle_heartbeat_response` calls `Outbox::append` while the member
lacks entries).

**Finding 1.** The core does not need a snapshot ready before it asks: storage may say it is
being made, and the core asks again. A snapshot made at compaction "in case" is not what the
contract asks for.

## 2a. Once storage holds what it said it did not, the owner has the core send it

Read on 2026-11-20 in raft-rs at `8e4cef172421bf77b2ae1c26628a9531b0be41f0`, the pin hyper-raft's
differential runs raft-rs at (vendor/hyper-raft/ORIGIN.md).

[RAFT-RS] `Storage::entries`, `src/storage.rs`:

> Entries are supported to be fetched asynchronously depending on the context. Async is optional.
> Storage should check context.can_async() first and decide whether to fetch entries asynchronously
> based on its own implementation. If the entries are fetched asynchronously, storage should return
> LogTemporarilyUnavailable, and application needs to call `on_entries_fetched(context)` to trigger
> re-fetch of the entries after the storage finishes fetching the entries.

[RAFT-RS] `RawNode::on_entries_fetched`, `src/raw_node.rs`, "A callback when entries are fetched
asynchronously": for a context of `GetEntriesFor::SendAppend { to, term, aggressively }` it returns
when "term or leadership has changed" or "the peer has been removed", and otherwise calls
`self.raft.send_append(to)`.

[RAFT-RS] `RawNode` has no such call for a snapshot (none of its public methods at that commit
takes one made): the core's next try is its next send to the member, as on the member's answer
to a heartbeat (`handle_heartbeat_response`, `src/raft.rs`):

> if pr.matched < self.r.raft_log.last_index() || pr.pending_request_snapshot != INVALID_INDEX
> {
>     self.r.send_append(m.from, pr, &mut self.msgs);
> }

hyper-raft's `RawNode::send_append(member)` is `on_entries_fetched`'s call: "A leader sends
`member` what follows its progress, if it may be sent now: what its owner asks once storage has
read the entries it said it did not hold yet (`StorageError::LogTemporarilyUnavailable`, which
leaves the member's progress as it was), as raft-rs's `on_entries_fetched` does. Any other role
sends nothing." (`vendor/hyper-raft/src/node.rs`). hyper-durable's catch-up from the log's file
calls it for each member whose page the store has read (`Replica::stream_catch_up`).

**Finding 4 [Analysis].** A snapshot answered temporarily unavailable leaves the member's
progress as it was (§2), the same state a page answered `LogTemporarilyUnavailable` leaves, so
the same call sends it once made: the owner need not wait for the core's next try, which is a
heartbeat's answer away, and never comes where nothing beats.

## 2b. After a snapshot, the member's answer, and a heartbeat's when it is lost

Read on 2026-11-20 in raft-rs at `8e4cef172421bf77b2ae1c26628a9531b0be41f0` (as §2a).

[RAFT-RS] `RawNode::report_snapshot`, `src/raw_node.rs`: "ReportSnapshot reports the status of
the sent snapshot." It steps a `MsgSnapStatus` from the member, `reject` set for
`SnapshotStatus::Failure`.

[RAFT-RS] `handle_snapshot_status`, `src/raft.rs`: a progress in the snapshot state becomes a
probe either way (`pr.become_probe()`, after `pr.snapshot_failure()` on a failure), then:

> // If snapshot finish, wait for the msgAppResp from the remote node before sending
> // out the next msgAppend.
> // If snapshot failure, wait for a heartbeat interval before next try
> pr.pause();
> pr.pending_request_snapshot = INVALID_INDEX;

[RAFT-RS] `handle_heartbeat_response`, `src/raft.rs`: every heartbeat's answer resumes the
member's progress (`pr.recent_active = true; pr.resume();`) and, while it lacks entries, sends to
it (§2a's quote).

**Finding 5.** A member sent a snapshot is replicated to again on its answer, and the answer may
be lost as any message may (Ongaro's dissertation §3.5: Raft retries a lost RPC); what recovers
it is the leader's heartbeat, whose answer resumes the paused probe. A leader that never beats
leaves the probe paused for good once that answer is lost. hyper-raft's answers to heartbeats say
where the member's log ends (`HeartbeatAnswers::Position`) and are taken as an append's answer, so
one beat recovers it; under elections by suspicion a leader beats only on a round its owner gave
(`Raft::wake_leader`), and a sole voter was given none (mantle docs/design/replica.md §4).

## 3. TiKV makes the image on demand, in a worker

[TIKV] `PeerStorage::snapshot`, when no snapshot is ready and the requesting peer is in the
region, sets `*snap_state = SnapState::Generating { .. }`, builds `GenSnapTask::new(..)` and
hands it off with `self.set_gen_snap_task(task)`; while it is generating (`TryRecvError::Empty`
on its receiver) and at the end of the call, it returns
`raft::StorageError::SnapshotTemporarilyUnavailable`. Not read for this note, and to be checked
against the source before it is quoted: that the task runs on a worker apart from the raftstore
thread, and that TiKV's log garbage collection (its `CompactLog` admin command) makes no snapshot.

**Finding 2.** A production Raft store with a disk-resident state machine images only for a
member that asks: its `Storage::snapshot` starts the image and answers "temporarily
unavailable" until it is made.

## 4. Snapshotting and log truncation are separate for a disk-based state machine

[DISS] §5.1 describes snapshotting for state machines held in memory, where the snapshot is what
lets the log be truncated. §5.2 ("Snapshotting disk-based state machines") treats a state machine
whose state of record is on disk: applying an entry changes the on-disk state, so once the state
machine's writes are on disk the entries before them may be discarded from the log, and a
consistent image of that state is needed only to send to a follower that is too far behind
(paraphrase).

**Finding 3 [Analysis].** Mantle's range engine is a disk-based state machine (a persisted engine
whose checkpoint is its durable index), so its compaction is §5.2's: make applied state durable,
then move the log's start. Imaging the state at every compaction is §5.1's cost paid by a
machine that does not need it.

## 5. What hyper-durable does with it (Analysis)

- A compaction persists the machine and moves the log's start; it reads no rows. The compaction
  rule's image size comes from `StateMachine::image_bytes`, which reads no state.
- `Held::snapshot` serves a held snapshot only if it reaches the core's `request_index` and the
  log's start; else it records the index needed and answers `SnapshotTemporarilyUnavailable`.
- The replica's next drive makes the image then (`Replica::make_wanted`), at the point the
  machine applied, with the configuration held there, and in the same drive asks the core to send
  it to each member behind the log's start it may send to now (`Replica::send_made`, through
  `RawNode::send_append`; Finding 4), not at the core's next try.
- Open: the image is still made in one step on the node's shard when it is asked for. The next
  step is TiKV's shape: build it off the shard, or in bounded slices (docs/design/replica.md §7).

## 6. Making the image without holding the shard (Analysis, 2026-11-20)

Ground truth read in mantle's engine at the time of this note:

- `ShardDb::scan` (crates/engine/src/shard_db.rs): "As S3's listings page, so no scan state lives
  across writes." A page's continuation is a key; nothing pins the rows a scan read.
- Every read of a `ShardDb` takes `&mut self` (its caches, its memtables' seals, its loads), so the
  engine is one owner's, its shard's; no other thread reads it.
- A checkpoint is the durable image of the trunk at an index (`ShardDb::checkpoint_now`), but the
  engine keeps no reader's hold on its extents, which later work reuses once a later checkpoint is
  durable.

So neither option the task named as "off the shard" has a view to read from without new engine
work. In slices, a cursor over the live engine reads one point only while nothing writes it: the
replica already holds committed entries unapplied while its machine has no room
(`StateMachine::apply` answering false, `RawNode::pause_apply`), bounded by the core's committed
page and the log's room, and the same hold keeps the range's state at the image's point while
the slices are read. The slice's bytes are the drive's committed page
(`max_committed_size_per_ready`), the quantum a drive's applies take, so the owner's round robin
stays fair within one unit of work (Shreedhar and Varghese, SIGCOMM 1995, Theorem 4.5, as
hyper-durable's `owner.rs` cites it).

