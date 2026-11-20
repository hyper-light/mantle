# 100 — Holding the log for a member's image

Status: research note, 2026-11-20, for docs/design/replica.md §4 and §7. It gathers what the
primary sources do about the entries a member being sent a snapshot needs once it installs it:
does compaction keep them, what sets and releases that, and what bounds it. Code was read in the
sources' own text on 2026-11-20 at the commits named; the dissertation is quoted with its page.
Analysis is marked as such.

## 1. Sources

- **[CRDB]** cockroachdb/cockroach, master at `d30c905fff79ef825adc96bcc647f1872a90f2ff`:
  `pkg/kv/kvserver/raft_log_queue.go` (`computeTruncateDecision`, `newTruncateDecision`),
  `replica_raft.go` (`addSnapshotLogTruncationConstraint`,
  `getSnapshotLogTruncationConstraintsRLocked`), `replica_command.go`
  (`sendSnapshotUsingDelegate`, `sendSnapshotTimeout`), `pkg/base/config.go`.
- **[ETCD-RAFT]** etcd-io/raft, main at `ae830f4a15a0bdb5f59bb7988e50663e92cc028f`:
  `tracker/progress.go`, `raft.go`, `storage.go`.
- **[ETCD]** etcd-io/etcd, main at `f872eb465ed3d2d056ec16742a1a2f8e322a5751`:
  `server/etcdserver/server.go` (`compactRaftLog`, `sendMergedSnap`).
- **[TIKV]** tikv/tikv, master at `4d5e1a0968d72deb5aecd65dfd16176b9a01e080`:
  `components/raftstore/src/store/fsm/peer.rs` (`on_raft_gc_log_tick`),
  `components/raftstore/src/store/peer_storage.rs` (`cancel_generating_snap`).
- **[DISS]** D. Ongaro, "Consensus: Bridging Theory and Practice", PhD dissertation, Stanford
  University, 2014, ch. 5.

## 2. CockroachDB holds the log at a pending snapshot's index

[CRDB] `computeTruncateDecision`'s comment states the failure this note is about:

> Consider what would happen if we truncated to the commit index whenever a node is behind and
> thus needs to be caught up via a snapshot. While we're generating the snapshot, sending it to
> the behind node and waiting for it to be applied we would continue to truncate the log. If the
> snapshot generation and application takes too long the behind node will be caught up to a point
> behind the current first index and thus require another snapshot, likely entering a never ending
> loop of snapshots. See #8629.

(#8629: "stability: repeatedly applying snapshot for the same range", 2016.) The protection:

> The pending snapshot index acts as a placeholder for a replica that is about to be added to the
> range (or is in Raft recovery). We don't want to truncate the log in a way that will require that
> new replica to be caught up via yet another Raft snapshot.

`if snap := input.PendingSnapshotIndex; snap > 0 { decision.ProtectAfter(snap, ...) }`. Its test
(`raft_log_queue_test.go`): "Don't truncate away pending snapshot, even when log too large."

What sets it: `sendSnapshotUsingDelegate` calls `addSnapshotLogTruncationConstraint` at the
leader's applied index before the snapshot is made: "This log truncation constraint is held until
the snapshot has been delivered to the end recipient, or fails trying", and "The cleanup function
needs to be called regardless of success or failure of sending to release the log truncation
constraint." On success the recipient has applied the snapshot and the leader has stepped its
answer before the send returns; the follower is then protected by the follower rules ("we never
truncate off a recently active follower"). A three-second grace period after completion was
removed in 2022 (`141f28b7`), the race it covered being gone.

Its bound is time alone, `sendSnapshotTimeout`, one hour by default:

> While a snapshot is in transit, Raft log truncation is halted to allow the recipient to catch
> up. If the snapshot takes very long to transfer for whatever reason this can cause the Raft log
> to grow very large. We therefore set a conservative timeout to eventually allow Raft log
> truncation while avoiding snapshot starvation.

No size bound applies to the pending snapshot; the 16 MiB `LogTooLarge` threshold only stops
protecting followers that are not recently active.

## 3. etcd pauses compaction while a snapshot is in flight

[ETCD] `compactRaftLog`:

> When sending a snapshot, etcd will pause compaction. After receives a snapshot, the slow
> follower needs to get all the entries right after the snapshot sent to catch up. If we do not
> pause compaction, the log entries right after the snapshot sent might already be compacted. It
> happens when the snapshot takes long time to send and save. Pausing compaction avoids
> triggering a snapshot sending cycle.

`sendMergedSnap` counts the snapshot in flight and, once sent, holds it "for another 30 seconds to
block log compaction. If the follower still fails to catch up, it is probably just too slow to
catch up. We cannot avoid the snapshot cycle anyway." A failed send releases at once. etcd also
keeps `SnapshotCatchUpEntries` (5,000) behind every compaction. [ETCD-RAFT] holds nothing itself:
`MemoryStorage.Compact` leaves it to the application not to compact past what it applied, and
`PendingSnapshot` is never used to limit compaction.

## 4. TiKV keeps the log behind the least match, bounded by size

[TIKV] `on_raft_gc_log_tick` compacts to the least `matched` of the peers (`replicated_idx`), so a
peer being sent a snapshot, whose match stays behind the log's first index, keeps the log until it
applies the snapshot and its match jumps to the snapshot's index. Past `raft_log_gc_count_limit`
or `raft_log_gc_size_limit` the log is halved whatever that peer holds. It also does the reverse
of holding: "If RaftEngine's compact idx exceeds the snapshot idx, means the snapshot idx has
already been compacted. We should cancel the snapshot generating as it is now out of date."
(`cancel_generating_snap`, which keeps one still valid: `idx <= snap_index + 1`).

## 5. The dissertation leaves it to implementations

[DISS] p. 51: "It is not feasible to defer compaction until log entries have been "fully
replicated" to every member in the cluster, since a minority of slow followers must not keep the
cluster from being fully available". Figure 5.3, p. 53: "Leaders resort to sending a snapshot only
when they have already discarded the next log entry needed to replicate entries to the follower
with AppendEntries." Chapter 5 names no hold for a snapshot in transfer and does not discuss
repeated snapshots.

## 6. What hyper-durable does with it (Analysis)

- **The hold.** A leader's compaction moves the log's start no further than the point of the image
  made for members behind the start, from the drive that begins it (at the machine's durable index
  then, which no image it makes is behind) through its transfer, and is held again at the point of
  any snapshot that leaves, as [CRDB] sets its constraint before the snapshot is made.
- **What releases it.** Every other member's match reaching the point (its install answered, as
  [TIKV]'s match and [CRDB]'s follower rules take over), a transfer reported failed (as [CRDB] and
  [ETCD] release on failure), the member's removal (it leaves the core's tracker), the lead's loss,
  and the bound.
- **The bound.** [CRDB] bounds by time and [TIKV] by size. hyper-durable's log has a hard room
  that stalls the group's writes once full, and a stalled replica takes no message, so neither the
  install nor its answer can arrive while it stands: a hold kept through a stall would hold the
  compaction the stall waits for, for good. The bound is the group's room: the stall lets the hold
  go, and the compaction ends the stall as it would with no image. The log is shared by a node's
  ranges, and the node's compaction pass for room, run when any range on it is stalled, keeps
  another range's hold while the pass frees room otherwise, and lets each hold that keeps back a
  compaction go, compacting again, only when no other compaction started (2026-11-20, below). Up
  to it the hold costs nothing beyond the entries a group may hold anyway.
- **A snapshot its start passed** is not sent ([TIKV]'s cancel): one queued while stalled is dropped
  as it leaves and reported failed, and the core asks again.
- **Open.** A group whose own writes fill its room within one image's making and transfer still
  stalls before the member installs, and the stall lets the hold go: such a member is still sent
  image after image (§7).

## 7. A stalled leader's sends and answers (Analysis, 2026-11-20)

Whether a leader stalled for room could send the snapshot and take its member's answer, so that a
hold could be kept through the stall:

- **Sending is sound.** The core lets a leader's queued messages leave while the term and vote it
  leads in are durable, whatever its writes out (hyper-raft `src/node.rs`, I1 and I2,
  `RawNode::releases_now`): what a leader sends, its members persist for themselves ([DISS]
  §10.2.1, "Writing to the leader's disk in parallel"; raft-rs gives a leader's messages in
  `Ready::messages`, sent before the `Ready`'s write, and a follower's in
  `Ready::persisted_messages`). A snapshot is of applied, so committed, state and depends on no
  refused write.
- **Hearing is sound.** An answer moves the member's progress, which is volatile, and may move the
  commit, which counts the leader only through its durable writes (I3; [DISS] §10.2.1: "The
  leader may even commit an entry before it has been written to its own disk, if a majority of
  followers have written it to their disks; this is still safe", research/06 A1.2).
- **It saves no image alone.** A group stalled on its own room is full of the entries after the
  image's point; its stall ends only once a compaction frees them, and those are what the member
  that installed the image needs next. Kept through the stall, the hold must also let the member
  be sent those entries and acknowledge them while the leader stays stalled: the stalled leader
  takes part in replication, which hyper-durable's stall rule refuses (docs/durable.md §2.4) and
  whose interplay with the refused writes made again (R-4, I7) needs its own proof. Not built.
- **The node's pass** (built): one log holds every range's entries, so a range stalled on the
  log's room is served by any range's compaction. The pass keeps a hold while other compactions
  free room, as [TIKV] keeps its log behind the least match until a size bound, the bound here the
  pass that finds no other room.
