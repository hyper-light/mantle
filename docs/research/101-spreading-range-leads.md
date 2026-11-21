# 101 — Spreading the ranges' leads over a cell's nodes

Status: research note, 2026-11-20, for docs/design/node.md §6.7. It gathers what the primary
sources do to keep a cluster's Raft leaders (or leaseholders) spread over its nodes: what they
balance, what they refuse to hand a lead to, and what bounds their moves; and what Raft's own
transfer does and costs. Code was read in the sources' own text on 2026-11-20 at the commits
named. Analysis is marked as such.

## 1. Sources

- **[DISS]** D. Ongaro, "Consensus: Bridging Theory and Practice", PhD dissertation, Stanford
  University, 2014, §3.10 "Leadership transfer extension"; read in its source,
  ongardie/dissertation at `0c10ea1b53d73bf6d163bdde8e4141a1cbd8f36a`, `basicraft/consensus.tex`.
- **[RAFT-RS]** tikv/raft-rs at `ad13f3d90780f53aea2488c6a4b76c0d334bf136`, `src/raft.rs`
  (`handle_transfer_leader`, `handle_append_response`, `tick_heartbeat`, `reset`, `step_leader`'s
  `MsgPropose`).
- **[CRDB]** cockroachdb/cockroach at `d30c905fff79ef825adc96bcc647f1872a90f2ff`:
  `pkg/kv/kvserver/lease_queue.go` (`MinLeaseTransferInterval`, `canTransferLeaseFrom`),
  `pkg/kv/kvserver/allocator/allocatorimpl/allocator.go` (`LeaseRebalanceThreshold`).
- **[PD]** tikv/pd at `832369c415d4f9ab86c8e1cd79bf583d342b1a84`:
  `pkg/schedule/schedulers/balance_leader.go`, `pkg/core/store.go` (`LeaderScore`); the PD
  configuration file reference (docs.pingcap.com, `schedule.leader-schedule-limit`).
- **[TIKV]** tikv/tikv at `4d5e1a0968d72deb5aecd65dfd16176b9a01e080`:
  `components/raftstore/src/store/peer.rs` (`ready_to_transfer_leader`); the TiKV configuration
  file reference (docs.pingcap.com, `raftstore.leader-transfer-max-log-lag`).

## 2. CockroachDB: leases toward the mean, a store's transfers a second apart

[CRDB] balances leaseholders (the replica that serves a range's reads and proposes its writes,
kept with the Raft leader) by count: `kv.allocator.lease_rebalance_threshold`, default 0.05, is
the "minimum fraction away from the mean a store's lease count can be before it is considered for
lease-transfers". Transfers for balance are spaced per store: `kv.allocator.min_lease_transfer_
interval`, default `1*time.Second`, "controls how frequently leases can be transferred for
rebalancing. It does not prevent transferring leases in order to allow a replica to be removed
from a range." `canTransferLeaseFrom` allows a transfer only once
`timeutil.Since(lastLeaseTransfer) > minInterval`, the time of the store's last transfer, and
lets one through at once when the leaseholder is outside the range's lease preferences or its
store is overloaded. Its comment names the purpose: the last transfer's time is considered "to
avoid thrashing". The allocator's lease checks take an `excludeReplsInNeedOfSnap` flag, which
leaves out replicas that may need a snapshot, i.e. are behind the leader's log.

What Mantle takes (analysis): the mechanism, a store's transfers for balance spaced by a least
time since its last, which keeps a store from thrashing; not the value. A second is CockroachDB's
tuning, and CLAUDE.md §4 takes no other system's tuning, so Mantle spaces its hand-overs, and has
an heir wait before one, by the range's patience, which its node measures (node.md §2.4, §6.7).
Mantle applies the spacing to every hand-over the spread begins, including a move to a range's
first choice, which CockroachDB lets through at once: one rule for every move.

## 3. TiKV PD balances leader counts; TiKV refuses a lagging or changing target

[PD]'s balance-leader scheduler compares stores' leader scores, `LeaderScore`: by default the
count of leaders a store holds over its leader weight (or the leaders' size, by policy). It sorts
source stores by score descending and targets ascending, and moves leaders from the first to the
second in batches (`BalanceLeaderBatchSize`, 4), each a transfer-leader operator, admitted only
while `OperatorCount(operator.OpLeader) < GetLeaderScheduleLimit()`: `leader-schedule-limit`,
default 4, "The number of Leader scheduling tasks performed at the same time". Targets pass a
`StoreStateFilter` with `TransferLeader: true` and a `SpecialUseFilter`; a region with pending
peers is passed over (`NewRegionPendingFilter`); and `shouldBalance` keeps small differences
from moving anything.

[TIKV] refuses to begin a transfer the target cannot finish: `ready_to_transfer_leader` answers
`"log gap"` when `last_index >= index + ctx.cfg.leader_transfer_max_log_lag` (the target's
matched index `index`); `leader-transfer-max-log-lag`, default 128, at least 10, is "The maximum
number of missing logs allowed for the transferee during a Raft leader transfer". The same check
refuses a `"non voter"`, a `"pending snapshot"` and a `"pending conf change"`.

What Mantle takes (analysis): the even count of leaders a node as the goal; the target's
fitness, as Mantle's heir's (trusted by the node's detectors, not announced stopping, its disk not
judged slow); the lag check, stricter in Mantle (no entry behind what the range committed, as its
entries are batches of commands); no transfer while a configuration changes; and a bound on the
transfers out at once, here one a node.

## 4. Raft's transfer: catch the target up, `TimeoutNow`, give up after an election timeout

[DISS] §3.10's steps: "The prior leader stops accepting new client requests"; it "fully updates
the target server's log to match" its own; and it "sends a TimeoutNow request to the target
server", which "has the same effect as the target server's election timer firing". "If leadership
transfer does not complete after about an election timeout, the prior leader aborts the
transfer" and resumes accepting client requests. The section names the case this note is about:
"In some cases, one or more servers may be more suitable to lead the cluster than others", for
example "a server with high load would not make a good leader". It also says "we have not
currently implemented or evaluated this leadership transfer approach".

[RAFT-RS] implements it so. On `MsgTransferLeader` the leader ignores a request for the target it
is already transferring to, and aborts a transfer to another target before beginning the new one;
it sends `MsgTimeoutNow` at once when `pr.matched == self.r.raft_log.last_index()` and an append
otherwise, then `MsgTimeoutNow` once the target's answer shows it caught up. It sets
`election_elapsed = 0` ("Transfer leadership should be finished in one electionTimeout"), and
`tick_heartbeat` aborts the transfer once `election_elapsed >= election_timeout`; a new term
(`reset`) aborts it too. While `lead_transferee` is set a proposal is dropped,
`Error::ProposalDropped`. hyper-raft keeps raft-rs's transfer (`RawNode::transfer_leader`,
`Raft::lead_transferee`), and hyper-durable's `Replica::transfer` calls it.

What Mantle takes (analysis): each hand-over costs its range its proposals for about a round trip
and an election at most, which the commands' sessions ask again (node.md §5.4), so hand-overs must
be few and bounded; and the core gives a transfer up on its own, so the caller need only begin
none to a target that cannot win (behind, unreachable, or mid-change) and count those that end
otherwise.

## 5. What Mantle does, and why a ranking rather than counts

Analysis. A Mantle cell holds exactly one range of each of five layers (node.md §6.6), every node
of a cell of three a voter of every range. CockroachDB and PD gather counts across the cluster
because their ranges are many, made and split at run time, and placed on different replica sets;
Mantle's are few, fixed and named in the configuration, so the even count can be stated in advance
as a ranking each range's members derive alike from the range's own configuration: the range at
place `k` of `n` voters ranks first the voter at `k mod n` in ascending order of id, then the rest
in turn, upward when `k / n` is even and downward when odd. Three voters' first choices are two,
two and one; with one down, three and two; five voters' one each. The ranking says where a lead
goes, and PD's counts say whether: a leader hands only to a voter ranked above itself that leads
two or more fewer ranges than its own node, by the leaders the ranges it hosts name, since one
move narrows a gap of two or more and none smaller (PD's tolerance, at its least). The rank of a
range's leader only rises while the cell's health holds: at most `n − 1` moves a range, and no
flapping; a range alone never moves; and five ranges over three nodes settle two, two and one from
any leads, as every one of the 243 assignments does in `node::lead`'s test. Elections drawn
independently a range would leave a node leading none of five ranges in 38% of cells of three
(`3·(2/3)^5 − 3·(1/3)^5`), and a ranking by a hash of (range, node), rendezvous hashing, the same
odds; hence a ranking by place. The measured cause and the cell's numbers are node.md §6.7's.
