# Range replicas: how a range runs its log

Status: design, 2026-09-28. Sources: docs/research/06 (consensus, "06 §x"), 07 (focal's
consensus stack); docs/design/metadata.md, whose ranges this runs, and raft-log.md, the log
it writes to.

A replica is one member of one range's Raft group on one node. It holds focal-raft's core
(`RawNode`, 07 §1.2), its group's view of the device's log, the range's engine, and the state
machine of the range's layer: Bucket, Name, File or Block (metadata.md §1). It turns what the
core asks for into log submissions, engine batches and messages, in the order Raft's safety
needs.

## 1. What an entry carries

A normal entry carries a batch of commands, each from a gateway. Batching many commands into
one entry, and many entries into one flush, is where a consensus log's throughput comes from
(06 §A9). A configuration change is focal's `ConfChangeV2` entry.

Each command names the gateway's session, its serial number, and the lowest serial number
whose response the gateway has not yet received, following the Raft dissertation's client
sessions (06 §A1.8):

- **Registration is a command.** A gateway registers with a range before its first command,
  and the session's ID is the index of the entry that registered it, which no other entry
  shares.
- **A duplicate is answered, not applied.** The range keeps, per session, the responses to
  commands at or after the lowest serial number not yet acknowledged, and answers a
  duplicate from them. So a command takes effect "exactly once according to [its] first
  appearance in the Raft log" (06 §A1.8), including when the gateway retries after its
  leader fails, or re-proposes an entry the fast track displaced (07 §1.4).
- **Expiry is decided by the log.** Every entry carries the leader's time when it was
  proposed, as LogCabin's leader stamps its entries (06 §A1.8), and sessions unused for the
  range's session lifetime expire at the entry that passes it. Every replica decides the same.
  A range also holds at most a configured number of sessions, and registering past it
  expires the one least recently used. A command whose session is unknown or expired is
  refused, never run in a new session, since running it could apply it a second time.

Sessions live in the engine beside the range's rows, so they change in the entry's batch
and survive a restart with them. An entry reads each session it names once, at its first
command, checks and changes it in memory command by command, and writes it once at the end;
a registration, which may expire the session least recently used by the order of last use,
first writes the sessions changed so far. Each command read, searched and wrote back every
answer its session kept: an entry of 256 commands from a session keeping 256 answers took
2.6 ms and takes 328 µs, and its answers and rows are the same, which a test checks against
the command-by-command order over random entries (audit P04;
[measurements](../measurements/2026-09-29-metadata-apply.md)).

A session forgets an answer only once its gateway has acknowledged it, by naming a lowest
unanswered serial past it; that serial is the session's watermark, and a serial below it is a
repeat. In the dissertation, the client piggybacks the lowest sequence number it has no
response for, and that is what lets the state machine discard older responses (06 §A1.8);
nothing else may. Commands reach the log in any order, a
gateway's retries and the fast track's re-proposals among them, so the answer kept longest
need not be the lowest serial: forgetting it for room, as the range once did, raised the
watermark past serials still in flight, which were then taken for repeats and never applied.
A session keeps its unacknowledged answers within a count and a budget of bytes, and past
either it expires, as the dissertation bounds sessions: the command that passed the bound is
answered, and every command after, retries and serials still in flight alike, is refused as
from an expired session, never applied, so the gateway registers anew knowing which commands
took effect.

Sessions filter duplicates only within a session, and a command can outlive its own: one in
flight when its session expired has an unknown outcome, and the dissertation leaves the
client an error and nothing more, LogCabin crashing the client (06 §A1.8). A gateway that
re-sends such a command in a new session, or a copy delayed past its session's end, reaches
the range as a new command. So every Name-range write that carries a file is idempotent by
the file itself, not by its session. The gateway makes a file for each write attempt and
never reuses one (metadata.md §2), and the range marks every file it takes with how it took
it: the version a PUT or completion made, a part, a part adopted by a composite, or a file
released because its write was refused. The mark stays until the collector has reclaimed the
file. A write whose file is already marked is a copy. It is answered as the first was, the
version's ID or `PartWritten`, or `Expired` if the first was refused, and it neither takes
nor releases anything. Without this, a part re-sent after its completion was refused
`NoSuchUpload` and released a file the object's version held, and a PUT re-sent made a second
version of one file, which a delete of either released under the other. The rule for a
gateway is the dissertation's: after `SessionExpired`, register anew and re-send the command
unchanged, carrying the same file, and take its answer. A write that carries no file, an
empty object's PUT, has nothing to recognise it by: re-sent, it makes another version, as a
client's own retry of a PUT does in S3.

## 2. Applying an entry

The commands of one entry apply as one engine batch at the entry's index. That keeps the
engine's applied index at an entry boundary, so replay after a crash starts cleanly at the
first entry after it (metadata.md §3). Within the entry, each command reads what the
commands before it wrote, through an overlay that buffers the entry's writes over the
engine and answers reads from both. A command whose effects the layer refuses, a failed
precondition say, answers its refusal and writes nothing. A committed entry that does not
decode, or whose command breaks the state machine, fences the replica. Every replica applies
the same log, so the break is in the data. The replica then recovers from its peers, and the
range carries on without it.

Applying never reads the clock, the network or anything else a replica does not share.
Time comes from the entry, which is what lets every replica reach the same rows.

## 3. The order of a `Ready`

For each `Ready` the core gives, the replica does these steps in order:

1. Installs the `Ready`'s snapshot, if it carries one. The engine takes the snapshot's rows
   and makes them durable before the log records the snapshot's index as the group's start,
   so the log never starts past what the engine holds. The engine keeps the snapshot's
   index and term beside its rows. If the log write then fails, the replica finishes the
   install on restart: the engine stands at the snapshot, which is committed state, and
   the failed write was never acknowledged. So the log starts at the snapshot, holds nothing
   past it, and records it as committed.
2. Sends the messages a leader may send before its own write. The core marks them apart
   from those that must wait (07 §1.2). A leader writing in parallel with its followers is
   Ongaro's §10.2.1 optimization (06 §A1.9).
3. Submits the `Ready`'s entries, hard state, snapshot point and fast-track proposals to the
   log as one update (raft-log.md §2), and waits for it to be durable. The log waits for
   room in its queue rather than refusing a replica's update, since the core takes no other
   call while a `Ready` is out (raft-log.md §3). An update larger than a frame goes in parts
   that each fit, its entries first and its hard state last, the order etcd's raft writes a
   `Ready` in when the store cannot write it atomically (06 §A10.3); the `Ready` is done once
   every part is durable, so a crash between parts leaves entries never acknowledged.

   The log may still refuse a part for want of room another write frees: the group retains
   past its bound, every segment holds live records, or the log holds all the groups it
   may. Then the `Ready` waits in the replica, whole, with its parts not yet written, and
   `drive` reports the refusal. The `Ready`'s committed entries are applied at once, since
   the core gives out only entries committed and durable, so the group can compact past
   them. Until the node frees room, compacting the group or its neighbours on the log, and
   drives again, the replica refuses every call with `Stalled` and lets no tick move its
   timers: a member waiting for room takes no part in the group. Messages and ticks held
   while the update flushed, before the log refused a part, stay held until the `Ready` is
   done.
4. Sends the messages that must follow durability: a follower's acknowledgements and votes.
5. Applies the committed entries (§2).
6. Tells the core, which may hand over more committed entries and messages.

`drive` does these steps and waits at step 3 for the update to be durable. `begin` does not
wait: it gives out step 2's messages and the reads confirmed, submits the update, applies the
entries already committed, and returns with the `Ready` still out and `persisting` set, so the
node sends the messages while the log flushes, and a node driving many ranges submits every
range's update before the one flush that makes them all durable. A later `begin` or `drive`
finds the update durable and does steps 4 to 6; `begin` never looks at an update it has just
submitted, since that flush is not done microseconds later and whether it happened to be
would make what `begin` gives out depend on the log's thread. A range still has one `Ready`
out at a time, and a follower's acknowledgement never leaves before its write is durable.
Before, `drive` returned only after the flush, so the messages a leader may send during it
waited for it (audit §5.1). The simulation takes half its members' `Ready`s this way,
leaving about 170 a run flushing across a step, and checks linearizability as before. What
the node's scheduler owes the ranges it drives, time for heartbeats, reads and applying
beside the flushes it waits on, is the node's, which is not built (STATUS).

**What comes while a `Ready` flushes.** focal-raft's core takes no call while a `Ready` is
out: every mutation returns `Invariant("an operation while a ready is out")`, and
`advance_append` refuses a term or vote that moved meanwhile (07 §1.2; focal-raft
`src/node.rs`, `RawNode::operate` and `advance_append`). focal's own durable shell refuses
the same calls with a retryable `PersistencePending` and says the host is to "retain or
requeue [its] input", keeping no second ingress queue itself (07 §2.3; focal-consensus
`README.md`). etcd's raft, whose semantics focal ports, keeps taking messages, proposals and
ticks between handing out a `Ready` and its `Advance`: its node loop stops only offering the
next `Ready` while one is out (etcd-io/raft `node.go`, `func (n *node) run`, fetched
2026-09-30; docs/research/06 §A10.3).

The replica is that host, and it refused instead: while a `Ready` was out, `step` returned
`Stalled` and ticks were dropped. Under steady load a node overlapping its flushes finds
each member's `Ready` out whenever the network next delivers, so every acknowledgement
reached a leader that was flushing and was lost, and the leader's clock stood still, sending
no heartbeat. `a_leader_whose_answers_come_while_it_flushes_still_commits`
(`crates/range/tests/group.rs`) drives three members that way for 200 rounds: before, none
of the leader's 200 proposals committed and its followers' match stayed at 1; now the leader
takes 101, flushing between them, and 99 commit, the last two still in flight. `ticks_while_a_ready_flushes_still_count` shows a leader whose every tick lands
during a flush sending no heartbeats before, and heartbeats to both followers now.

So while a `Ready`'s update flushes, the replica holds the messages and ticks it is given,
in the order they came, and takes them once the `Ready` is done, before it takes the next
`Ready`: snapshots' reports first, as before, then each message after the ticks that came
before it. Ticks are never refused, and are held up to the longest election timeout the core
draws, twice `election_tick` (focal-consensus `set_randomized_election_timeout`): the core
acts on a timer at most once in that many ticks, and a device stalled for many timeouts
replayed each of them, a follower asking for votes 116 times in one burst
(`ticks_held_through_a_long_flush_replay_at_most_one_timeouts_worth`). Past that bound a tick
is counted as already held. Messages hold at most, for each other
member of the configuration, one flow-control window of appends: `max_inflight_msgs` of
`max_size_per_msg` bytes and one entry past it (`max_entry_bytes`), which the core admits
alone, each message charged its encoding and its in-memory frame. A member's heartbeats,
votes and answers are small beside that. A message past the bound is refused with
`MessagesHeld`, as the network may drop it: Raft's messages may be lost, and its senders
retry (Ongaro and Ousterhout, ATC 2014, §5.1). Proposals, reads and campaigns are still
refused with `Stalled` while a `Ready` is out; their callers retry. A snapshot's report is
kept, the latest for each member, even while the replica waits for room: replication to a
member pauses until its snapshot's fate is known, and a report refused and lost left the
member paused for good (a seed of the simulation found it).

**Reads.** A replica confirms reads a round at a time. One round is out at a time, carrying
every read asked before it began, and one quorum's heartbeats confirm them all at the commit
index the round began at. A read asked while a round is out waits for the next: that round's
index may predate a write committed since, whose client may already have its answer, and the
read must see it (06 §A1.5). Before, each read asked for its own round of heartbeats. A round
whose confirmation never comes, its leader gone or its heartbeat lost, is dropped after an
election timeout's ticks with its reads, as a lost message's reads are, and those waiting go in
a new round; the reads waiting hold at most an entry's bytes of contexts, past which a read is
refused to be asked again. `a_read_asked_while_a_round_is_out_waits_for_the_next`
(`crates/range/tests/group.rs`) holds a round's heartbeats while a write commits: the read
asked after it is confirmed at or past the write, and letting it join the round out confirms
it before the write. Without the bound on a lost round, reads in the simulation never finish
(audit §5.5). Leases are not used.

The transport tells the sending replica whether each snapshot arrived. A leader stops
replicating to a member while that member's snapshot is out, so a lost snapshot left
unreported would stall the member for good; the simulation found this. A snapshot's own
stream always learns its fate.

**Bounds a member checks at open.** Every member of a range runs the same settings, so an
entry the leader takes must fit one frame of every member's log: the range bounds an entry
(`max_entry_bytes`), the leader refuses a larger proposal, and a member whose log's frame
cannot hold one refuses to open. A group compacted to its applied state retains at most one
`Ready`'s entries, a leader's uncommitted proposals or the appends in flight to a follower,
and one entry past either bound, which the core admits alone; a member whose log bounds a
group below that, in bytes or in entries of the fewest bytes, refuses to open, since a
`Ready` refused for room there could wait for good.

## 4. Compaction, snapshots and restart

The engine makes applied state durable on its own schedule. When it has, the replica writes
the durable index to the log as the group's new start, keeping a window of entries behind it
for followers that lag. The log then frees the entries before it (raft-log.md §5; 06 §C.b.2).
A member that needs entries from before the start is sent a snapshot instead: every row of
the range as of the index where the replica last compacted, or opened a compacted log, with
the configuration there. A snapshot's index is at or past the log's start, so the member
continues from the entries after it. For now the rows travel inside the snapshot message.
The production engine will move its files out of band instead (12 §6.3).

On restart, the engine opens at its durable index, and that index and the configuration the
engine holds there are what the core is told it has applied. The log gives the hard state
and every entry after the start, and the replica applies the committed entries past the
engine's index again. Because applying is deterministic, replaying them rebuilds the same
rows.

The engine can open past the commit the log kept. A member learns of some commits only
after persisting their entries (focal-raft's light ready), and applies them without writing
the commit to the log, while the engine makes applied rows durable on its own schedule. The
core refuses a member that has applied past what it knows committed. Every entry the engine
applied was committed, and the log holds it, so on open the replica writes the engine's
index to the log as its commit, as it finishes an install the log never recorded. The
simulation found this once members could be lost for good. A leader lost right after a commit
left a follower that never heard the commit again, and that follower compacted, crashed and
reopened with its engine ahead of its log.

A member's log may reopen with an uncertainty mark: its last frame, acknowledged, no longer
reads, and recovery restored the term and vote it held and cut its entries (raft-log.md §6).
The member may then lack entries it acknowledged, and a vote judged on its shorter log could
elect a leader without one it helped commit. So until its log holds them again, or an entry
of a later term, it judges a request for its vote against the last entry it acknowledged, as
it would have with the entries: a candidate behind that entry gets no answer, one that holds
it is judged by the core as usual. It cannot lead without the entries, so it does not
campaign: an explicit campaign is refused with `Uncertain`, and while its election timer
runs, which keeps it from holding a lease on a leader that is gone, the requests of its own
campaigns are not sent. An order to take over leadership is dropped too. The mark ends by
Raft's own properties: a log that reaches the mark's index again holds the leader's entries up
to it, and an entry of a later term from a leader proves every committed entry the mark
covers is in the log, since terms never fall along a log (Ongaro and Ousterhout, ATC 2014,
§5.3–§5.4). A new leader's first entry is of its own term, so a member rejoins as soon as a
leader reaches it. The simulation found that withholding every vote, or stopping the timer
instead, left a group with one member lost and one uncertain unable to elect anyone.

## 5. Testing

Replicas are tested in deterministic simulation (`crates/range/tests/sim.rs`). Three
nodes, or five on even seeds, each with a simulated device for its log and a model engine,
run over a simulated network that delays, drops, reorders, duplicates and partitions
messages. Nodes crash, losing whatever their log and engine had not made durable, and
restart; a group of five has two down at once for about a quarter of its steps. Half the
members' `Ready`s are taken with `begin` and finished at a later step (§3), during faults and
after them, so each member's next tick and the messages delivered to it come while its
`Ready` flushes and are held: about 220 messages and 170 ticks a run. Devices fail writes and flushes,
which fences a node's log and takes the node down until it restarts from what the device
kept. Once or twice a run a member is lost for good, its device and engine with it, and a
member under a new identity replaces it as §6 describes; a second loss waits for the first
replacement to finish. Members' clocks step forward and back by up to two seconds, and a leader stamps the entries
it proposes with its own time. Replicas compact at random, so lagging
members, new ones among them, are caught up by snapshot. Three gateways, each with its own session, put
and get two keys at once. Puts go through the log, retried through leader changes with the
session's serials. Gets are confirmed by ReadIndex and answered from the leader's rows once
they reach the confirmed index. Every run is its seed (06 §A5). After every run, these
must hold:

- every index was applied with the same answers on every member that applied it;
- once faults stop, every operation completes;
- every answered put exists exactly once on every member, however often it was retried;
- every member holds the same rows;
- each key's history, as the gateways saw it in the simulation's time, is linearizable by
  Horn and Kroening's WGL search (06 §A6.8);
- every member's configuration names the live members as its voters, and nothing else, and
  every run finished at least one replacement.

A soak of 20,000 seeds passes, with thousands of fenced logs among them, and with 39,948
members lost for good and replaced: two in every run but 52. Failed flushes exposed an
install the log never recorded, which restart now finishes. Losses exposed the two further
faults §4 and §6 describe: an engine durable past its log's commit, and a replacement ended
before every voter knew its configuration. With messages and ticks held while a `Ready`
flushes (§3), a soak of 20,000 seeds passes holding 4,376,190 messages and 3,331,225 ticks,
and refusing 4 messages past the bound. With the hold reverted, 2,000 seeds stay linearizable
and live, since only half the `Ready`s are begun and a refused message is retried: the
harm needs the steady overlap `a_leader_whose_answers_come_while_it_flushes_still_commits`
drives, and the simulation's count of held messages is what catches the revert. The simulation catches two broken variants on purpose,
within the first seeds: gets served from any member's rows without ReadIndex fail
linearizability, and a replica that applies a repeated command again stores a put twice.

A run is exactly its seed. Each member has one update out at a time and the simulation waits
for it before the step ends, so every frame holds what the seed put in it, and each frame is
confirmed before its update is answered (raft-log.md §6), so no write the device sees hangs
on the writer's timing; the logs run under `Waits::Never`, since no submitter returns within
a wait. A test runs two seeds twice each and compares every count and every operation the
gateways saw, with its steps; over 300 seeds two runs gave the same counts. Before, the
writer's clock decided whether an idle confirmation was written before a crash, which moved
the simulated device's seeded faults, and `begin` looked at an update just submitted, whose
flush the writer might have finished: counts differed between runs of a seed by under 1%.

## 6. Replacing a member

A member whose device fails has lost its persistent state, and a member that has "cannot
safely rejoin the cluster with its prior identity"; a member under a new identity replaces it
by membership changes (06 §A1.2, Diss §3.8). `membership::Replacement` reads the configuration
the leader has applied and names the change to propose next, so a change a leader drops, or
loses with its leadership, is proposed again, and one already applied never is:

1. The new member joins as a learner. It receives entries and votes on nothing, so adding it
   changes no quorum, whatever state it is in (06 §A1.7; Diss §4.2.1).
2. Once the leader records that the learner has confirmed holding every entry it knows
   committed (`Replica::caught_up`), one entry makes it a voter and removes the failed member,
   as a joint change the group leaves by itself. While joint, commitment and elections need
   majorities of both configurations (Diss §4.3), so the swap {A,B,C}→{A,B,D} never passes
   through a configuration a lost member could split. It is the change 06 §C recommends for
   replica swaps, and the one CockroachDB uses (06 §A4). The 2015 single-server bug does not
   reach it: learners count toward no quorum, the swap is joint, and focal-raft proposes no
   change until a new leader has applied its whole log, which first commits an entry of its
   own term (06 §A1.7).
3. The replacement ends once every voter of the final configuration has said it committed the
   entry that made it (`Replica::configuration_known`, from the commit index each follower
   reports). A member applies a configuration when it applies its entry, and until then still
   counts the members the change removed. The simulation showed the cost of ending sooner. A
   leader applied the change that left the joint configuration and was lost before the others
   learned it had committed. The two survivors still applied the joint configuration, whose
   old half had lost two of its three members, so they could elect no one. Once every voter
   knows, losing any one member leaves voters that elect under the final configuration.

The snapshot a replica prepares for lagging members names the configuration it was prepared
at, and a member refuses a snapshot that does not name it. A member added after the leader
last compacted could therefore never be caught up by that snapshot. When an applied change
names a member the prepared snapshot does not, the replica prepares the snapshot again at the
change. Membership changes are rare, so this costs one image each.

## 7. Open

- A range's `max_entry_bytes`, and its log's frame, must hold the largest entry its gateways
  send, `mantle_meta::wire::largest_entry_bytes`: a CompleteMultipartUpload of 10,000 parts at
  S3's longest bucket, key and headers, 561,804 bytes; one of 10,000 parts with ordinary names
  is 540,167 (audit §16.5). The gateway refuses an upload ID the Name range never made and
  part numbers outside S3's before proposing, so no request makes a larger one. The node that
  sets a range's settings checks them against it.

- More than one ready of a range in flight. The log answers an update only once a later
  record confirms its flush (raft-log.md §6), so a range that waits on each ready before
  taking the next waits two flushes an update, and closed-loop appends run at half the rate
  they did (measurements/2026-09-29-log-confirmation.md). With the next ready's frame
  behind the last, its record would confirm the last at no flush of its own. focal-raft's
  `Node::ready` refuses a ready while one is out, so this needs the core to hand out readies
  ahead of their persistence, as raft-rs's asynchronous ready does. Advancing a ready at its
  flush and holding back only its acknowledgements is not safe: a leader would count toward
  commitment a frame recovery may still take for a torn tail. The core also takes no
  message or tick while a ready is out, so the replica holds them until it is done (§3);
  with readies in flight ahead of their persistence the core would step them directly, as
  etcd's does, and the hold would go.
- The bound on messages held while a ready flushes, a window of appends for each other
  member, assumes a flush is shorter than a heartbeat interval, so that a member's
  heartbeats, votes and answers beside its appends are few. The node's measured flush times
  against its tick should confirm it; until then a flush that outlasts it refuses messages
  past the bound, which Raft's retries cover, and the simulation counts those refusals.
- Proposals, reads and campaigns are refused with `Stalled` while a ready is out, and their
  callers retry. A node whose ranges are always flushing under load needs them held as
  messages are, or a scheduler that gives each range a moment between readies (§3).
- The window of entries kept for lagging followers before one is sent a snapshot, and
  moving snapshots out of band with the production engine.
- Read leases, if a deployment states its clock-drift bound (06 §A1.5); reads are confirmed
  by ReadIndex rounds (§3) until then.
- The session lifetime and bounds, sessions a range holds and the answers and bytes of
  answers one keeps, from how long gateways go between commands to a range and how many
  they keep in flight.
