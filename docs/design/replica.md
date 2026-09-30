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
[measurements](../measurements/2026-09-29-metadata-apply.md)). A session keeps its answers
within a count and a budget of bytes; past either, the oldest are forgotten, and a retry of
one is a repeat.

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
   timers: a member waiting for room takes no part in the group.
4. Sends the messages that must follow durability: a follower's acknowledgements and votes.
5. Applies the committed entries (§2).
6. Tells the core, which may hand over more committed entries and messages.

`drive` does these steps and waits at step 3 for the update to be durable. `begin` does not
wait: it gives out step 2's messages and the reads confirmed, submits the update, applies the
entries already committed, and returns with the `Ready` still out and `persisting` set, so the
node sends the messages while the log flushes, and a node driving many ranges submits every
range's update before the one flush that makes them all durable. A later `begin` or `drive`
finds the update durable and does steps 4 to 6. A range still has one `Ready` out at a time,
and refuses every other call with `Stalled` until it is done; a follower's acknowledgement
never leaves before its write is durable. Before, `drive` returned only after the flush, so
the messages a leader may send during it waited for it (audit §5.1). The simulation takes
half its members' `Ready`s this way under faults, leaving about two hundred a run flushing
across a step, and checks linearizability as before. What the node's scheduler owes the
ranges it drives, time for heartbeats, reads and applying beside the flushes it waits on, is
the node's, which is not built (STATUS).

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
nodes, each with a simulated device for its log and a model engine, run over a simulated
network that delays, drops, reorders and partitions messages. Nodes crash, losing whatever
their log and engine had not made durable, and restart. Devices fail writes and flushes,
which fences a node's log and takes the node down until it restarts from what the device
kept. Once or twice a run a member is lost for good, its device and engine with it, and a
member under a new identity replaces it as §6 describes; a second loss waits for the first
replacement to finish. Replicas compact at random, so lagging
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
before every voter knew its configuration. The simulation catches two broken variants on purpose,
within the first seeds: gets served from any member's rows without ReadIndex fail
linearizability, and a replica that applies a repeated command again stores a put twice.

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

- The window of entries kept for lagging followers before one is sent a snapshot, and
  moving snapshots out of band with the production engine.
- ReadIndex reads, and leases if a deployment states its clock-drift bound (06 §A1.5).
- The session lifetime and bounds, sessions a range holds and the answers and bytes of
  answers one keeps, from how long gateways go between commands to a range and how many
  they keep in flight.
