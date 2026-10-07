# Range replicas: how a range runs its log

Status: design, 2026-09-28; command checksums and digests 2026-09-30. Sources:
docs/research/06 (consensus, "06 §x"), 07 (focal's consensus stack), 31 (integrity);
docs/design/metadata.md, whose ranges this runs, and raft-log.md, the log it writes to.

A replica is one member of one range's Raft group on one node: hyper-raft's durable shell and
its core (hyper-raft `docs/durable.md`, `docs/raft.md`), its group of the device's log, and the
range's engine with the state machine of its layer: Bucket, Name, File or Block (metadata.md §1).
It turns what the core asks for into log writes, engine batches and messages, in the order Raft's
safety needs (§3).

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
  A range also holds at most a configured number of sessions. Past it, a registration takes
  the place of the least recently used session only once that session's lifetime has passed,
  and is otherwise refused `SessionsFull`, with the time that lifetime ends: a session within
  its lifetime may have commands in flight whose outcome its gateway learns only through it,
  and expiring it to make room took a live session from each gateway in turn (audit S17;
  `sessions_expire_by_entry_time_and_the_least_recent_makes_room`). A command whose session is unknown or expired is
  refused, never run in a new session, since running it could apply it a second time.

Sessions live in the engine beside the range's rows, so they change in the entry's batch
and survive a restart with them. An entry reads each session it names once, at its first
command, checks and changes it in memory command by command, and writes it once at the end;
a registration, which may take the place of a session whose lifetime has passed, found by
the order of last use,
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

**A command carries its own checksum** (research/31 §5.4, B12; D16). A command is encoded in the
gateway's memory and then replicated, so a flip before its encoding goes to every replica alike,
and the log's frame CRC, computed later, protects it. Each command therefore carries a CRC-32C
computed where it is built, at the gateway, over its encoding, and every replica checks it at
apply. A mismatch fences the command, not the replica: the command is answered as corrupt, writes
nothing on any member, since every member finds the same mismatch in the same entry, and its
gateway sends it again from the request it still holds; the failure is attributed to the
gateway's node and core (node.md §5.6). The engine's per-key-value protection guards the rows
after apply (engine.md §3).

**Members compare digests** (research/31 §5.8, D21). Replicas apply the same log
deterministically, so a digest of a range's rows at an applied index is the same on every member
whose state is right. Members compute it at an index the leader names, paced as the scrubber is,
and compare; a member whose digest differs while its log agrees holds wrong state, the case of
protocol-aware recovery where the log is right and the state is not, and it is rebuilt from its
peers as §6 rebuilds a damaged member. Meta found 40% of its RocksDB corruptions already copied to
other replicas, and named comparing replicas cheaply as the open problem (research/31 §5.8).

## 3. The durable shell

The replica is hyper-raft's durable shell (`hyper_durable::Replica`, hyper-raft
`docs/durable.md`) over the member's group of the device's log and the range's state machine
(`RangeMachine`, §2). The shell keeps the order Raft's safety needs, stated as invariants over
what a restart would read (durable.md §3): a message that carries a term or a vote leaves only
once they are durable, a follower's acknowledgement of an entry only once its log holds it, a
leader counts itself toward a commit only once its own write is durable, a change of
configuration applies only once the member's durable commit covers it, and the log's start never
passes what the engine holds durably. Replacing mantle's own shell, it kept every rule of that
shell that was right and dropped the ones its core no longer needs (durable.md §10). It replaced
it because, measured on mantle's workload against mantle's shell at `1c179e8`, it is at least as
fast and allocates no more (durable.md §13): a group of three or five on real files commits at a
median 39–44% lower, 30–43% fewer allocations an entry, the same reallocations, and one member
evenly ([measurements](../measurements/2026-10-03-range-on-the-shell.md)). The measurement found
a follower's queue of messages grown again whenever its writes held more queues than the one
spare the core kept; the core now keeps one for each write that may be out (hyper-raft core, "A
spare queue for each ready in flight").

**A drive.** The owner calls `drive` when it stepped something into the replica, when a tick came
due, or when the log's answer woke it through the waker it passed (durable.md §2.2). A drive takes
the log's answers in the order the writes were submitted, and for each durable write releases the
messages that waited for it and tells the core; applies what the core gives to apply and the
commit fence allows, one page at most (`max_committed_size_per_ready`, or one larger entry), the
rest waiting for the next drive (`Driven::more`); and takes at most one `Ready`, giving out at once
what a leader may send before its own write, attaching everything else to the write, and
submitting the write with the waker. A write submitted in a drive is never looked at in it, so
what a drive gives out never hangs on how soon a flush happened to end. One write of a `Ready` is
one update of the log: the snapshot's point, the entries, the fast track's proposals, then the
hard state, cut into parts that each fit a frame (`GroupLog::parts`) with the entries first and
the hard state last, so a crash between parts leaves only entries no one acknowledged; the write
is durable once every part is.

**Readies ahead of their persistence.** The core gives the next `Ready` before the last is
durable (hyper-raft core step R-4), and the shell takes them to the log's depth, one write in each
of the log's three pipeline frames (`GroupLog::depth`, raft-log.md §3). Meanwhile the core takes
messages, proposals, reads and ticks: nothing is held and nothing refused while a write is out.
mantle's shell took one `Ready` at a time, held the messages and ticks that came while it flushed
within a window of appends a member and one election timeout, and refused proposals, reads and
campaigns with `Stalled`; under steady load a write waited for two flushes, its frame's and the
record that confirms it (measurements/2026-09-29-log-confirmation.md), where the next write's
record now confirms the last. A member whose writes stay out through many election timeouts, its
device holding them while its owner ticks it, campaigns as its clock says, and each campaign
supersedes the vote requests of the one before that have not left (hyper-raft core, "A campaign
supersedes the requests still waiting"): once the device goes on, it sends at most one campaign's
requests for each write it had out and its last campaign's. mantle's shell bounded the same burst
by holding at most one election timeout's ticks, after a follower whose device stalled for 100
timeouts replayed them as 116 requests at once; on the shell before the core's rule, a hundred of
the longest timeouts with three writes out sent 234, two for each of 117 campaigns
(`a_member_whose_writes_stay_out_through_many_timeouts_sends_one_campaign_a_write`).

**A write the log cannot make durable.** A write refused for room another write frees (the
group's retained bound, the log's segments, its groups or its queue) waits, whole, with every
write after it, which the log refuses too (`LogError::Behind`), so no later write is durable
before it (durable.md §2.4). The replica then takes no part in the group: it refuses messages,
proposals, reads and campaigns with `Stalled`, as the network may drop them, and is not ticked,
the ticks it missed never replayed. The refused writes are made again, as one write of everything
the core holds not yet durable, once room may have been freed: once its own compaction is durable
(`compact`), or on its owner's word (`resume`) after another group of the log compacted or left. A
snapshot's report that comes meanwhile is kept, the latest for each member, since replication to
that member pauses until its snapshot's fate is known. Anything else the log answers a write with,
a failed write or flush above all, fences the replica: a failed flush leaves each sector it
covered durable or not while reads return the new bytes (Rebello et al., ATC 2020; research/03
§6), so nothing the process holds says what the device kept. Every write out is dropped with what
waited for it, and every call answers `Fenced` until the node reopens the log, which recovers what
is durable (raft-log.md §6), and opens the member afresh from it; the group goes on without it
meanwhile. The core and the engine run inside an unwind boundary, and an unwind fences the
replica the same way. `a_failed_flush_fences_the_member_and_the_group_goes_on_without_it`,
`a_failed_write_fences_the_member_and_the_group_goes_on_without_it`,
`a_write_refused_for_room_waits_until_the_group_compacts`,
`a_write_refused_for_a_full_log_waits_until_another_group_compacts` and
`a_write_refused_for_want_of_a_group_waits_until_one_leaves` (`crates/range/tests/group.rs`).

**A change of configuration applies only on a commit the member's durable state holds.** A
member's commit is volatile (Diss §3.8). The core counts elections and commitment by the newest
configuration its log states, committed or not, as the thesis does (Diss §4.1; hyper-raft
`docs/raft.md` §3.4, which left etcd's rule of the configuration applied after hyper-check's
random walk elected two leaders of one term under it); the owner is told a configuration as it
applies the change. A change applied on a commit its durable state does not hold reverts in a
crash, while what others did on the member's word stands: under etcd's rule, which the core kept
until hyper-raft `83f193a`, the directed runs of §5 showed a founder of two that removed its only
peer and lost power while the commit's write flushed reopening counting the stopped peer, never
to elect again. The shell keeps the durable commit
`C_d`, the greater of the commit its durable writes stated and the engine's durable index, and
applies a change only once `C_d` covers it, the entries after it waiting behind it with the
core's apply paused (core step R-6, an apply pause like etcd's `applyingEntsPaused`); the next
write states the commit, or a write of the hard state alone when none is due, one flush a change
(durable.md §4.1). Every write states the commit the core holds when it is laid out, and a sole
voter states its last entry, which its own write commits, so a sole voter's commit no longer goes
unwritten. The core states in its answers to appends and heartbeats only the commit it knows
durable, told every commit the shell makes durable outside a `Ready`'s hard state
(`RawNode::commit_durable`, R-6), so a leader counts a follower's commit toward
`configuration_known` (§6) only where the follower's restart keeps it. Mantle's layers have no
entry a member acts on at its next start (`RangeMachine::acts_at_start`), the fence's other case,
so changes of configuration are all it holds, and the shell writes no commit for a quiet group
(`QUIET`, docs/design/constants.md): a restart learns the rest from its group and its log.

**Reads.** A leader confirms reads a round at a time, the core's rounds (hyper-raft
`ReadRounds::Shared`, focal's F43): one round carries every read asked since the round before, and
a read asked after a round was sent is asked for by the next, never confirmed by one that left
before it, whose index may predate a write committed since (06 §A1.5;
`a_read_asked_after_a_round_left_waits_for_the_next`). A round that was lost is asked again by the
leader's heartbeats. The shell gives a read back once a quorum confirmed it and the replica has
applied through its index, so rows answer it linearizably. A leader that steps down drops the
reads it held, as a lost message is dropped, and the read's caller asks again of whoever leads
(`a_read_no_quorum_confirms_is_never_served_and_is_asked_again`); the reads waiting are bounded by
the core's `Limits::pending_reads`, past which a read is refused. Leases are not used. mantle's
shell kept one round out at a time and gave a lost round's reads back after an election timeout;
reads then waited up to a round trip more each (research/07 §10).

The transport tells the sending replica whether each snapshot arrived. A leader stops
replicating to a member while that member's snapshot is out, so a lost snapshot left
unreported would stall the member for good; the simulation found this. A snapshot's own
stream always learns its fate.

**Elections on ticks.** The range elects on its owner's ticks (durable.md §8) until the node
carries the node-pair liveness stream and its ranges elect by suspicion (hyper-raft timing step
L-2, §7). The settings are the ones mantle's shell ran with, so the switch changed the shell and
not how a range elects: an election after `election_tick` ticks without a leader, the timeout
drawn anew at every arming between `election_tick` and twice it, a heartbeat every
`heartbeat_tick` ticks, with pre-vote and check-quorum (06 §A1); the counts are focal's, ten and
two (research/07 §2.2), and the tick period is derived from the slowest voter path's durable
acknowledgement (node.md §2.4). `Range::settings` carries them; the node that sets every range's
settings is not built (node.md §10), and the tests state them. The shell's other controls on
ticks, a heartbeat on the owner's word (`beat`), a timeout set from outside
(`set_randomized_election_timeout`) and patience past the timeout (`set_patience`), are what
focal's shell does on ticks; mantle's shell had none of them, and a range uses none.

**Bounds a member checks at open.** Every member of a range runs the same settings, so an
entry the leader takes must fit one frame of every member's log: the range bounds an entry
(`max_entry_bytes`), the leader refuses a larger proposal, and a member whose log's frame
cannot hold one refuses to open (`claim`). A group compacted to its applied state retains at
most one write's entries, a leader's uncommitted proposals or the appends in flight to a
follower, and one entry past either bound, which the core admits alone; a member whose log
bounds a group below that, in bytes or in entries of the fewest bytes, refuses to open, since a
write refused for room there could wait for good.

**The core's bounds.** The core bounds every queue it keeps by what its owner states
(`hyper_raft::Limits::derive`), and a range states each from its own settings, so no bound is
chosen apart from them:
- **Largest message.** It is an append. The core takes entries while their encoding fits
  `max_size_per_msg` and always at least one, so an append carries that or one entry of
  `max_entry_bytes` with its fixed bytes. A range's entries carry no context.
- **Members.** A configuration names at most the boot configuration's members and the one learner
  a replacement adds, the only change a range's membership makes (§6).
- **Queue memory.** Each queue holds one write's worth: the bytes above, counted as the most
  entries they carry at an entry's bytes in memory each. So a member always holds a leader's
  message whole.
- **Writes out.** One write is out at a time, which the shell raises to the store's depth.

**What the core reads of the log.** The core reads the group's durable state through the
shell's store, hyper-log's group handle (`GroupStore` over `GroupLog`, claimed when the member
opens), through which the member makes every write of its group and which the log refuses to
anyone else. Only the group's own writes move what it reads, so the handle keeps, on the
replica's thread, what the answered writes left: the start, the last entry, every retained
entry's term as runs of one term, the hard state and marks, and the bytes of the recent entries
within the log's `group_cache`. It answers the core from them with no message to the log's owner
thread, and asks the owner only for entries older than its cache, a view while proposals are
out, and every read after a failure it cannot account for. The state machine applies entries
where the log holds them, borrowed, with no copy (`EntryRef`), and the handle cuts each write
into parts on the replica's thread, so the member holds no handle on the log itself: it borrows
the log only to claim its group (measurements/2026-10-01-group-log.md).

## 4. Compaction, snapshots and restart

The engine makes applied state durable on its own schedule. A compaction makes it durable
(`compact`) and writes its durable index to the log as the group's new start, keeping a window
of entries behind it for followers that lag, never past what the log holds durably nor a start
still out; the log then frees the entries before it (raft-log.md §5; 06 §C.b.2). A member that
needs entries from before the start is sent a snapshot instead: every row of the range as of
the last entry the leader applied, made on demand, with the configuration the group held there
(hyper-raft `docs/durable.md` §2.3). A snapshot's index is at or past the log's start, so the
member continues from the entries after it. A member installs one before its log records the
new start, its engine's rows durable first, so the log never starts past what the engine holds.
For now the rows travel inside the snapshot message. The production engine will move its files
out of band instead (12 §6.3).

The engine keeps, beside its rows, the configuration as of the last entry it applied and the
term of that entry (`marker::TERM`): every member applying the same entries writes both alike.
The term changes only at a new leader's first entry, its empty one (thesis §3.6.2's no-op; the
core appends it as it takes the lead), whose batch carries it, so an ordinary entry's batch
carries nothing more; an install writes the snapshot's term with its rows. So a restart knows the
point its engine stands at, index and term, without reading its log.

On restart, the engine opens at its durable point, and the configuration it holds there is the
core's. The shell first makes the log agree with the engine (durable.md §4.3): a log that starts
past the engine does not open (entries the engine needs are gone); an engine at a point the log
does not hold, a snapshot installed whose start the log never recorded or entries the engine
applied that the log lost, moves the log's start to that point with nothing past it and its
commit to it; and an engine past the log's commit within its entries raises the commit to it.
The core then opens at the engine's index, and the committed entries past it are applied again;
applying is deterministic, so replaying them rebuilds the same rows. The simulation found the
cases this covers once members could be lost for good: a leader lost right after a commit left a
follower that never heard the commit again, and that follower compacted, crashed and reopened
with its engine ahead of its log's commit; failed flushes left an install the log never recorded.

A member's log may reopen with an uncertainty mark: its last frame, acknowledged, no longer
reads, and recovery restored the term and vote it held and cut its entries (raft-log.md §6).
The member may then lack entries it acknowledged, and a vote judged on its shorter log could
elect a leader without one it helped commit. The core keeps the mark (hyper-raft core step R-5,
`Config::lost`): until its log holds the entries again, or an entry of a later term, it judges a
request for its vote against the last entry it acknowledged, its claim, as it would have with
the entries, and refuses a candidate behind it. It campaigns on its log, its own vote not
counted, where the others are a quorum of each half of its configuration without it, and is
elected exactly when such a quorum answers for no more than its log holds (core step R-7,
durable.md §5.2); a member of one or two voters, which the others can never be a quorum without,
waits, its campaign refused with `Uncertain`. The mark ends by Raft's own properties: a log that
reaches the mark's index again holds the leader's entries up to it, and an entry of a later term
from a leader proves every committed entry the mark covers is in the log, since terms never fall
along a log (Ongaro and Ousterhout, ATC 2014, §5.3–§5.4); elected, its mark ends.

**Damage and its repair.** Alagappan et al. (FAST 2018, "AGL+18"; research/03 §AGL+18)
compare what a replicated log can do with a member whose storage is damaged. Crashing the
member is safe and leaves it down for good; truncating its log, or wiping it and restarting
it as a voter, is unsafe, since a member that forgets an entry it acknowledged can form a
majority with members that never had it; marking it non-voting loses its promises; and
reconfiguring it out needs a majority to commit the change (AGL+18-F1). Their protocol,
CTRL, repairs the member in place instead. It tells a crash from damage by a persist record
per entry, keeps its node-specific state, term and vote, in two copies because no peer holds
it, and has a damaged follower name its faulty entries to the leader, which sends them
again, while a leader with faulty entries learns from its followers whether each was
committed before it serves (AGL+18-F3 to F6). Where both copies of the node-specific state
are lost it crashes the node. Its guarantee: a committed entry with a correct copy anywhere
is recovered, or the group waits for it (AGL+18 §3.2).

A member here is repaired in place wherever what it lost is replicated state, and replaced
only where it lost its own promises, which no peer holds:

- *Entries lost, hard state whole.* The log restores the term and vote of a lost last frame
  from its persist record and marks the entries (raft-log.md §6). The member keeps its
  identity, and the core judges votes by its mark and elects it only on its log (above). The
  vote is CTRL's question for the whole lost range at once: a grant is `dontHave`, a whole
  voter's refusal `have`, a marked voter's `haveFaulty`; a group waits where CTRL's does, where
  no correct copy of an entry answers (durable.md §5.2).
- *The leader counts what the member lost.* A leader's record of what a member holds only
  rises, and a refusal at or below it reads as a stale one, so a member that acknowledged an
  entry and lost it would never be sent it again while that leader led; seed 64 of the
  simulation, once damage at rest was in its faults, found a member one entry behind for good.
  As in CTRL, the member names what it lacks (core step R-5): marked, it answers a heartbeat or
  an append that counts entries past its log with a refusal flagged lost, naming the last entry
  it holds, and the leader takes its record of the member back there and sends the lost entries,
  a snapshot only where it compacted them (durable.md §5.1). CTRL measured the lost entries
  against a snapshot's image as kilobytes against megabytes (AGL+18-F9); hyper-raft measured one
  lost entry of 30,000 repaired in 1.8 KB and 32 µs against a snapshot's 30.7 MB and 8.9 ms
  (hyper-raft `docs/benchmarks.md`, "Repair by entries (R-5)"). mantle's shell asked for a
  snapshot reaching its mark until the core could take the refusal.
  `a_marked_member_is_repaired_by_its_lost_entries_and_elected_only_on_its_log`.
- *The engine applied entries the log lost.* A member that made its rows durable past what
  the damaged log kept holds committed state, and its hard state is whole. It keeps its
  identity: its log starts at the engine's point, as after an install, and the mark stays while
  the log lacks an entry it covers. The start needs that entry's term, which the core checks a
  leader's appends against, and the log no longer holds it; the engine keeps it (above).
  mantle's shell inferred it from the terms along the log, knew it only where the engine's
  index was the mark's or the last entry left was of the mark's term, and rebuilt the member
  elsewhere. `a_member_whose_engine_applied_what_its_damaged_log_lost_keeps_its_identity`;
  `a_member_whose_engine_applied_what_its_damaged_log_lost_is_repaired_in_place` repairs such a
  member in a group of three, every acknowledgement it sends naming only entries its log
  holds.
- *Promises lost.* A group whose lost frame held fast-track proposals, which its persist
  record does not carry, or whose records do not run unbroken, is damaged: the log serves it
  to no one (raft-log.md §6). What the member promised, a vote or an approval at an index, is
  its own; forgotten, it could promise the opposite in the same term, and no peer can say what
  it promised. CTRL crashes such a node. The member here does not open under its identity:
  claiming the group answers `ReplicaError::Damaged`, and the node removes the group's records
  (`remove`) and rebuilds the member under a new identity on the same device
  (`Replica::rebuild`, §6), which is safe where wiping it as a voter is not (AGL+18-F1;
  research/03 R7.4) and brings the group back to full strength where a crash leaves it short.
- *The whole log damaged.* A frame that no longer reads with a later frame after it fails the
  log's open (raft-log.md §6), for every group on the device; the device is replaced, and each
  member on it with it, as a member lost for good (§6). CTRL would repair such a frame's entries
  from peers in place (§7).

## 5. Testing

Replicas are tested in deterministic simulation (`crates/range/tests/sim.rs`). Three
nodes, or five on even seeds, each with a simulated device for its log and a model engine,
run over a simulated network that delays, drops, reorders, duplicates and partitions
messages. Nodes crash, losing whatever their log and engine had not made durable, and
restart; a group of five has two down at once for about a quarter of its steps. Each member's
log answers its writes as they are submitted (`tests/support/store.rs`), and the shell takes
each answer at the member's next drive: half the time a member drives once a step and leaves its
writes out past the step, its next readies taken ahead of their answers and the messages and
ticks delivered to it taken by its core while they are out, during faults and after them; half
the time it drives until none is out. Devices fail writes and flushes,
which fences a node's log and takes the node down until it restarts from what the device
kept. Once or twice a run a member is lost for good, its device and engine with it, and a
member under a new identity replaces it as §6 describes; a second loss waits for the first
replacement to finish. A down member's device is damaged at rest now and then, one bit of
one of its log's frames flipped on the medium, as §4 describes: its last frame, which its
restart restores and marks, and the member is repaired in place; its last frame after it takes
a fast-track proposal of its own, which leaves its group damaged, and the member is rebuilt
under a new identity on its device; or an earlier frame, which leaves its log damaged, and the
device is replaced. One member's data is short or being rebuilt at a time, the most a group
of three survives, and no member is lost for good meanwhile. Members' clocks step forward and back by up to two seconds, and a leader stamps the entries
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

A run settles only once every member marked by damage has been repaired, its log holding
again what it acknowledged, and each run checks that every mark it made ended. With damage
at rest in the faults, 6,000 seeds are linearizable and live on the shell: 4,974 frames
damaged, 208 members marked and all 208 repaired by their lost entries, 131 of them with an
engine past their log kept under their identity, 1,655 members rebuilt on their device and
1,235 devices replaced; 14,821 members lost for good and replaced; members' writes left out
past 926,870 drives, 1,310,802 messages and 1,024,743 ticks taken while writes were out, and
14,520 drives that found a write waiting for room (`MANTLE_SIM_SEEDS=6000`, debug build, the
hyper-raft crates of the snapshot vendor/UPSTREAM.md names, 2026-10-06, this machine, 340 s at
a one-minute load average of 6.7 at the start and 9.2 at the end). The recorded seeds a default
run takes, 1 to 58, are the fewest from 1 in which a restart finds each kind of damage: seed 58
is the first whose member opens marked on these crates (1 to 57 mark none).

Soaks of mantle's own shell before the durable one found three faults whose rules the shell
keeps (20,000 seeds, 2026-09-30): an install the log never recorded, which a restart now
finishes; an engine durable past its log's commit (§4); and a replacement ended before every
voter knew its configuration (§6). The simulation catches a broken variant on purpose: gets
served from a member's rows without ReadIndex are not linearizable, at seed 1. A command a
member applies again is answered from its session, and a put delivered again is recognised by
its file besides, so taking the session's check out alone stores no put twice in 400 seeds;
the exactly-once check rests on the two together.

Directed runs in the same file put the power cut where the commit fence (§3) is needed. A
group founded by member 1 makes one change of configuration: a founder of two removes its only
peer, which the operator stops once the founder says every voter knows; a leader of three, and
then a follower, each lose power while the leader removes the third; a sole voter adds a
learner. The member's device cuts the power at each of its writes and flushes after the change
is proposed in turn, `Fault::PowerCut`, and once after the change is made everywhere, the
target driving until its writes are answered and driving once a round with its writes left out.
The member restarts from what its device kept and,
driven alone, must reach any configuration it had applied, and the group must elect a leader
and commit an entry on every member. Before the fence, every case failed: the founder, its
`Ready` begun, applied the removal while the commit's write flushed, its power cut after ten writes and
flushes, and never elected again, counting its stopped peer; the leader and the follower of three
reopened counting the member they had removed; the sole voter, killed after the change was
made, reopened without the learner it had added, its commit never logged.

Directed replacements put the power cut inside the window between a replacement's change
committing and the commit a member's log states (hyper-raft `docs/durable.md` §11, D-1's first
test). Member 3 of a group of three is lost for good, member 4 joins, and the leader runs
`membership::Replacement` until it says every voter knows the final configuration. The leader,
a follower that stays a voter, and the joining member in turn lose power at each of their
device's writes and flushes from member 4's joining, 30 to 40 operations each and every window
among them, the target driving until its writes are answered and driving once a round with its
writes left out. Then every member loses power at the moment the leader
says the replacement is done, keeping only what its device made durable, and each, driven
alone, must reopen in the final configuration; one member of it is lost for good, each of the
three in turn, and the other two must elect a leader and commit an entry. The runs passed on
mantle's own shell before R-6 (origin/dev `1c179e8`) and after it, where a follower's answer left
after the write that stated its commit, so no word the leader counted was volatile, and §6's one
exception answers next in a later term; and they pass on the durable shell, whose members take
readies ahead of their writes and whose answers state their durable commit (R-6). Counting a
voter that holds the
change's entry rather than one whose durable commit covers it (`matched` for `committed_index`
in the shell's `configuration_known`) fails all three at once, on mantle's shell and on the
durable one: members reopen in the joint configuration, whose old half has lost two of its
three, and the two left elect no one.

`tests/processes.rs` runs the same replacement as real processes: each member a process of its
own on a real file through the direct-I/O file layer and the platform's full flush, the
supervisor the network over TCP on the loopback interface, running rounds as the directed runs
do. The member to be killed names the window as it reaches it, a follower with its write
stating the commit out, the leader with the change held behind its fence, and waits there for
`SIGKILL`; it is started again from its file, and the supervisor gives it the replacement again,
since the order lived in the process: the leader started again holds the change in its log,
counts by it, and may be elected again, and then it is the one that must finish the
replacement. Each is killed at each of its three windows in
turn: the learner's addition, the swap, and the end of the joint configuration. Then every
member is killed the moment the leader says the replacement is done, each must reopen from its
file in the final configuration, and with one lost for good the other two must elect and commit.
The engine keeps nothing across a process's death, so a restarted member applies again what its
log states committed. The mutation above fails all three the same way, on both shells.

A run is exactly its seed. Each member's log answers its writes as they are submitted, the
store waiting for the log's answer before `submit` returns, and the shell takes the answer at
the member's next drive, never in the drive that submitted it, so what a drive gives out hangs
on the simulation's calls alone, and every frame holds what the seed put in it; the logs run
under `Waits::Never`, since no submitter returns within a wait. A test runs two seeds twice
each and compares every count and every operation the gateways saw, with its steps. The real
asynchrony of the log's answers is the real-process test's (`tests/processes.rs`), where a
member's waker wakes it as its log answers.

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
   reach it: learners count toward no quorum, the swap is joint, and the core proposes no
   change until a new leader has applied its whole log, which first commits an entry of its
   own term (06 §A1.7).
3. The replacement ends once every voter of the final configuration has said its log states a
   commit through the entry that made it (`Replica::configuration_known`, from the commit each
   follower reports). A member counts by the newest configuration its log states (§3), so one
   whose log lacks the entry still counts the members the change removed, and a voter whose
   reported durable commit reaches the entry holds it. The leader counts itself without a report: it
   applies the change only once its own log states the commit (§3). A member's answers carry its
   durable commit, not its commit (hyper-raft core step R-6, `docs/durable.md` §4.4): the core
   holds an answer to the commit the write it waits for states, and the replica tells the core
   each commit it makes durable outside a `Ready`'s hard state (§3). Before R-6 a follower's
   report left after the write that stated it, since a follower's messages wait for its
   `Ready`'s write, and a member that committed alone as leader, in `advance_append`, and
   stepped down in the same term answered next in a later term, with that term's write. The
   simulation showed the cost of ending sooner, under etcd's rule of the configuration applied
   that the core kept until hyper-raft `83f193a`. A
   leader applied the change that left the joint configuration and was lost before the others
   learned it had committed. The two survivors still applied the joint configuration, whose
   old half had lost two of its three members, so they could elect no one. Once every voter
   knows, losing any one member leaves voters that elect under the final configuration.

A member whose group its log found damaged is rebuilt the same way on its own device
(`Replica::rebuild`): the group's records are removed, which is all the log takes of a
damaged group, a member under a new identity opens there from a new engine, and the leader
runs the replacement of the damaged identity by the new one. The damaged identity never opens
again, since it may have voted in terms its log no longer shows; the node records the new
identity as the group's member on the device before the removal, so a restart after it opens
the new one again. `a_member_whose_group_was_damaged_at_rest_is_rebuilt_from_its_peers`
damages a member's last frame holding a proposal: its open answers `Damaged`, the group goes
on, the rebuilt member catches up and replaces it, and every entry any member applied is
held, with its answers, on every member after.

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

- The depth: a group's writes are taken to the log's three pipeline frames
  (`GroupLog::depth`), and a group of frame-sized writes holds all three frames' bytes; whether
  the third earns its place for one group as it does for the log (hyper-raft `docs/durable.md`
  §14, item 1).
- The apply on the owner's thread: a drive applies one page at most, and whether apply time ever
  exceeds what a shard's quantum allows is measured once the node's shards run (durable.md §7,
  §14 item 4).
- A frame damaged with a later one after it fails the whole log's open, and every member on
  the device is replaced (§4). CTRL repairs such entries in place from peers, the persist
  record naming what the frame held (AGL+18-F5, F6); the log would report the groups the frame
  touched as marked through its entries rather than refusing to open, which is the log's
  (raft-log.md §6).
- A log that has refused `Full` does not recover room for a frame larger than about a quarter
  of its room, even once every other record in it is compacted dead: no frame is written
  after, so no sweep frees a segment (a probe of the log at 2026-09-30, reported to the log).
  `a_ready_refused_for_a_full_log_waits_until_another_group_compacts` uses entries of a
  quarter of a frame until it does.
- The window of entries kept for lagging followers before one is sent a snapshot, and
  moving snapshots out of band with the production engine.
- Read leases, if a deployment states its clock-drift bound (06 §A1.5); reads are confirmed
  by ReadIndex rounds (§3) until then.
- The session lifetime and bounds, sessions a range holds and the answers and bytes of
  answers one keeps, from how long gateways go between commands to a range and how many
  they keep in flight.
