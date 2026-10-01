# The node: one process from a laptop to a fleet

Status: design, 2026-09-30. Sources: docs/research/25 (the node's runtime, cited as "25 §x"),
07 (focal's consensus stack), 08 (slates' runtime and transport), 09 (cells and S3's
internals), 11 (operating-parameter models), 06 (consensus), 12, 23 and 24 (the engine), 04
(placement and repair), 10 (device health); the audit of 2026-09-29 (cited as "audit §x"); and
the records this one joins: architecture.md, chunk-store.md, raft-log.md, replica.md,
metadata.md, gateway.md, s3-protocol.md, durability.md, encryption.md, measurement.md and
constants.md.

`mantle serve` is the process that runs mantle. The pieces it runs exist as libraries: chunk
volumes that group-commit and verify every read (`mantle-chunk`), a Raft log shared by every
range on a device (`mantle-log`), range replicas that run focal-raft's core over that log and an
engine (`mantle-range`), the Name, File, Block and Bucket layers with their sessions, sweeps,
reclaimer, collector and coordinator (`mantle-meta`), the gateway's PUT, GET and completion
drivers (`mantle-gateway`), the S3 protocol (`mantle-s3`), device identification and
calibration (`mantle-disk`), and erasure coding (`mantle-ec`). Each is either sans-I/O, naming
the requests it needs and taking their answers, or owns threads of its own behind a bounded
queue. None of them has a process around it yet. This record says what that process is: the
threads it runs and how they meet, what it measures and what it is told, how it drives
thousands of replicas over shared logs, how nodes talk to each other, how an S3 request becomes
the drivers' requests and how those are served, and the order it is built in.

A laptop is a region of one cell of one node (architecture §1). It runs everything below; the
network sockets, the router and the mover between cells have nothing to do. A node's peers
include itself, reached by the same routing, admission and fencing as any other peer, so the
path a laptop exercises is the fleet's.

## 1. The process

### 1.1 Roles

A node has up to three roles, each set by its configuration. As a **storage node** it owns
chunk volumes on its data devices (chunk-store.md). As a **range host** it holds range
replicas and their engines on its metadata devices, with one shared log per device
(raft-log.md §1, replica.md). As a **gateway** it listens for S3 requests and runs the drivers
that serve them (gateway.md). A laptop has all three on one device. A large cell separates
them where measurement says the resources contend (architecture §3), and the code is the same
either way.

### 1.2 Threads

| Owner | Threads | What runs there |
|---|---|---|
| Network runtime | a tokio multi-thread runtime, one worker per core the process is granted | QUIC connections and the datagram plane (§3), SWIM, HTTP connections (§4), the drivers of each S3 request, the routing layer (§5) |
| Metadata shards | one per granted core | range replicas: stepping, ticking, proposing, `begin`, applying, serving confirmed reads (§2) |
| Coding pool | one per granted core | erasure encoding and decoding of whole blocks, block checksums |
| Volumes | per volume a writer, a cleaner and a scrubber, which exist (chunk-store.md §4); per device, readers up to the measured read depth, which are new (§1.3) | the chunk store |
| Logs (exist) | one writer per metadata device (raft-log.md §3) | group commit of every range's updates |
| Engine background | flush and compaction pools shared by every engine instance on the node (24 §4.7, engine P14) | the engine's own background work |

The network runtime never blocks. tokio says why: "A blocking operation performed in a task
running on a thread that is also running other tasks would block the entire thread, preventing
other tasks from running", and CPU-bound work belongs in "a separate thread pool dedicated to
CPU bound tasks" (25 §1). Replicas therefore do not run as tokio tasks. Their work includes
engine reads that miss the cache and wait on the device, reads of log entries no longer in
memory, which are synchronous (audit §12.2), and applies that take milliseconds: completing an
upload of 10,000 parts applies in 6.3–6.7 ms (measurements/2026-09-29-metadata-apply.md). A
shard owns its replicas outright, so a `Replica` is touched by one thread and needs no lock.
Encoding a block under RS(9,6) needs a work space of 128 MiB (gateway.md §4) and is CPU-bound for
its duration, so it goes to the coding pool.

A granted core is what `std::thread::available_parallelism` reports: the operating system's
answer, affinity masks and quotas included where it reports them. Three pools of that size
oversubscribe the cores when all three are busy at once; the operating system shares time
among them. Each class's scheduling delay is measured (§8), and whether dividing the cores by
measured demand serves better than the operating system's sharing is open (§11).

### 1.3 Where the async side meets the blocking side

Nothing on a tokio worker waits on a device, and nothing on a device thread waits on the
network. Three mechanisms keep it so.

**Tickets for writes.** The log already answers a submission through a `Pending` that can be
polled or waited on (`mantle_log::Pending`), but its answer travels a `std::sync::mpsc` channel
that wakes nothing. The submission gains a `std::task::Waker`, which the writer thread wakes
when it sets the answer. A chunk volume gets the same: today `Volume::put` returns only once
the chunk is durable, holding its caller's thread for a flush; it gains a submission that
returns a ticket at once. A tokio task awaits a ticket with its own waker. A shard's waker
pushes the range onto the shard's ready queue and unparks the thread, so a completion wakes the
one range it belongs to and no loop scans every range (audit §11.3). `std::task::Waker` is in
the standard library, so neither crate takes a dependency on a runtime. A ticket carries the
generation of what submitted it, the range replica's incarnation or the request's, and an
answer that comes back after its owner was replaced is dropped (audit §11.3). Dropping a ticket
gives up the caller's interest only: a write the queue admitted is written, and its outcome is
what recovery finds; it is never reported as a failure that had no effect (audit §11.3).

**Threads for reads.** A volume's read runs on its caller's thread and holds the device at its
gate, at most the depth where calibration found throughput stops growing (chunk-store.md §7).
Each device gets that many reader threads, since a read past the depth only waits; an async
caller hands its read and its reservation to them and awaits a ticket. A device that
calibration has not measured reads one at a time (chunk-store.md §7), so it gets one reader.

**Bounded handoffs.** Work passes between the runtime, the shards and the coding pool through
queues whose room is a reservation from the admission authorities of §2.5. A shard never waits
to put work on a queue; a full queue is a typed refusal that the sender turns into its own
answer (a message dropped, a request refused with `503 SlowDown`). tokio's blocking pool is not
on any data path: it spawns up to 512 threads by default and then queues without bound, "since
the queue does not apply any backpressure" (25 §1). Startup and shutdown use it for file
operations, each under a permit.

### 1.4 What is stated, what is measured, what is derived

| Stated by the operator | Measured by the node | Derived by the node |
|---|---|---|
| the node's roles; which devices hold data and which hold metadata | each device's identity (`mantle-disk`), and its calibration: flush cost, durable bandwidth, read depth, read gap, first-write penalty (chunk-store.md §2, §7) | every queue bound, window and budget in §2.6, §3.3 and §4 |
| listen addresses | the memory the operating system allows the process: cgroup `memory.max` and `memory.high` on Linux, job limits on Windows, physical memory otherwise | the tick period and election timeout (§2.4) |
| the cell to create or join (§1.5), and the node's failure-domain labels: zone, rack | granted cores | the scheme each block is stored in (durability.md §4) |
| the failures to survive, `f`, and the domain level they are counted at (architecture §11) | each peer's round-trip time and delivery rate, from the transport | the members a range needs and where they go (§6.5) |
| the durability target: unless stated, a block's annual chance of loss at most 10⁻¹¹, S3's eleven nines applied per block (durability.md §1) | flush, apply, snapshot and scheduling delays; an idle engine instance's cost (12 §6.1) | what the node reports it can survive |
| where the root key comes from: a key file or a key service (encryption.md §2) | the cost of sealing, hashing and coding a segment or a block (`mantle bench hash`, `mantle bench ec`) | |
| optionally: a memory ceiling, the share of a caller's deadline a queue may take, and a restart-time budget (11 §18 items 3–4) | | |

The two policy shares have no model that storage can supply (11 §18). Until a deployment states
them, a queue is bounded at the lower bound its own service time sets, and checkpoints are
triggered by space alone (11 §4.5, §9), as the chunk store does today.

**Memory.** When the operator states no ceiling, the node's budget is the smallest that admits
one unit of work of every class at once: one PUT's block window and its coding work space, one
GET's block, one `Ready` of the largest entry for each range with work, the engines' minimum
write buffers, one transfer slot per peer. That floor is computed from the settings, and the
node refuses to start, naming the class, when it exceeds what the operating system allows.
Independent defaults, each safe alone, add up past a laptop's memory: the chunk store's pools
alone can keep 96 MiB per volume (audit §12.3). A laptop needs the smallest population that
serves its work (audit §6.3), and more is granted by stating a ceiling.

A configuration key the node does not know is refused, as the S3 layer's readers refuse what
they do not know. `mantle status` prints every derived value with the input or constraint that
bound it (audit §12.6).

### 1.5 Identity and bootstrap

**A node** is named by a random 128-bit ID drawn from the operating system at `mantle init`,
with a key pair from AWS-LC and a certificate from its cell's authority. Neither is ever
reused: a node that loses its disks comes back under a new identity, as a Raft member that lost
its state must (replica.md §6).

**A cell of one.** `mantle init` on a laptop creates the cell's certificate authority, the
node's identity, the root key's first generation (encryption.md §2) and a configuration naming
the data directory. The first `mantle serve` finds its logs empty and creates the cell's
ranges, each a group of one voter from `Range.boot`: the cell's root range (its membership,
volumes, range directory and root-key generations), the region's root range holding the cell
map (architecture §4), a Bucket range, and one range each for the Name, File and Block layers
spanning its whole key space. In a fleet the region's root range is hosted apart from any one
cell's ranges; on a laptop it is one more range on the one node. The
node reports at startup that it survives no device failure, as README's table says it must.

**Joining.** An existing node mints an invitation (`mantle invite`): a one-time token recorded
in the root range, so the log decides whether it was used. `mantle init --join` sends the token
and a certificate request over the enrollment protocol, whose key the joining node keeps; the
authority signs a certificate whose subject it chooses itself, as focal's enrollment does
(07 §4.1). The new node is recorded in the root range with its failure-domain labels and
devices. It then hosts replicas as placement adds them: each as a learner, caught up, and made
a voter by one joint change (replica.md §6), until every range has `2f + 1` voters one to a
domain (§6.5). Until the cell has the domains `f` needs, the node reports the failures it can
survive, and placement uses the domains there are.

**Restart** needs nothing but the node's own directories. It reads its membership, its peers'
addresses and the last cell map it persisted, and serves from them while the control plane is
down (architecture §4, §9).

Where the authority's private key lives once a cell has many nodes is open (§11); on a laptop it
is in the node's directory.

### 1.6 Directories

```
<node directory>/                     not on a data device (encryption.md §2)
  config                              what the operator stated (§1.4)
  identity/                           node key, certificate, the cell authority's certificate
  keys/                               root-key file, owner-only, one per generation
  map                                 the last cell map and membership, persisted as received
<device directory>/mantle/            one per device the node uses
  LOCK                                held while the node runs: one owner per device (audit §14.1)
  DEVICE                              node ID, device ID, profile version, CRC-32C
  profile                             the device plan: identity, calibration, the policy chosen
  volume                              the chunk volume, when the device holds data
  meta/                               when the device holds metadata
    log                               the shared Raft log (raft-log.md)
    ranges/<replica ID>/              one engine instance per range replica (12 §6.1)
    staging/<replica ID>/<snapshot>/  a snapshot being received, before it is installed (§6.3)
```

A volume on a raw device is named in the configuration instead of `volume` (chunk-store.md §2).
The `LOCK` is an exclusive advisory lock (`flock`, `LockFileEx`) naming the node's incarnation,
so two processes, or one process through two paths to one device, never own one volume or log
(audit §14.1). `profile` is the persisted device plan the audit asks for (audit §6.1, §14.2): it
separates the format choices fixed at format from the runtime choices, and it is measured again
when the device's identity, firmware, file system or topology no longer match it. On a laptop
the key file and the data share a disk, which the node reports.

### 1.7 Starting, stopping, rolling back

Startup runs in this order, each step owning what it starts:

1. Read the configuration and identity; take each device's `LOCK`.
2. Identify every device and compare it with its `profile`. A device whose profile no longer
   matches is calibrated again, within the scratch bounds calibration keeps; a device whose
   durability or write constraints mantle cannot meet is refused before anything is written
   (audit §6.2).
3. Build the storage graph from the identities, so volumes and logs that share a physical
   device share its admission authority (§2.5), and derive the budgets (§2.6).
4. Open the volumes and the logs, which recover (chunk-store.md §6, raft-log.md §6); open each
   replica's engine and replica, which replays past the engine's durable index (replica.md §4).
   A member whose log recovery cut back past what its engine applied opens `Damaged`: the node
   records a new identity for the range on that device, then rebuilds the member under it
   (`Replica::rebuild`), and the range's leader adds it as a learner and swaps it for the old
   one once it has caught up (replica.md §6).
5. Start the network runtime; bind the QUIC endpoint and the datagram socket; start SWIM; dial
   the peers the persisted membership names.
6. Hand the replicas to the shards, which start ticking.
7. Open the S3 listener last.

A step that fails stops what the steps before it started, in reverse, and returns its error: the
volume's start already does this for its own threads (audit S12), and the node applies the
same rule to every owner. No thread outlives a failed start.

A stop is safe at any instant, because nothing is acknowledged before it is durable and every
owner recovers from its own records. An orderly stop is kinder to the cell: the client
listeners stop taking connections (native connections are told to drain by the protocol's own
close; HTTP/1.1 answers carry `Connection: close`), requests in flight finish within their deadlines, each range
this node leads hands leadership to a successor and waits until the successor has shown it
leads, since a leader that only steps down can leave a rolling update with no leader (audit
§11.7), and then the shards finish their outstanding `Ready`s and the logs and volumes close.
A stop that runs past its requests' deadlines stops the rest as a crash would.

**Versions.** Every on-disk format already names its version (a volume's superblock, a log
segment's header, an entry's format byte in `mantle_meta::wire`), and a connection's first
exchange names the versions each side speaks, as focal's does (07 §4.1). A binary refuses a
format it does not know rather than guess. A new entry or record format is written only once
every member of a range has said it can read it, the pattern of focal's decoder fences (07
§2.5), so a node can be rolled back to its previous binary until that point, and a
mixed-version cell runs in the meantime (audit §8.7).

## 2. The metadata scheduler

### 2.1 Ranges on shards

Each replica belongs to one shard for its life on the node, placed on the shard with the least
measured work. A shard holds replicas of ranges whose logs are on any of the node's metadata
devices; a device's ranges spread over every shard, so one device's replicas are not held to
one core. Cross-range group commit does not depend on which thread submits: each log's writer
takes everything queued while its last flush ran (raft-log.md §3).

A shard runs a loop over a ready queue of ranges with something to do: a message arrived, a
timer came due, commands or reads are waiting, or a ticket completed. An idle range costs
nothing but its timer (§2.4).

### 2.2 A range's turn

The replica API already has the shape audit §5.1 asks for: `begin` gives out the messages a
leader may send before its own write, submits the update without waiting, applies what is
already committed, and returns with `persisting` set; a later `begin` finishes the `Ready` once
its update is durable (replica.md §3). A range's turn does this, in order:

1. Take the messages that arrived, with `step`. While the range's `Ready` is out the replica
   holds them, within one flow-control window of appends per member, and refuses the rest with
   `MessagesHeld`, which the shard counts and drops, as the network may (replica.md §3).
2. Take the ticks that came due, with `tick`.
3. If no `Ready` is out: propose the commands waiting for the range as one entry of at most
   `max_entry_bytes` (replica.md §1), and start the reads waiting, with `read_index`.
4. Call `begin`. Hand every message it gives to the transport at once: a leader's appends go
   out while its own write flushes, and a follower's acknowledgement is given out only once its
   write is durable, which the replica enforces. Answer the commands in `applied` to the
   gateways waiting for them, serve the confirmed `reads` from the engine once it has applied
   their index, and give `unconfirmed` reads back to their callers.
5. If `persisting` is set, the range waits for its ticket; its completion puts the range back on
   the ready queue, and its next `begin` finishes the `Ready`. If `stalled` is set, the log has
   refused the update for want of room: the range waits on the device authority (§2.5), which
   asks the engines on that log to flush so the ranges can compact (§6.2), and wakes it when a
   compaction frees room. If the replica answers `Fenced`, its log failed to make a write
   durable and nothing the process holds says what the device kept; every replica on that log is
   taken off its shard, the log is reopened, which recovers what is durable, and its members
   open afresh from it.

A shard never calls `drive` or `wait_persisted`, which wait on the log. It begins every
runnable range in one pass before any of their flushes completes, so their updates meet in one
frame on each log, which is what audit §5.1 asks a scheduler to do.

While a `Ready` flushes the replica refuses proposals, reads and campaigns with `Stalled`
(replica.md §3, §7). The shard holds a range's waiting commands and reads in the range's own
queue instead, and proposes them at the next turn with no `Ready` out, so no caller sees
`Stalled`. The queue is bounded in bytes by Little's law: the range's measured command rate
times the delay the queue may add, and at least one largest entry, so the largest command a
range accepts always fits (11 §4.4–§4.5). Past the bound a command is refused `Busy`, which the
gateway answers with `503 SlowDown` or retries within its deadline (§5.4).

More than one `Ready` of a range in flight would halve each update's wait (replica.md §7) and
needs a core that hands out `Ready`s ahead of their persistence; it stays open.

### 2.3 Fairness, and the reserve for control

Within a shard, ranges with data work take turns by deficit round robin (25 §7). Each round a
range's deficit grows by a quantum and its turn spends it in bytes of work: the encoded update
it submits and the committed entries it applies. Deficit round robin keeps each backlogged
range within one largest unit of its fair share over any number of rounds, and does O(1) work a
turn when the quantum is at least that unit: "The Work for Deficit Round Robin is O(1), if for
all i, Quantum_i ≥ Max" (25 §7). The quantum is therefore the largest unit a turn can carry: one
`max_entry_bytes` entry proposed plus one `Ready` of `max_committed_size_per_ready` applied.
This replaces the replica's count of 64 `Ready`s a drive, which bounded neither bytes nor time
(audit §12.6).

Control comes first in every pass: ticks, votes, heartbeats and their answers, and the second
half of `Ready`s whose updates are durable. Strict priority protects urgent work only when the
urgent work is itself bounded (audit §13.3), and this work is: each range's held messages are
bounded by its flow-control window, and its ticks by one election timeout (replica.md §3). A
pass spends at most one round of data work before it returns to control, and a round is
bounded in bytes by the quanta of the ranges in it.

Fairness among tenants is not the shard's: a range's commands come from the gateways' sessions
in the order the gateways admitted them, and a tenant's share is decided at the gateway
(architecture §8; §4.5). The shard's fairness is among ranges, so one hot range cannot hold a
core that other ranges' leaders need for their heartbeats.

### 2.4 Time

focal-raft counts ticks and reads no clock (07 §5.1). A shard keeps a timer wheel of each
range's next tick, as slates' runtime keeps timers, whose cost is per event rather than per tick
(08 §3.2), and ticks a range only when its tick is due.

The tick period is derived, as focal-timing derives it: `election_tick × period ≥ 10 × tail`,
where the tail is the slowest voter path's (07 §5.1). The counts of ticks are focal's, an
election after 10 and a heartbeat every 2 (07 §2.2), so an election timeout is at least ten tail
round trips and a heartbeat interval at least two. Raft's timing requirement is `broadcastTime
≪ electionTimeout ≪ MTBF`, and the dissertation puts broadcast time at 0.5–20 ms "depending on
storage technology", because a receiver persists before it replies (06 §A1.1); ten is focal's
reading of "≪". mantle's tail is therefore the tail of an append's durable acknowledgement, which
adds to the network's: the path's round trip, the follower's scheduling delay and its log's flush,
each measured (audit §11.7). A leader still heartbeats at every tick. A node with thousands of
ranges pays `L(r − 1)/h` heartbeat sends a second for `L` led ranges of `r` members at heartbeat
interval `h` (audit §15.1); heartbeats to one peer are sent together in one datagram (§3.4), as
CockroachDB coalesces them per node (06 §A4.3). Letting idle ranges stop ticking altogether, as
CockroachDB and TiKV do, needs a protocol of its own for waking a quiesced range without
weakening its election or read rules (audit §15.1), and is open (§11).

A range's time for its entries is its leader's clock, stamped on each entry and never moving
back (metadata.md; `mantle_meta::clock`). The offset between nodes' clocks bounds the grace that
handover deadlines and collection need (metadata.md §6); the node estimates it from its probes'
timestamps, each estimate's error bounded by the probe's round trip.

### 2.5 One admission authority per bottleneck

A per-volume queue cannot bound a device that several volumes, a log and many engines share
(audit §14.1). The node keeps one admission authority for each resource that can saturate, and
every piece of work reserves from each authority it will use before it allocates:

- **Memory**, one for the node: a tree of budgets in the form of focal-memory's `MemoryBudget`
  (07 §5.3), whose children are the classes of work (S3 requests, range replicas and their
  queues, engines, transport buffers, snapshots, coding) and whose completion lane keeps back
  what admitted work needs to finish, so work admitted under pressure can still complete.
- **Each physical device**, found through the storage graph so that two volumes, a log and the
  engines on one SSD share one authority: outstanding operations, bytes and estimated device
  time, with shares for foreground writes and reads, the log, engine flush and compaction, the
  cleaner, the scrubber, repair and snapshot reads. The engine's rate limiter for the device is
  this authority's share for it (24 §4.7).
- **Each peer path**: bytes in flight by class (§3.3).
- **The coding pool**: jobs and their work space, reserved from memory.
- **Each tenant**: the gateway's admission (§4.5).

A reservation is held from the moment work is admitted until its answer is delivered: through
the queue, while deferred, while it executes, and while its response waits to be sent. A
dequeue does not return room. That is the rule S03 fixed in the log's queue, where accounting
that ended at dequeue let an unbounded backlog form behind a bounded channel (audit S03, §9 item
2), and every stage of the node keeps it. A reservation is released on completion or
cancellation, never by a timeout that leaves the work running.

### 2.6 Where the budgets come from

| Budget | Model | Measured inputs | Source |
|---|---|---|---|
| A queue's bound in requests and bytes | Little's law: `Q = μ·d`, `Q_bytes = BW·d`, with `d ≥ S + 4·dev(S)`; refuse when the minimum sojourn over an interval exceeds `d` (CoDel) | service rate `μ`, bandwidth `BW`, service time `S` and its deviation, sojourn times | 11 §4.4; `d`'s share of the caller's deadline is policy (§1.4) |
| A device's read depth | where throughput stops growing | calibration | chunk-store.md §7 |
| Background shares on a device | the largest share that keeps measured foreground latency within its bound; raised while blocks have lost redundancy | foreground latency under cleaning, scrubbing and repair | 11 §18 item 7; 04 R3 item 4; 10 §1 item 10 |
| The log file's size | twice the live bytes: append rate times the engine's durable lag | both, measured by the log | raft-log.md §5 |
| Engine write buffers, node total | `min(memory share, replay rate × restart budget)` | replay rate at each open | 12 §6.2, §6.6 |
| A range's `max_entry_bytes` | the largest command a gateway sends: a completion of 10,000 parts, 561,804 bytes | `mantle_meta::wire::largest_entry_bytes` | replica.md §7; audit §16.5 |
| Sessions a range holds | the gateway incarnations that may hold a live session (§5.3) | the cell's membership and its gateways' restarts | replica.md §1 |
| A PUT's block window `w` | `w = ⌈R·T/B⌉`: body rate times the block chain's latency over a block's bytes, capped by memory | the body's arrival rate, each chain's latency | gateway.md §2; audit §16.3 |
| A GET's block window | the same, with the client's read rate | the rate the client takes bytes | gateway.md §3 |
| A peer's receive window | `min(BDP, the peer's share of the transport budget)` | delivery rate and round trip per peer | §3.3; audit §13.4; 25 §4 |
| Entries a range keeps for lagging followers | keep entries while resending them costs less than a snapshot: retained bytes ≤ the range's engine size | both sizes | raft-log.md §4; 06 §A4.3 (CockroachDB chooses by the writes missed) |

Every row changes as its inputs are measured again. None is a constant in the code, and each
appears in constants.md as derived, with its model.

## 3. Transport

Nodes talk over two layers, as the hecate specification lays them out (07 §4.7): QUIC for
everything stateful, through quinn with rustls on AWS-LC (crypto.md), and a separate plane of
sealed UDP datagrams for consensus control and membership. Neither is built; focal-wire's
transport core is the starting point for the first, ported rather than depended on, since the
crate is coupled to focal's domain (07 §4.8, §7.2 C), and slates' unwired control-datagram codec
is the starting point for the second (07 §4.7).

### 3.1 Classes of message

| Class | What it carries | How it travels |
|---|---|---|
| Control | Raft votes and pre-votes, heartbeats, the answers to appends and heartbeats, leadership transfer; SWIM probes | the datagram plane, when the message holds no entries and fits one datagram; ahead of everything |
| Replication | Raft appends carrying entries, and any control message too large for a datagram | one QUIC stream per metadata shard to each peer, above requests |
| Request | a gateway's commands to ranges and their answers, range reads, chunk reads and writes for S3 requests | one QUIC stream per request, a share per tenant |
| Bulk | snapshots, repair, rebalancing, moves between cells | one QUIC stream per transfer, below requests, with a floor (§3.3) |

The class is decided by what a message is, never by the stream a peer chose: a peer cannot make
a snapshot urgent by sending it on a control stream (audit §13.3), and every message is checked
against the classes its sender's role may use (§3.6). A large snapshot sent as control starves
elections (audit §13.3), which is why snapshots move out of band and in bounded slices (§6.3).

### 3.2 QUIC connections and streams

A node keeps one QUIC connection to each peer it works with, shared by every range, request and
transfer between the two, as focal does (07 §4.2). It does not keep focal's limit of two
exchanges in flight per peer, which throttles thousands of ranges (07 §4.6, §7.2 C).

Replication messages from one shard to one peer travel on one long-lived stream, so a range's
messages keep their order and the stream count does not grow with ranges. A message on it is a
frame: a fixed header read into a fixed buffer, with its class, the range, the sender's term and
descriptor generation, its length and a CRC-32C of the frame (CLAUDE.md §6), then the body. The
receiver checks the length against the class's bound and takes a reservation before it reads the
body (audit §11.8, where focal allocates whole frames before admitting the handler). A
replication stream is always read: a message whose range cannot take it is dropped and counted,
as the replica already refuses what it cannot hold, so one full range never stalls the stream
for its shard.

A request is one bidirectional stream: a frame each way, as focal's exchanges are (07 §4.4). A
bulk transfer is one stream read only into reservations, so flow control holds the sender back
while the receiver has nowhere to put the bytes.

Priorities are quinn's: "Locally buffered data from streams with higher priority will be
transmitted before data from streams with lower priority", with round-robin among streams of one
priority when `send_fairness` is on (25 §4). Replication is above request and request above
bulk.

0-RTT is off. "Disabling 0-RTT entirely is the most effective defense against replay attack"
(25 §5), rustls's default is already none (`max_early_data_size` "The default is 0", 25 §5), and
a node's first message on a new connection may be a mutation.

### 3.3 Credits

focal's settings allow about 144 MiB of receive and send window per connection, 18 GiB over 128
connections, outside any budget (audit §11.8). mantle's windows come from its memory.

QUIC's flow control is limit-based: the receiver advertises how much it will take, and raises
the limit as the application reads (25 §4). So what a node buffers for a peer is bounded twice:
by the connection's receive window, which quinn buffers at most, and by the reservations the
node holds for what it has read. quinn states the first: "Worst-case memory use is directly
proportional to max_concurrent_bidi_streams * stream_receive_window, with an upper bound
proportional to receive_window" (25 §4).

A peer's receive window is `min(BDP, share)`. The bandwidth-delay product is the peer's measured
delivery rate times its round trip, which quinn's own guidance asks for ("at least the expected
connection latency multiplied by the maximum desired throughput", 25 §4). The share is the
peer's part of the node's transport budget: while the peers' products sum within the budget each
gets its own, and past it each is scaled in proportion. quinn lets the node change a live
connection's receive window and its stream count (`Connection::set_receive_window`,
`set_max_concurrent_bi_streams`, 25 §4), so windows follow the budget as it changes. The send
window, which bounds what quinn retains of the node's own writes, is set the same way, since
"Endpoints that wish to handle large numbers of connections robustly should take care to set
this low enough to avoid memory exhaustion" (25 §4). The per-stream window is fixed when a
connection is made; it is set to the peer's BDP then, so one bulk stream can fill the path, and
the stream count bounds how many can.

Each class then takes the audit's rule, `window ≤ min(stream budget, connection share, node
remaining budget, admitted sink capacity)` (audit §13.4). Received bytes are read only into a
stage that is itself budgeted: a chunk being written is read into an assembly slot the size of
the chunk, reserved first and handed to the volume's ticket whole, so a chunk larger than the
window never deadlocks and bytes read never pile up ahead of a busy disk (audit §13.4).

Bulk work is lowest, but it has a floor. A cell must rebuild its largest failure domain within
the time its durability target allows, so repair needs at least `D/T` of bandwidth for lost
capacity `D` and repair time `T` (audit §14.3; durability.md §5). While repair is behind that
rate, the gateways admit request-class chunk traffic against the path's capacity less the
floor, so strict priority never starves repair for good (audit §13.3).

### 3.4 The datagram plane

**Why a separate socket.** QUIC's own unreliable datagrams "employ the QUIC connection's
congestion controller", which must "either delay sending the frame until the controller allows it
or drop the frame" (25 §6): a vote sent that way shares its window with bulk transfers, and is
delayed or dropped whenever they have filled it. A socket of its own escapes that. It takes on
what the transport would have done: RFC 8085 asks a UDP application that forgoes a
congestion-controlled transport to "control the rate at which it sends UDP datagrams to a
destination host", "not sending on average more than one UDP datagram per RTT" when it sends
few, and never to exceed the path MTU (25 §6).

**What it sends.** A control message goes on the plane when it carries no entries and its
encoding fits the plane's datagram size; otherwise it takes the replication stream. Every
heartbeat for one peer due at a tick is packed into one datagram, as far as it holds them. The
plane's rate to a peer is thus one packed datagram a heartbeat interval, plus replies clocked by
what the peer sends, and a heartbeat interval is at least two tail round trips (§2.4), which
keeps what the plane originates within RFC 8085's one datagram per round trip. Nothing is
retransmitted; Raft retries, and SWIM probes again. A datagram is no larger than
the largest quinn reports for the same peer, which follows its path MTU estimate (quinn's
`max_datagram_size` "may change over the lifetime of a connection according to variation in the
path MTU estimate", 25 §4), and no larger than RFC 8085's fallback, 1,280 bytes on IPv6 or 576 on
IPv4, before a path has been measured (25 §6).

**Keys.** A datagram is sealed with AES-256-GCM under a key derived from the TLS session of the
QUIC connection to the same peer, through the exporter both ends can compute
(`Connection::export_keying_material`, 25 §4; RFC 8446 §7.5, 25 §5), with one key for each
direction. The key's epoch is the connection's: every new connection gives new keys, so a
restarted node never reuses a nonce under an old key, the obligation audit §11.8 names for
slates' counter-zero sealer. A datagram's cleartext prologue names the sender, the epoch and a
counter that only rises; the prologue is the AEAD's associated data and the counter its nonce.
The sealed body carries its CRC-32C, as every payload does (CLAUDE.md §6).
The plane therefore authenticates exactly whom the QUIC handshake authenticated (§3.6), and a
peer with no live connection gets no datagrams.

**Receiving.** The receiver checks, in order, the length, the prologue, that it holds the
epoch's key, the replay window, the tag, and only then decodes, the acceptance order of slates'
codec (07 §4.7). The replay window follows RFC 4303: the right edge is the highest counter
verified, a counter left of the window or already seen is dropped before any cryptography, and
the edge moves only after the tag verifies (25 §6). Its width starts at RFC 4303's default of 64
and widens to the reordering the plane measures on the path, within memory the transport budget
reserves.

### 3.5 Membership and failure detection

SWIM runs on the datagram plane within a cell: a probe each protocol period to a member taken
round-robin from a shuffled list, indirect probes through other members on a timeout, and
suspicion before a member is declared failed (06 §A8.2). The period is at least three times the
round-trip estimate, as SWIM requires (06 §A8.2). What SWIM concludes is a hint for routing and
for when repair starts; membership that decides anything lives in the root range, and no Raft
election or read depends on it (06 §A8).

Lifeguard found that "slow processing by the failure detector module itself is the primary cause
of the false positives that SWIM's Suspicion mechanism fails to suppress" (25 §8). mantle has
that failure mode exactly: a node whose runtime or shards are behind reads its probes late. The
node takes Lifeguard's remedy, stretching its own probe timeouts when it is itself slow, but not
Lifeguard's counter, whose limits and scores "currently use heuristically determined values"
(25 §8). It measures its own lag directly, the delay between a probe's arrival and its handling,
and stretches its timeouts by that delay's tail.

### 3.6 Identity and authorization

Every connection is TLS 1.3 with certificates on both sides, verified against the cell's
authority (rustls's `WebPkiClientVerifier`, 25 §5), under an ALPN of mantle's own; enrollment
has its own ALPN (§1.5). A peer's certificate maps to its row in the root range: node ID, role
and state. The mapping is checked again on every new stream, so revocation reaches open
connections, as focal re-checks its registry (07 §4.1).

A role permits classes: a range host sends control and replication for ranges it hosts; a
gateway sends requests to ranges and volumes and never a Raft message; a storage node answers
chunk requests. A Raft message's sender must be the node its certificate names, as focal's
`step_authenticated` binds `from` to the certificate (07 §2.2), and must be a member of the
range's configuration or a learner it is adding. Node identity authenticates the peer; the
range, its descriptor generation and the operation's identity in each frame decide whether a
message may act (audit §11.8), and a range refuses a frame from a term or generation it has
left (architecture §5).

### 3.7 Defenses

A peer that is buggy or hostile cannot make the node do unbounded work:

- A frame's length is checked against its class's bound before its body is read, and its
  reservation is taken first (audit §11.8). Decoding is bounded by the bytes that follow each
  length, as `mantle_meta::wire` already decodes (wire.rs), so a malformed frame is an error and
  never an allocation it cannot fill.
- Handshakes in progress, connections per identity and streams per connection are bounded by
  the budget (07 §4.1's admission; 25 §4). QUIC's own advice for slowloris is to limit
  connections and impose "restrictions on the minimum transfer speed" (25 §4); mantle bounds
  what a slow peer holds by its reservations and, when the budget is short, closes the
  connection that has made the least progress for longest.
- A datagram costs at most a lookup and one tag check before it is dropped (§3.4).
- Stale answers, from an old term, generation or incarnation, are dropped by the check that
  names them, before they reach a replica. QUIC's own advice is to "track cost of processing
  relative to progress" (25 §4): a peer whose frames are mostly refused as stale or malformed is
  disconnected.
- Raft does not retry on top of QUIC's retransmission for replication: a lost stream is
  reopened and the core's own probe resends what is missing (audit §13.3).

### 3.8 Slow, congested and unstable paths

Every deadline between nodes is a progress deadline: bytes remaining over the measured delivery
rate plus the path's tail delay and the remote side's measured service time (a flush for a
write), within the caller's overall budget (audit §13.5). A fixed five-second call on a 64
kbit/s path fails a healthy 128 KiB append (audit §13.5); a deadline computed from the path lets
it finish, and says when the path cannot sustain what is asked.

On a thin path the bulk class keeps no more bytes queued at the sender than the path delivers
in one tail round trip, so a control datagram waits behind at most that much bulk, which the
election timeout allows for ten times over (§2.4). Strict priority cannot preempt a packet
already on the wire (audit §13.1), so the queue ahead of the wire is what the node controls.
QUIC's minimum datagram is 1,200 bytes (25 §4), which takes 1.2 seconds at 8 kbit/s (audit
§13.1); on such a path the tick stretches with the measured tail, elections take tens of
seconds, and the node reports the append rate the path can sustain rather than refusing to run.

After an outage the node reconnects with exponential backoff and jitter bounded by the
membership's measured round trips, so a cell does not reconnect in step. A reconnect presents
the same identity and revalidates its certificate and the ranges' epochs. Bulk transfers resume
from their last verified slice (§6.3). The network matrix of audit §13.6 is how these rules are
qualified (§9).

## 4. The S3 front end

### 4.1 The protocols

Clients reach a node two ways, and both lead to the same request path (§4.2), drivers and
admission authority.

- **mantle's own protocol over QUIC** is the native one, as slates carries its own (08): QUIC
  from quinn with rustls on the AWS-LC provider (25 §5), with S3's semantics (buckets, keys,
  versions, multipart uploads, conditional requests, checksums, storage classes) as its
  operations, used through mantle's client library and CLI. It is the same transport the nodes
  speak to each other (§3), so a client gets independent streams with no head-of-line blocking
  between requests, connection migration as a laptop moves between networks, and admission
  signals and credits before it sends data. Its framing, flow control, priorities, resumption
  and congestion control are designed in research notes 27 (upload scheduling), 30 (resilient
  transfer) and 31 (caching, ordering and integrity).
- **An HTTP/1.1 listener speaking the S3 wire protocol,** on by default, so stock S3 tools (the
  AWS CLI, boto3, the AWS SDKs, rclone) work unchanged; it translates each request onto the same
  path. HTTP/1.1 is the only HTTP version, because it is the only one Amazon S3 serves: offered
  HTTP/2 by ALPN, its endpoints (`s3.amazonaws.com`, `s3.us-east-1`, `s3.us-west-2`,
  `s3.dualstack.us-east-1` and `s3express-control.us-east-1`) answered `http/1.1`, and none
  advertised HTTP/3 by `Alt-Svc` (checked 2026-09-30; AWS documents HTTP/3 only for CloudFront).
  HTTP/2 and HTTP/3 are not offered. The listener is hyper's HTTP/1.1 server over TLS from rustls.
  On a laptop it may serve plain HTTP on the loopback address; SigV4 still authenticates every
  request there, and a request carrying SSE-C headers is refused over plain HTTP, as S3 "rejects
  any requests made over HTTP when using SSE-C" (20 §3.3).

hyper's defaults are its own choices ("Default is 200, but not part of the stability of hyper
... You are encouraged to set your own limit", 25 §2), so the node sets each from its budget:

- HTTP/1.1's buffer holds one request head at S3's limit of 8 KB (05 §10.2) and the body
  segment being read. hyper panics below 8,192 bytes (25 §2), so the node checks the value
  before the call, as CLAUDE.md §1 requires of any dependency that can panic.
- Header lists are bounded at S3's 8 KB (05 §10.2).
- No header timer. hyper's `header_read_timeout` defaults to 30 seconds and panics when set
  without a timer (25 §2). A slow client holds only its connection's reservation; when the node
  needs the room, it closes the connection that has made the least progress for longest.

### 4.2 A request's path

1. **Admit the connection** against the connection budget.
2. **Route** with `mantle_s3::route`: operation, bucket and key, by virtual-hosted or path
   style. A header or query parameter that appears twice is resolved once, the same way for
   routing, signing, policy and the body (audit §9).
3. **Authenticate** with `mantle_s3::sigv4`: the `Authorization` header, a presigned URL, or
   anonymous. Owner identity, SSE-C authority and bypass privileges come from what authentication
   established, never from fields a caller supplies (audit §9).
4. **Authorize**: the bucket's row from the Bucket range, through a cache whose staleness is
   bounded (metadata.md §1, §6), and its policy judged by `mantle_s3::policy`.
5. **Admit the request**: the tenant's share (§4.5) and the request's working set from memory,
   the devices and the peers it will use. A PUT reserves its first block's window and, when its
   scheme is coded, a coder's work space; a GET its first block.
6. **Answer or continue.** Refusals so far need no body. hyper sends `100 Continue` only when
   the body is first read (25 §2), and RFC 9110 lets a server answer "with a final status code"
   in its place (25 §2), so a request refused here is refused before a client using
   `Expect: 100-continue` sends its body, which is what S3 tells PUT clients to rely on: "If the
   message is rejected based on the headers, the body of the message is not sent" (25 §3).
7. **Run the drivers**: a `Put`, `Get` or `Completion` from `mantle-gateway`, and the S3 layer's
   own documents for everything else. Each driver's requests are served as §5 describes.
8. **Respond** with `mantle_s3::response`'s documents, streaming a GET's bytes as the `Get`
   driver gives them out.

### 4.3 Bodies

A body is read only when the `Put` driver `wants_body`, one segment at a time, and backpressure
reaches the client through the native protocol's flow control or, on the HTTP/1.1 listener, TCP's. An `aws-chunked` body is decoded as it
streams by `mantle_s3::chunked`, each chunk's signature checked, and a trailing checksum checked
at the end. Nothing is committed in the Name range until the body's length, digests, chunk
signatures and trailer have all checked (audit §9): the `Put` driver takes `end` with the
expected values, and only then writes the file and commits the version (gateway.md §2).

The memory a request holds does not grow with its object. A PUT holds its window of blocks and
the segment being sealed; a GET holds its window of blocks, and a slow reader holds only that
window and its pins (audit §16.7). A 5 GiB part streams through the same slots as a small one
(audit §16.2). Each block's chain of placement, chunks and Block row runs as its own flight
within the window, which audit §16.3 asks for; a window of one serializes them, and the window
grows while the body arrives faster than one chain drains, up to what memory admits (§2.6).

A body that stops arriving holds its reservation and its blocks, which the driver keeps renewing
(gateway.md §2), for as long as the node has room: multi-day uploads on slow links are part of
the workload (audit §16.2). When the node needs the room, the request that has made the least
progress for longest is ended with `RequestTimeout`, S3's answer for a connection "not read from
or written to within the timeout period" (25 §3), and what it wrote is left to the sweeps.

### 4.4 Errors

Each driver's error maps to S3's answer, and a failure whose outcome is unknown is never
reported as success or as a refusal with no effect:

| Outcome | S3 answer |
|---|---|
| `PutError::EntityTooLarge` | 400 `EntityTooLarge` |
| `IncompleteBody` | 400 `IncompleteBody` |
| `BadDigest`, `BadChecksum` | 400 `BadDigest` (05 §3.2) |
| `InvalidPartNumber` | 400 `InvalidArgument` (`mantle_gateway::put`) |
| Name refusals: a failed precondition, a missing bucket, a lock | 412 `PreconditionFailed`, 404 `NoSuchBucket`, 403 `AccessDenied`: each the code S3 gives it (05 §11.2) |
| `Unplaced`, admission refused, a range splitting or frozen for a move | 503 `SlowDown` |
| An internal step whose outcome is unknown after the request's retries (§5.4) | 500 `InternalError`, which clients retry |

A body with both `Transfer-Encoding` and `Content-Length` is refused 400 and the connection
closed, as RFC 9112 requires for a framing error in a request (25 §2).

### 4.5 Overload

A tenant's share is decided at the gateway, by tokens from the cell's admission service, as
DynamoDB's routers hold tokens from its global admission control; per-range and per-node limits
remain as ceilings (architecture §8; 09 §3.6). Over its share, or when an authority cannot fund
the request, the answer is a refusal before the body: on the native protocol a typed refusal
with the wait it derives (research 27), and on the HTTP/1.1 listener `503 SlowDown`, where a
refusal to a client that did not wait for `100 Continue` carries `Connection: close` and the body
is not read.

SlowDown sheds load only if its cause lasts no longer than clients retry. Under the standard
retry mode as AWS now documents it, a throttled request is tried three times, backing off a
random delay below one second and then below two (25 §3); SDKs that have not opted in to that
behavior differ in timing (25 §3). So the node refuses for conditions measured in those units:
an admission queue whose sojourn exceeds its target, a range being split or frozen, a peer out of
credit. A condition that outlasts the retries, such as a cell out of space, is reported as what
it is, and its remedy is operational. Whether to send `x-amz-retry-after` is open, since whether
S3 sends it is unverified (25 §3).

### 4.6 Keys for encryption

The root key's generations are decided once for the whole cell (audit §9). Each
generation is a row in the cell's root range: its number, when it was made, and a fingerprint of
the key, never the key. The key material comes from the source the configuration names: a key
file per generation in the node's `keys/` directory, or a key service through the same wrap and
unwrap calls (encryption.md §2). A node whose key does not match the committed fingerprint
refuses to seal or open under that generation, rather than sealing under a key other nodes do
not hold.

A rotation commits the new generation's row first; nodes load its key; new files take it once
every gateway reports holding it; the rewrap pass then moves older files' data keys to it
(encryption.md §2). A generation is retired only once no header names it. Backing up the key
files is part of backing up the cell: a cell restored without them holds ciphertext nobody can
open (audit §8.7, §14.4).

## 5. Serving the drivers' requests

The drivers name what they need and take the answers (gateway.md). The node's routing layer
serves each request.

### 5.1 Routing

| Request | Served by |
|---|---|
| `Place` (put) | the node itself, from the cell's volume table (§7) |
| `Chunk` (put and get) | the volume's node: in-process through a ticket, or over the request class |
| `Block`, `File`, `Name` commands (put, completion) | the leader of the range whose span holds the block, file or key |
| `Header`, `Extents`, `Block`, `Upload`, `Parts` reads (get, completion) | the leader of that range, by ReadIndex (replica.md §3) |

A range is found by a floor lookup on the key in the node's cached range descriptors, each with
its generation and membership epoch (architecture §5). A range that has split, merged or moved
refuses a stale descriptor with its own, or with where its span went (metadata.md §3), and the
node learns it and sends again; a node whose descriptors no longer cover a key reads the
directory from the root range. A range whose leader has moved answers with the leader it knows.
A stale route costs a round trip and cannot produce a wrong answer (architecture §5).

### 5.2 Chunks

A chunk write carries the bytes and their CRC-32C, which the volume checks before it writes
(`Volume::put_checked`), and is answered `Stored` once durable. The volume's own answers map to
the driver's: `Busy`, `Full`, `Fenced`, a failing volume, or no answer by the progress deadline
all become `Refused`, and the driver sends the chunk to the next volume placement offered
(gateway.md §2). A write that timed out may still land. It is then a chunk no block names, which
the volume's reconciliation against the Block layer's reverse rows takes once its deadline has
passed (metadata.md §6; to be built, §7). A retry of the same chunk with the same bytes is
answered as done, and a retry with different bytes is refused (chunk-store.md §4, audits B05,
S05), so sending a chunk again never changes what an earlier write stored.

A chunk read asks for exactly the bytes the driver names, which the volume verifies before it
returns them; `Corrupt`, an error or no answer by the deadline is `Unreadable`, and the driver
reads another copy or decodes the block (gateway.md §3). A volume that returned `Corrupt` also
reports the chunk for repair (chunk-store.md §7).

### 5.3 Sessions

Each gateway holds one session with each range it writes to, registered by a `Register` command
at first use (replica.md §1). Commands carry the session, a serial, and the lowest serial whose
answer the gateway has not yet received, so the range forgets answers only once the gateway has
them. A gateway keeps at most as many commands in flight per session as the range's rules let a
session keep answers (`Rules::max_answers`, `max_answer_bytes`); beyond that its commands wait at
the gateway, bounded like any queue (§2.6).

Three answers need the gateway's handling, and replica.md §1 fixes each:

- **`SessionExpired`**: the command's outcome is unknown. The gateway registers again and sends
  the same command unchanged, carrying the same file or the same ID, and takes its answer; the
  range recognises a copy by its file and answers as it answered the first (replica.md §1). An
  `Expired` answer to the copy means the first was refused and its file released, and the driver
  starts a new attempt with a new file.
- **`SessionsFull { until_ns }`**: the range holds as many live sessions as it may. The gateway
  waits until then, or until its request's deadline, whichever is sooner, and answers
  `503 SlowDown` if the deadline comes first. The range's bound is the gateway incarnations that
  may hold a live session: each gateway's current one, and the one before it for a gateway that
  restarted within a session lifetime, whose old session counts until it expires. The cell's
  membership states both (§2.6).
- **`Repeated`**: an answer the gateway already acknowledged; it has the answer.

A session unused for its lifetime expires by the log's time (replica.md §1). The lifetime is how
long gateways go between commands to a range, measured once gateways run (replica.md §7).

### 5.4 Deadlines and retries

A request's deadline is its client's patience, which the server cannot know; it is bounded by
the admitted queueing delay and the progress rules of §3.8. Within it, each internal step has a
progress deadline and a retry budget: a command is resent in the same session with the same
serial, to the leader the range names, until the budget is spent. Every retry keeps the
operation's identity: the same session and serial, the same file, the same chunk key and bytes
(audit §13.5, §16.4). A retry never draws a new identity for a mutation whose outcome is
uncertain; only a refusal known to have had no effect starts a new attempt.

Chunk writes are not hedged by default; Tectonic's reservation-hedged writes to the first `n` of
`n + Δ` targets are a measured choice for later (04 R4; gateway.md §4).

### 5.5 The laptop's path

On a laptop every request's destination is the node itself. The routing layer delivers it
through a loopback peer that skips the socket, TLS and serialization, and keeps the rest: the
reservations charged by the encoded length the frame would have, the class, the range's
fencing and the sessions. A command reaches the local shard's queue for its range; a chunk write
reaches the local volume's ticket. The drivers, the sessions and the ranges run the
same code on a laptop as in a cell.

## 6. Metadata ranges on the node

### 6.1 Engines

The production engine is mantle's Rust port of RocksDB 11.8.1 (research 24), one instance per
range replica with its WAL off, as ZippyDB and TiKV run one instance per shard (12 §6.1). The
port's file system is `mantle-disk` (24 §2.2), so its flushes are the platform's full flush and
its files can live on the simulated device (§9.1). Every instance on a node shares one write
buffer manager, one block cache, one rate limiter per device, the flush and compaction pools,
and one SST file manager, each a node object from engine phase P14; where RocksDB would stall a
writer, the port refuses with a typed error (24 §4.7), which the range answers as `Busy`. The
rate limiter's rate is the device authority's share for engines (§2.5).

The cost of one idle instance decides how many ranges a node can host, and if that cost times
the ranges needed passes the memory budget, the fallback is one instance per device with a
column family per range (12 §6.1). That cost is measured once P7 runs.

The `Engine` trait (`mantle_meta::engine`) gains what metadata.md §4 already names: a checkpoint
of files at an index, and export and ingest of files, for snapshots, splits and moves (§6.3,
§6.4). `Engine::image` and `install`, which carry every row in memory, are retired with them
(audit §5.4).

### 6.2 Applied index, flush and truncation

The ordering audit §5.3 asks to be proven, stated as the node runs it:

1. An entry is durable in the log before a follower acknowledges it or a leader counts it toward
   commitment (replica.md §3; raft-log.md §6).
2. A committed entry is applied as one engine batch carrying its index (replica.md §2); the
   batch is in memory, not yet durable.
3. The engine flushes on its own schedule, and on the write buffer manager's demand. After a
   flush, `Engine::durable` names exactly the state on disk, read at the persisted tier (12
   §6.2).
4. The replica compacts its log group to `min(durable, the oldest snapshot being sent) − the
   lagging followers' window` (§2.6). The log never loses an entry the engine could not
   rebuild. `Replica::compact` today makes the engine durable itself before it writes the new
   start, which on a shard would wait on a flush; the node splits it, so the flush runs in the
   engine's pool and the range writes its start once `durable` has moved.
5. A snapshot is taken only at an index the engine has flushed and checkpointed (§6.3), and the
   log is not truncated past a snapshot in flight (12 §6.3).
6. On restart the engine opens at its durable index and the replica replays the log past it; an
   engine durable past the log's commit writes its index to the log as the commit (replica.md
   §4).

An engine that fails a write or flush fences its replica, which recovers from its files and the
log or from its peers; a command the state machine refuses is an answer in the log, never an
engine failure (audit §5.3). Flushes are also forced when the log bytes past `durable` exceed
what the node can replay within its restart budget (12 §6.2), and a device near full refuses new
proposals with `Busy` before the engine's compaction or a snapshot can run out of space: the
device authority reserves the space a compaction or an install needs before either starts.

This ordering is checked on the model engine in simulation today. It is proven for the
production engine by real-process kill and corruption tests on real file systems, including a
full disk during compaction and during a snapshot (audit §5.3; §9.2).

### 6.3 Snapshots over QUIC

A snapshot is the engine's checkpoint at an applied index: its files, hard-linked, listed with
each file's size and CRC-32C, with the index, term, configuration and descriptor. That list is
the Raft snapshot's payload, kilobytes whatever the range's size (12 §6.3). Today's snapshot
carries every row inside the message and copies it per recipient (replica.md §4; audit §5.4,
§12.2); this replaces it.

The files travel on the bulk class, one stream each, in slices each with its own CRC-32C,
fenced by the range, the term, the descriptor generation and the snapshot's index, so a transfer
for a term or generation that has passed is cancelled at both ends. A transfer resumes from its
last verified slice. The receiver writes into `meta/staging/<replica>/<snapshot>/`, reserved
against the device's space first, verifies every file, flushes the files and the directory,
renames the directory into place and flushes its parent (CLAUDE.md §6), then installs it and
reports the snapshot's fate to the sender, which stops replicating to a member while its
snapshot is out (replica.md §3). A mismatch is a typed corruption error that feeds repair (12
§6.3).

The sender holds the checkpoint's links and its log from the snapshot's index until the
transfer ends or is cancelled. Snapshots in flight are bounded per node and per device by the
bulk budget, and learner catch-up shares repair's bandwidth (audit §5.4, §5.7).

### 6.4 Splits, merges and moves

The state machine of splits and merges is built and simulated (metadata.md §3); the replica's
side is not. A split is one command in the parent's log. At its index each replica checkpoints
the parent's engine into the child's directory, removes the child's span from the parent and the
parent's span from the child, and opens the child as a new group on the same log, with the
parent's members and a log starting after the split's index (12 §6.4). A checkpoint needs a
flush, so the split's apply is finished in the engine's pool, and the range waits on its ticket
as it waits on any durable step. No snapshot is needed,
since both halves stay on the parent's replicas until the directory records the split, and only
then may either move (architecture §6). A merge aligns the replica sets first, freezes the higher
range, and proposes its decision once every replica of the frozen range has applied the freeze
(STATUS). A move is a replacement that keeps the old member: add a learner, send a snapshot,
catch up, and swap by one joint change (replica.md §6).

Which ranges split, merge or move comes from measured size and load, splitting for size or
sustained heat and never for one key or a sequential pattern (architecture §6, §11). Moves are
bounded per node, per device and per cell by the bulk budget, and a learner's lag in bytes is
bounded; a member is removed only once its replacement holds the durable state the range needs
(audit §5.7).

### 6.5 Voters across failure domains

A range's `2f + 1` voters are placed one to a failure domain at the level the configuration
names (architecture §11). A replica's domain includes its node and its metadata device, and a
device holds one shared log whose failure fences every replica on it (raft-log.md §1), so no two
voters of a range share a node, and above the node level none shares a rack or zone. Three
voters in one rack do not tolerate a rack's loss however the chunks are placed (audit §5.7). A
membership change is checked against the root range's domain table before it is proposed, and a
change that would break the rule is refused. Replica sets overlap as little as the placement can
arrange: with `q`-of-`k` quorums, sets that share at most `k − q` nodes keep every other set
above quorum when one fails (architecture §8; 09 §8.4.1).

Metadata availability and data durability are separate promises (audit §5.7): a range available
with a quorum does not make its blocks durable, and a block's redundancy does not make its
range available.

## 7. Placement, repair and cells

| Piece | Exists | To build |
|---|---|---|
| A block's scheme from the domains available and the durability target | `mantle-ec` durability model and choice (durability.md) | choosing per block when sealed, from the cell's live domain count |
| Placement of a block's chunks | the Block range refuses two chunks on one volume (metadata.md §1) | the cell's volume table in the root range, published to every node in full on a loop (architecture §4); gateways choosing volumes by two random choices over copysets that satisfy the domain rule (04 R2); the Block range refusing chunks that share a domain |
| Repair | chunk reads verify and report damage; the scrubber lists damaged chunks (chunk-store.md §7, §9); the Block layer's reverse rows list a volume's blocks (metadata.md §1) | a repair service per cell ordered by remaining margin, with delays by failure class and budgets per disk, NIC and rack (04 R3); installing new placements by generation and retiring old chunks only after readers are safe (audit §8.3); a fence between repair and reclamation (audit §8.4) |
| Reconciling chunks no block names | the sweeps settle files and blocks (metadata.md §2) | each volume's chunks checked against the reverse rows, taking those past their deadline (metadata.md §6) |
| Rebalancing and draining | the replica's replacement (replica.md §6) | moves under a byte budget, ranked by imbalance removed per byte; drains through the repair path, behind an operation gate that keeps every range's quorum and every block's margin (architecture §7) |
| Device health | identification and calibration; the scrubber's at-risk mode (chunk-store.md §9) | peer-relative latency and error counters deciding placement and drains (10 §1) |
| The cell map and routing between cells | the design (architecture §4–§6.1) | the map in the root range, the thin router, redirects with the map's epoch |
| Moving a key range between cells | the protocol (architecture §6.1) | a TLA+ model first, then the mover with its fences and rate limits |

## 8. Observability and operating limits

A node exports what it measures, what it refused and why, alongside what succeeded (audit
§8.8):

| Layer | Exported |
|---|---|
| Devices | the profile and its version, calibration, read admission and refusals, flush-time distributions, scrub findings, a fenced volume or log, the cleaner's runway |
| Admission | per authority: reserved, queued, deferred, executing and response bytes and counts; refusals by class and cause; sojourn times |
| Shards | runnable ranges, scheduling delay, bytes of work per turn, held and dropped messages, stalled ranges |
| Ranges | leader changes, apply, flush and ReadIndex latency, entries and bytes retained, snapshots sent and received with their lag, sessions held and refused |
| Transport | per peer: round trip, delivery rate, windows, bytes in flight per class, datagrams dropped by cause, reconnects |
| Gateway | requests by operation and answer, first-byte and total latency, block windows, retries, unknown outcomes |
| Redundancy | blocks by remaining margin, repair debt and its rate, reclamation and collection backlogs and their age (audit §8.6) |
| Epochs | range generations, map epochs, stale routes refused |

Latencies are kept as log-bucketed histograms, whose accuracy follows from the estimator
(11 §14). `mantle status` prints the node's operating envelope: each derived limit, the value it
took, and the input that bound it (audit §12.6), with the accounting audit §15.1 asks for
evaluated on measured values, among them the node's memory as the sum of its owners and the
heartbeat rate its led ranges cost.

## 9. Testing

### 9.1 The node under simulation

FoundationDB built its deterministic simulation before its database and puts "network, disk,
time and PRNG" behind interfaces a single-threaded simulator replaces (06 §A5). The node is
written the same way. The shard loop, the admission authorities, the routing layer with its
sessions, the transport's framing, classes and credits, and SWIM are state machines that take
events and name their effects; tokio, the sockets and the threads are thin shims around them.
The simulation runs many nodes in one thread on a virtual clock: volumes and logs on the
simulated device that loses, keeps or tears unflushed writes (`mantle_disk::sim`), engines on
the model engine and, once the port runs over `mantle-disk`, on the port itself, which
FoundationDB could not do with a storage engine outside its simulator (06 §A5). The network is
focal-sim's fabric of paths, links, loss, MTU black holes and NAT rebinding (07 §5.2), and QUIC
runs as quinn-proto's sans-I/O endpoints over it, as focal's congestion test drives them (07
§5.2).

This is the many-group host audit §15.2 asks for, which the replica simulation of three members
and two keys is not. It drives thousands of ranges per node on shared logs with the real
scheduler, injects failed, delayed and ambiguous flushes, lost completions, stopped workers,
credit starvation, stale callbacks, cancelled clients, corrupt snapshots, slow receivers,
partitions, clock steps and one abusive tenant, and after every event checks: every authority
within its budget; every admitted request with one disposition, committed, refused, or unknown
and later resolved; linearizability of each key's history by the WGL checker (06 §A6.8); no
acknowledged write lost; a follower's acknowledgement never before its write; and, once faults
stop, every operation completes and every reservation returns. Each run is its seed. Counters
show that each fast path ran: leader messages sent during flushes, frames
shared by several ranges, datagrams packed, snapshots sent in slices, held messages. Each rule
removed on purpose fails a run (audit §15.2).

### 9.2 Real processes

End to end first (CLAUDE.md §8): `mantle serve` processes on real disks and sockets, driven by
real S3 clients. The AWS CLI, boto3 and the AWS SDK for Rust run their suites against it, and
ceph's s3-tests runs every supported feature (STATUS, planned 1). Processes are killed with
`kill -9` during PUTs, completions, splits and snapshots; disks fill; clocks step; and every
acknowledged object reads back whole. Linearizability is checked on histories recorded from
real processes under partitions and crashes, on the production engine (STATUS). The network
matrix of audit §13.6 runs on Linux under the container of `scripts/linux-test.sh`, with rates,
delays, loss and outages imposed on its links, and records durable progress, tails, memory and
refusals for each cell of the matrix. Every hot path of the node has a benchmark with a recorded
baseline, and a performance claim names its run.

### 9.3 Evidence per stage

| Stage (audit §17, §8.8) | Evidence |
|---|---|
| Laptop serving | the supported S3 profile passes the client suites and s3-tests; durable across `kill -9` and a full disk; idle and busy memory, threads and restart time measured and within their stated bounds; the node reports that it survives no device failure |
| One device, many ranges | the many-group simulation passes; on real hardware, shared log, chunk, engine, cleaning and scrub load with aggregate queue depth and live bytes within budget, and a stalled device leaving healthy devices serving (audit §15.3) |
| Regional cell | 3- and 5-voter histories with process crashes, partitions and disk faults accepted by the checker in simulation and with real processes; node, rack and zone losses near capacity with every acknowledged write read back; snapshot and catch-up storms; the network matrix |
| Multi-cell region and fleet | root map recovery, stale routes fenced, a range moved between cells under load with no lost write and no stale read, a cell's failure leaving the others' requests unaffected; measured map, placement and repair limits |

## 10. Build order

Each phase ends when its criterion holds on all six CI targets, and names the engine phases of
research 24 §3.1 it needs.

| Phase | Builds | Engine | Done when |
|---|---|---|---|
| A. The node inside | tickets with wakers for the log and volumes; per-device readers; shards with the staged turn, deficit round robin and timers; the admission authorities and budgets; directories, `LOCK` and profiles; startup, stop and rollback; the loopback peer; the many-group simulation host | the model engine | the many-group simulation passes its invariants at thousands of ranges per device with every mutation caught; real processes driving tickets and shards over real logs and volumes, killed at random, lose no acknowledged update or chunk |
| B. A laptop that serves S3 (audit §17, third) | the native QUIC protocol and the HTTP/1.1 S3 listener; the request path, bodies, errors and overload of §4; the drivers served in-process (§5); sessions; placement from local volumes; the key authority of §4.6; chunk reconciliation per volume | P1–P10 and P14 (the port opening, writing, flushing, recovering, reading and compacting, with shared budgets); P15 for baselines | the laptop row of §9.3 |
| C. A cell of several nodes (audit §17, fourth, first half) | QUIC transport with its classes, credits and defenses; the datagram plane; SWIM; invitations and joining; remote chunks; snapshots over QUIC; voters across domains; the replica side of splits, merges and moves | P12 (range deletions) and P13 (checkpoint, export, ingest) | 3- and 5-voter histories accepted under crashes, partitions and disk faults, in simulation and with real processes; a node lost for good replaced with no acknowledged write lost |
| D. A regional cell (audit §17, fourth) | repair ordered by margin; rebalancing and drains behind the operation gate; device health in placement; adaptive device plans; the network matrix; the multipart and GET pipeline for massive objects (audit §16); the fast track as an experiment with its own proof (audit §5.6) | P9's cache measured in both regimes (23 §0 item 6) | the regional row of §9.3, at the largest cell the deployment declares, within its stated latency, resource and rebuild limits |
| E. Cells and the fleet (audit §17, fifth) | the cell map in the root range, the router, the mover between cells after its TLA+ model; key authority across cells; versioned upgrades and rollback (audit §8.7); the chosen geographic contract (audit §8.1) | none new | the fleet row of §9.3: growth, cell retirement, stale routes and region failover keep ownership, with measured recovery objectives |

Universal and FIFO compaction (P11) are not needed unless the cost model chooses them for a
range's workload (12 §6.6).

## 11. Open

- **Cores among pools.** Three pools sized to the granted cores oversubscribe under full load;
  whether dividing cores by measured demand does better is measured once phase A runs.
- **Quiescing idle ranges**, and the protocol that wakes one without weakening its election or
  read rules (audit §15.1; 06 §A4.3).
- **More than one `Ready` of a range in flight** (replica.md §7), which needs the core's help.
- **The datagram plane against RFC 9221's datagrams on the QUIC connection.** The separate
  socket escapes bulk's congestion control and carries RFC 8085's obligations itself (§3.4);
  the network matrix decides whether the separation pays for itself.
- **The congestion controller** for QUIC: quinn's default until the matrix of audit §13.6 has
  run mantle's own mix; Copa's results in slates and focal are evidence for their workloads
  (audit §13.2).
- **The per-stream window**, fixed per connection in quinn (§3.3): whether a connection per
  class serves bulk better than one connection with a fixed stream window.
- **The cell authority's private key** once a cell has many nodes: which nodes hold it, or an
  external issuer.
- **The idle engine instance's cost**, which sets the ranges a node can host (12 §6.1).
- **Follower reads and leases**, kept out until a deployment states its clock bound (replica.md
  §7; audit §5.5).
- **The fast track**: disabled until its application and persistence proof and its crossover
  measurements (audit §5.6, §11.5).
- **Threads per volume.** Each volume starts a writer, a cleaner and a scrubber, 300 threads
  at 100 volumes (audit §14.1); one issuer per physical device funding its volumes from the
  device's authority is the alternative to measure.
- **SWIM's indirect probers and suspicion timeout** for a cell's size, from SWIM's analysis of
  detection probability against load (06 §A8.2).
- **The geographic contract** (audit §8.1): which model of cross-region writes and failover
  mantle offers.
- **`x-amz-retry-after`** on `SlowDown` (25 §3).
