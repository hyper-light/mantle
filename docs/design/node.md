# The node: one process from a laptop to a fleet

Status: design, 2026-09-30. Sources: docs/research/25 (the node's runtime, cited as "25 §x"),
26 (the concurrency model), 27 (upload scheduling), 28 (storage classes, power and longevity),
29 (device classes), 30 (resilient transfer), 31 (caching, ordering and integrity), 07 (focal's
consensus stack), 08 (slates' runtime and transport), 09 (cells and S3's
internals), 11 (operating-parameter models), 06 (consensus), 12, 23 and 24 (the engine), 04
(placement and repair), 10 (device health); the audit of 2026-09-29 (cited as "audit §x"); and
the records this one joins: architecture.md, chunk-store.md, raft-log.md, replica.md,
metadata.md, gateway.md, s3-protocol.md, durability.md, encryption.md, measurement.md and
constants.md.

`mantle serve` is the process that runs mantle. The pieces it runs exist as libraries: chunk
volumes that group-commit and verify every read (`mantle-chunk`), a Raft log shared by every
range on a device (`hyper-log`, vendored from hyper-raft), range replicas that run hyper-raft's
core over that log and an
engine (`mantle-range`), the Name, File, Block and Bucket layers with their sessions, sweeps,
reclaimer, collector and coordinator (`mantle-meta`), the gateway's PUT, GET and completion
drivers (`mantle-gateway`), the S3 protocol (`mantle-s3`), device identification and
calibration (`mantle-disk`), aligned direct I/O, the full flush and the device issuer
(`hyper-block`, vendored from hyper-raft), and erasure coding (`mantle-ec`). Each is either sans-I/O, naming
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

**Decision: no thread per unit of concurrency.** A client, a connection, a request, a range, a
replica, a volume and an in-flight I/O are records or tasks, never threads; the process's
threads are fixed by its cores and its devices, whatever the client count (26 recommendation 1).
Threads per unit failed concretely: a benchmark that ran a thread per logical replica, all
waiting on one condition variable, held a macOS kernel spinlock for O(waiters) work per
broadcast until the kernel panicked (26 §1.3–§1.4), and SEDA measured throughput collapsing as
threads grow (26 §4.2).

| Owner | Threads | What runs there |
|---|---|---|
| Network runtime | a tokio multi-thread runtime, one worker per core the process is granted | QUIC connections and the datagram plane (§3), SWIM, HTTP connections (§4), the drivers of each S3 request, the routing layer (§5), clients and connections as tasks |
| Metadata shards | one per granted core | range replicas: stepping, ticking, proposing, driving, applying, serving confirmed reads (§2) |
| Coding pool | one per granted core | erasure encoding and decoding of whole blocks, block checksums, the write path's inverse checks (§5.6) |
| Device issuers | one per physical device | every volume's writer, cleaner and scrubber and the device's Raft log writer, as state machines (chunk-store.md §4; raft-log.md §3); the device's dispatcher (§2.7). As built: every volume's writes and flushes, dispatched to the device's pool; the writers, cleaners and scrubbers are still threads of their own (STATUS item 4) |
| Device pools | per device, at most the device's measured depth, only where the platform has no asynchronous interface the device can use (below) | blocking reads, writes and flushes the issuer hands them. As built: the volumes' writes and flushes, on every platform (below) |
| Engine background | flush and compaction pools shared by every engine instance on the node (24 §4.7, engine P14), whose file I/O goes through the device's dispatcher | the engine's own background work |

With `C` granted cores, `D` physical devices and pools of `p_i` workers, the process's
long-lived threads are `3C + D + Σ p_i` plus the engine's pools, whatever the number of
clients, ranges or volumes (26 §4.7, DERIVED). They were `3C + Σ d_i + L + 3V` for `L` log
devices and `V` volumes, three threads per volume, 300 at 100 volumes (audit §14.1); the issuer
per physical device replaces them, which closes the question this record left open.

**Device I/O in flight, by platform, at every step.** The issuer keeps its device's depth in
flight through the platform's asynchronous interface where one exists: io_uring on Linux, one
ring per device with registered buffers and files, durable writes linked to an fsync with
`IORING_FSYNC_DATASYNC`, the kernel's own workers bounded at `min(entries, 4 × CPUs)`; overlapped
I/O on one completion port on Windows, with `FILE_FLAG_NO_BUFFERING` so the I/O stays
asynchronous (26 §2.2–§2.3). io_uring drove "about 1.2M IOPS" without polling, twice Linux AIO's
on the same workload (26 §2.2). macOS has no such interface for a full flush or more than 16
requests (26 §2.4), and Linux may have io_uring disabled (`ENOSYS`, `EPERM`; research/02 §2.13);
there the device's pool of blocking workers carries the depth, sized at the smaller of the
device's reported queue and its measured knee. Research note 26 proposed the native interfaces
only from the node step, keeping the laptop on the pool everywhere; the design takes them at
every step on the platforms that have them, because a backend chosen by platform keeps one code
path per platform from laptop to fleet, where one chosen by scale would run the laptop on a path
the fleet does not, and because the native path is the more efficient at any size. SQPOLL and
its polling core are used only where the node's measured CPU per I/O shows a core's worth of
saving (26 §7).

*As built (2026-10-01).* `hyper_block::issuer` is the pool path, and it runs on every
platform: io_uring on Linux and the completion port on Windows are not yet built, so Linux and
Windows keep their depth with blocked workers as macOS does. The issuer's thread and its
`min(device queue, measured depth, budget left)` workers start when the device opens and draw
on the process budget before any starts; a device calibration has not measured runs one worker
(`issuer::depth`), as its reads go one at a time (chunk-store.md §7).

**A process thread budget.** Every pool draws from one budget, decided before any thread
starts, so devices times depth cannot add up past it. On macOS its ceiling is the OS's own
statement of what one process may run, `kern.wq_max_threads` (512 on the development machine),
the cap Apple's workqueue under GCD and Swift concurrency applies to itself (26 §1.5, §2.5);
elsewhere the pools exist only where io_uring is unusable, under the same budget, whose ceiling
on Linux is the smaller of `kernel.threads-max` and the soft `RLIMIT_NPROC`, and on Windows the
500 worker threads Microsoft states as a thread pool's default maximum ("Thread Pools", Best
Practices) (`hyper_block::threads`). A pool that
would pass it is refused, naming the device and depth, before any thread exists (26
recommendation 4); the device then runs at the depth the budget leaves.

**The coding pool's cores.** Coding-pool workers are pinned one to a physical core where the OS
allows it (`sched_setaffinity` on Linux, `SetThreadAffinityMask` on Windows), so the write path
can run a transform's inverse on a different core from the transform (§5.6). macOS offers no
binding affinity, so there the inverse runs on another worker, on whichever core the scheduler
gives it, and a fault tied to one core is caught only when the two land apart.

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

**Tickets for writes.** The log answers every call through a ticket (`hyper_log::Pending`,
`hyper_log::Fetching`): the answer travels a reply port only its caller receives on, so it
unparks that caller and no other, and a submission or fetch may carry a `std::task::Waker`,
which the log's owner thread wakes once, after the answer (hyper-log `ORIGIN.md`, L-2). A chunk volume gets the same: today `Volume::put` returns only once
the chunk is durable, holding its caller's thread for a flush; it gains a submission that
returns a ticket at once. A tokio task awaits a ticket with its own waker. A shard's waker
pushes the range onto the shard's ready queue and unparks the thread, so a completion wakes the
one range it belongs to and no loop scans every range (audit §11.3). `std::task::Waker` is in
the standard library, so neither crate takes a dependency on a runtime. A ticket carries the
generation of what submitted it, the range replica's incarnation or the request's, and an
answer that comes back after its owner was replaced is dropped (audit §11.3). Dropping a ticket
gives up the caller's interest only: a write the queue admitted is written, and its outcome is
what recovery finds; it is never reported as a failure that had no effect (audit §11.3).

**Reads through the device's issuer.** An async caller hands its read and its reservation to the
device's dispatcher and awaits a ticket. The dispatcher holds the device at the depth where
calibration found throughput stops growing (chunk-store.md §7), since a read past it only waits,
and issues through the device's asynchronous interface or its pool (§1.2). A device that
calibration has not measured reads one at a time (chunk-store.md §7).

**A wake reaches only what it admits.** Every place where many wait for one thing wakes them one
to one (26 §5.3): a queue with room hands it to its waiters in their order, start-tag order
where the queue is fair (§2.7), and wakes exactly those it admitted, each through its own slot,
a thread's `park`/`unpark` or a task's `Waker`, as the chunk store's read gate already does; its
waiting list is bounded, and past it the answer is `Busy`. An event that concerns every waiter,
a fence or a shutdown, completes each waiter's slot with the answer and wakes each once, a cost
bounded by the list's bound and paid once per event, never once per answer. A `Condvar` is used
only where its waiters are a few known threads, a writer and its owner, with `notify_one` where
one can proceed; `notify_all`, `Barrier` and a start latch are never used where the waiters are a
pool, a client population or anything sized by measurement. The log's `room`, which broadcast to
every waiting replica on each answered submission, and the measurement workers' latch are
replaced on these terms (26 recommendation 2). A burst of `B` arrivals then costs `B` queue
entries and at most one wake per admission; the herd cannot form because no object has more
waiters than its queue's bound (26 §6).

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
A stop that runs past its requests' deadlines stops the rest as a crash would. A stop for low
battery (§1.8) is an orderly stop that also ends each open native upload at a checkpoint
(gateway.md §2.1), so what power's return replays is the drained state and not a run.

**Versions.** Every on-disk format already names its version (a volume's superblock, a log
segment's header, an entry's format byte in `mantle_meta::wire`), and a connection's first
exchange names the versions each side speaks, as focal's does (07 §4.1). A binary refuses a
format it does not know rather than guess. A new entry or record format is written only once
every member of a range has said it can read it, the pattern of focal's decoder fences (07
§2.5), so a node can be rolled back to its previous binary until that point, and a
mixed-version cell runs in the meantime (audit §8.7).

### 1.8 Power, thermal state and background work

A node on a laptop runs on battery, sleeps and heats; a node in a rack does not, and the same
code reads the same inputs and finds them steady.

**The inputs are read from the OS, by notification, and normalized** (research/28 §4.1, D13):
the power source {mains, battery, UPS, unknown}, the saver mode {off, standard, high} and the
thermal state {nominal, fair, serious, critical}.

| Input | macOS | Linux | Windows |
|---|---|---|---|
| Source | `IOPSGetProvidingPowerSourceType`; notify key `kIOPSNotifyPowerSource` | `/sys/class/power_supply/*/{type,online,status}`, uevents | `GetSystemPowerStatus`; `GUID_ACDC_POWER_SOURCE` |
| Saver | `NSProcessInfo.lowPowerModeEnabled` and its notification | `/sys/firmware/acpi/platform_profile` | `GUID_POWER_SAVING_STATUS`, `GUID_ENERGY_SAVER_STATUS` |
| Thermal | `NSProcessInfo.thermalState` | thermal zones' temperatures and trip points | none public found; the NVMe drive's own temperature (research/10 §6.6) |

Each is read once at start and then on its notification; where none exists (thermal zones, the
drive's temperature) it is read at the cadence its source updates, no faster than once a minute
for NVMe's minute-resolution fields (research/10 §9.2), since a poll of the drive is itself an
admin command that wakes it.

**Acknowledgement never changes with power.** A write is answered when it is durable, on battery
as on mains. What changes is how writes are batched (chunk-store.md §4: the writer waits up to
the batch service time `S` on battery, which at most doubles a lone request's latency and loses
no throughput), what background work runs, and when:

| Work | Its deadline, from | On battery or in saver mode |
|---|---|---|
| Repair | the durability model's urgency (research/04 §R3) | never deferred at a margin of one chunk or less; repair with margin to spare waits until its computed deadline |
| Cleaning | the free-space runway (chunk-store.md §8) | what the runway requires runs; the rest waits |
| Scrub | the period's upper bound (chunk-store.md §9.1) | mains time first; on battery only to meet the bound, in bursts |
| Engine compaction | the engine's write-stall triggers (research/12, 23) | what a stall would force runs; the rest waits |
| Lifecycle and tier moves | the source pool's capacity runway (storage-classes.md §7) | waits |
| Restores | the tier's documented completion time (storage-classes.md §6) | Expedited runs; Standard and Bulk when their objective requires |
| Calibration, benchmarks | the operator | refused unless forced (measurement.md §9) |

Background threads run at the platform's background priority (`IOPOL_THROTTLE` on macOS,
`IOPRIO_CLASS_IDLE` on Linux, a low I/O priority hint and EcoQoS on Windows), always beneath
mantle's own pacing, which holds whether or not the OS honours the hint (research/28 §4.5).

**Thermal state takes the platform's own stated response** (research/28 §4.5, D15). Apple
states one for each state, and the same mapping serves Linux's trip types and the drive's
warning and critical temperatures (research/10 R10):

| State | The platform's words | mantle |
|---|---|---|
| Fair; a passive trip point approached | "Defer non-user-visible activity" | background work as on battery |
| Serious; drive at or above WCTEMP | "reduce application's usage of CPU, GPU and I/O, if possible"; "Immediate remediation is recommended" | also: every writer, Express included, on the battery rule; the coding pool limited to what admitted foreground work needs; the device marked `Throttled` |
| Critical; drive at or above CCTEMP | "the minimum level needed to respond to user actions" | admission narrowed to what keeps durability and answers requests in flight; new work refused with S3's retryable 503; repair at no margin continues |

**mantle never programs device power states** (research/28 §4.2, D16). The OS owns APST and its
timers, setting them needs privilege, and they affect every user of the device. mantle shapes
its I/O so the device can sleep: batches on battery, background work coalesced into bursts at
the device's knee rather than trickled (research/29 §7.5), and no polling faster than the data
needs. Linux's defaults put an idle NVMe drive into a non-operational state after 100 ms, so a
writer that flushed lone requests a few hundred milliseconds apart would wake it each time
(research/28 §1, finding 6). mantle measures what waking costs (measurement.md §9, C5). An
operator who wants Express traffic to keep the drive awake may set the device's PM QoS latency
tolerance; mantle does not by default.

**Sleep.** A node that sleeps wakes with its ranges' time far ahead: sessions have expired,
handover deadlines of in-flight writes have passed and their blocks are swept; committed data is
untouched, and a one-voter range elects itself (research/30 §5.4). Every timer the node keeps is
judged against the range's agreed time, never a local clock that may have stopped while it
slept.

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

A range's replica is hyper-raft's durable shell (replica.md §3; hyper-raft `docs/durable.md`),
whose one entry point for work, `drive`, never waits: it takes the log's answers to the
replica's writes, applies what the commit fence allows, at most one page of committed entries
(`max_committed_size_per_ready`) or one entry larger than it, and takes at most one `Ready`,
giving out at once the messages that answer for no write and submitting its write with the
shard's waker. Readies are taken ahead of their writes' answers to the log's depth, three
frames, so a range's next update need not wait for its last one's flush (audit §5.1). A range's
turn does this, in order:

1. Take the messages that arrived, with `step`, and the ticks that came due, with `tick`. The
   replica takes both while its writes are out; one waiting for room refuses messages with
   `Stalled`, which the shard counts and drops, as the network may, and is not ticked
   (replica.md §3).
2. Propose the commands waiting for the range as one entry of at most `max_entry_bytes`
   (replica.md §1), and start the reads waiting, with `read_index`.
3. Call `drive`. Hand every message it gives to the transport at once: a leader's appends go
   out while its own write flushes, and a follower's acknowledgement and every vote are given
   out only once the write that holds what they say is durable, which the shell enforces.
   Answer the commands in `applied` to the gateways waiting for them, and serve the confirmed
   `reads`, which come once the engine has applied their index.
4. If `Driven::more` is set, the range has more to do without an answer from the log (a
   committed page past this drive's, or another `Ready` within the depth): it goes back on the
   ready queue for its next turn. Otherwise it waits: the log's answer to one of its writes
   wakes it through the shard's waker, which puts it back on the queue. If `Driven::stalled`
   is set, the log refused a write for want of room: the range waits on the device authority
   (§2.5), which asks the engines on that log to flush so the ranges can compact (§6.2), and
   calls `resume` when a compaction frees room. If the replica answers `Fenced`, its log
   failed to make a write durable and nothing the process holds says what the device kept;
   every replica on that log is taken off its shard, the log is reopened, which recovers what
   is durable, and its members open afresh from it.

A shard drives every runnable range in one pass before any of their flushes completes, so
their updates meet in one frame on each log, which is what audit §5.1 asks a scheduler to do.
The shard holds a range's waiting commands and reads in the range's own queue and proposes
them at its next turn. The queue is bounded in bytes by Little's law: the range's measured
command rate times the delay the queue may add, and at least one largest entry, so the largest
command a range accepts always fits (11 §4.4–§4.5). Past the bound a command is refused
`Busy`, which the gateway answers with `503 SlowDown` or retries within its deadline (§5.4).
A leader whose uncommitted proposals reach `max_uncommitted_size` refuses more, `Refused`,
and the shard keeps them queued.

### 2.3 Fairness, and the reserve for control

Within a shard, ranges with data work take turns by deficit round robin (25 §7). Each round a
range's deficit grows by a quantum and its turn spends it in bytes of work: the encoded update
it submits and the committed entries it applies. Deficit round robin keeps each backlogged
range within one largest unit of its fair share over any number of rounds, and does O(1) work a
turn when the quantum is at least that unit: "The Work for Deficit Round Robin is O(1), if for
all i, Quantum_i ≥ Max" (25 §7). The quantum is therefore the largest unit a turn can carry: one
`max_entry_bytes` entry proposed plus one drive's page of `max_committed_size_per_ready` applied,
which the shell holds a drive to (hyper-raft `docs/durable.md` §2.2). This replaces the
replica's count of 64 `Ready`s a drive, which bounded neither bytes nor time (audit §12.6).

Control comes first in every pass: ticks, votes, heartbeats and their answers, and the drives
of ranges whose writes the log answered. Strict priority protects urgent work only when the
urgent work is itself bounded (audit §13.3), and this work is: what a range's core queues while
its writes are out is bounded by the core's own limits on pending messages and appends in
flight, and a campaign supersedes the vote requests of the one before (replica.md §3). A pass
spends at most one round of data work before it returns to control, and a round is bounded in
bytes by the quanta of the ranges in it.

The shard's fairness is among ranges, so one hot range cannot hold a core that other ranges'
leaders need for their heartbeats. Fairness among tenants within a range is the range's own
queue's: the commands waiting for a range are taken by start-time fair queueing over tenants,
then principals, with §2.7's bounded state, and charged in encoded bytes, so a swarm of one
tenant's tiny commands on a hot prefix waits behind its own share rather than ahead of another
tenant's (research/27 §7.3, D9). This record first left tenant fairness to the gateways alone,
the range taking commands in the order gateways admitted them; but a range's commands come from
many gateways, none of which sees the range's queue, and DynamoDB kept partition-level limits
beside its global admission "for defense-in-depth" for that reason (research/27 §7.3). A
range's answers carry its admission level, so gateways shed locally before sending what it
would refuse (research/27 D9, after DAGOR §4.2.4). A range splits at an observed key when its
measured sustained load exceeds its measured capacity and that load is spread over keys, never
for one key or a sequential run (architecture §6, §11); "sustained" is the split's own measured
duration times a margin derived from the split's measured cost.

### 2.4 Time

The Raft core counts ticks and reads no clock on ticks (07 §5.1; hyper-raft `docs/durable.md`
§8), which is how a range elects until its node carries the node-pair liveness stream (below). A
shard keeps a timer wheel of each range's next tick, as slates' runtime keeps timers, whose cost
is per event rather than per tick (08 §3.2), and ticks a range only when its tick is due.

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

**Elections by suspicion, once the node carries the stream.** hyper-raft's core elects by its
owner's failure detectors instead of ticks where its owner gives it their word (core step L-2),
and the detectors are one heartbeat stream for each pair of nodes that share a group, each
heartbeat proving a recent durable flush on its sender's log and each pair judged by an NFD-E
detector configured from what the pair measured (`hyper-liveness`, L-3; hyper-raft
`docs/timing.md` §2.8–§2.9). One stream a pair costs the same however many groups the pair
shares, where a heartbeat per group grows with them (hyper-raft `docs/benchmarks.md`,
"hyper-liveness"). A range moves onto it only once the node can carry the stream, which takes
seven things the node does not have (§10, phases A and C):

1. **An owner that holds its replicas.** The shards of §2.1, each holding its ranges' replicas
   by handle, driving them as §2.2 says, and fanning each node's suspicion, trust and restart
   out to the groups with a member on that node (hyper-durable's `Owner::believe`). Today a
   range's replica is driven by its tests alone; the `mantle` binary runs no node.
2. **The plane.** The sealed datagram plane of §3.4 on a socket that stamps each datagram's
   arrival in the kernel (hyper-tokio's `PlaneSocket`: Linux `SO_TIMESTAMPNS`, macOS
   `SO_TIMESTAMP_MONOTONIC`; Windows stamps at the read), under the stream's contract: every
   datagram stamped before a time is fed before the stream is polled at that time, the clock
   read before the socket (timing.md §2.8). The node has no transport yet (§3).
3. **Which node each member is on.** Placement's map of each range's members to nodes, so the
   node keeps one pair for each node it shares a group with, within placement's bound on pairs
   (`Settings::max_peers`), and charges each pair the election span of the groups it shares
   (`Liveness::set_election`) (§6.5, §7).
4. **The node's run** (`Settings::run`). A count raised at every start and kept as a record of
   the node, written whole before the run is used (a temporary name, the platform's full flush, a rename, the
   directory's flush, a CRC-32C checked on read: hyper-block's `record`), so a restarted
   node's heartbeats are numbered on from its last run's and its peers count one restart, not
   two (timing.md §2.8). The node keeps no records of its own yet (§1.5–§1.6); its random
   incarnation has no order and cannot be the run.
5. **Flush evidence.** Every durable completion on the node's metadata logs reported to the
   stream (`Liveness::on_durable`; a range's drive gives its latest as `Driven::flushed`), and
   a liveness write made on a log device when the stream asks for one because none came in
   time (`Output::flush`), one out at a time. A node whose ranges write makes none; an idle one
   makes one per shortest interval among its pairs.
6. **Timers and their lateness.** The stream's heartbeat deadlines and each core's
   (`deadline`, `wake`) on the shards' timer wheels, and each wait for the stream's wake
   reported with the time it ended (`Liveness::on_wait`), from which the stream measures the
   owner's lateness `G`, the owner's own stalls included (timing.md §2.9).
7. **The cell's history.** The restarts and abandoned suspicions of the cell's nodes, the
   detectors' prior evidence on the time between failures (`Settings::history`), kept with the
   cell's membership in the root range (§3.5, §7).

Until then a range elects on ticks with focal's counts, as above (replica.md §3), and the stream
is vendored only as the shell's dependency (vendor/UPSTREAM.md). SWIM's place (§3.5) does not
change with it: SWIM's word stays a hint for routing and repair, and the stream's is what a
group's elections take.

### 2.5 One admission authority per bottleneck

A per-volume queue cannot bound a device that several volumes, a log and many engines share
(audit §14.1). The node keeps one admission authority for each resource that can saturate, and
every piece of work reserves from each authority it will use before it allocates:

- **Memory**, one for the node: a tree of budgets in the form of focal-memory's `MemoryBudget`
  (07 §5.3), whose children are the classes of work (S3 requests, range replicas and their
  queues, engines, transport buffers, snapshots, coding, caches) and whose completion lane keeps
  back what admitted work needs to finish, so work admitted under pressure can still complete.
  The caches' share is what admitted work leaves, divided among the node's caches (the engine's
  block cache, the gateway's row and block caches, the chunk cache) by equal marginal saving on
  each cache's miss-ratio curve, measured online in bounded memory: each curve times the measured
  cost of one of its misses gives the saving per byte at each size, and memory moves to the
  cache whose next byte saves most until no move gains more than the curves' measured error
  (research/31 §3.5, D7). A cache whose curve is flat at its smallest size gets nothing.
- **Each physical device**, found through the storage graph so that two volumes, a log and the
  engines on one SSD share one authority: outstanding operations, bytes and estimated device
  time, with shares for foreground writes and reads, the log, engine flush and compaction, the
  cleaner, the scrubber, repair and snapshot reads. The engine's rate limiter for the device is
  this authority's share for it (24 §4.7).
- **Each peer path**: bytes in flight by class (§3.3).
- **The coding pool**: jobs and their work space, reserved from memory.
- **Each tenant and principal**: the fair queues of §2.7, at the gateway's admission and at
  every dispatcher, and the tenant's share across gateways (§4.5).

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
| A PUT's block window `w` | `w = ⌈R·T/B⌉`: the rate the upload is entitled to, `R = min(body rate, the principal's fair share over the authorities it uses)`, times the block chain's latency over a block's bytes, capped by memory; recomputed at each block | the body's arrival rate, the principal's share (§2.7), each chain's latency | gateway.md §2; audit §16.3; research/27 §6.1, D7 |
| A GET's block window | `w = ⌈R·T_q/B⌉`: the client's entitled read rate times the chain latency at the hedge quantile, so a block at the hedge point does not starve the client | the rate the client takes bytes, the chain latency distribution | gateway.md §3; research/31 §4.8 |
| A device's dispatch budget `D_b`, `D_n` and unit `u` | the smallest in-flight bytes and operations at which calibrated throughput stops growing; the smallest transfer that reaches the device's bandwidth | calibration | §2.7; research/27 D2 |
| A gateway's heavy-hitter table `m` | the smallest `m` whose error `N/m` over a window of `N` requests is below the smallest per-principal limit enforced, within the gateway's memory share | request counts per window | §2.7; research/27 D8 |
| The retry ratio a gateway affords | `ρ_max = C/λ − 1` for measured capacity `C` and first-attempt load `λ` | both | §4.5; research/27 §5.7 |
| A peer's receive window | `min(BDP, the peer's share of the transport budget)` | delivery rate and round trip per peer | §3.3; audit §13.4; 25 §4 |
| Entries a range keeps for lagging followers | keep entries while resending them costs less than a snapshot: retained bytes ≤ the range's engine size | both sizes | raft-log.md §4; 06 §A4.3 (CockroachDB chooses by the writes missed) |

Every row changes as its inputs are measured again. None is a constant in the code, and each
appears in constants.md as derived, with its model.

### 2.7 Scheduling: no upload dominates, every upload as fast as it can be

The owner's question: a very large multipart upload is running when a spike of small uploads
arrives; no upload may dominate or block, and each must be as fast as it can be (research/27).
The two kinds are bottlenecked on different resources: a small PUT on device operations and
metadata commands, a large part on bandwidth, coding and memory (research/27 §2.1). The
evidence gives one shape at every authority: the delay a small request sees is the device's
in-flight work plus one largest unit per competitor, over the device's rate, and the large
stream keeps full bandwidth when nobody else waits (research/27 §4.9). Size priority across
principals is refused: under overload SRPT's guarantee leaves out the largest jobs, Homa measured
"99th-percentile slowdowns of 100x or more" for its largest messages, and a principal can split
its work into small requests to gain priority, which agents tuned to the scheduler will do
(research/27 §5.6).

**Charged by dominant share, in measured units** (research/27 D1). Each authority prices a
request in its own unit from a calibrated model: the device's cost as a function of operation,
size and read/write mix, in the form ReFlex, Libra and IOFlow calibrate per device; metadata
cost as commands and encoded bytes; memory as bytes held; coding as CPU. A request's charge in
the fair queues is its largest cost over the authorities it reserves from, each over that
authority's measured capacity: Dominant Resource Fairness in DRFQ's memoryless form, so a
principal that floods the metadata path and one that floods the disks pay in one currency, and
no flow is owed or penalized for its share before the others arrived. DRF is strategy-proof: a
principal cannot raise its share by misstating its needs. A cost not known in advance (a LIST, a
completion) is priced by 2DFQ's decaying per-tenant, per-operation maximum and corrected when
it completes. Every coefficient comes from calibration of the running device, per device and
never per class of device (D11b), and is measured again as the device's state changes.

**One dispatcher per physical device, with a measured depth and unit** (research/27 D2). The
device's issuer (§1.2) dispatches in start-tag order, keeping at most `D_b` bytes and `D_n`
operations in flight, the smallest values at which calibrated throughput stops growing; beyond
them work waits in the dispatcher's queues, where order can still be chosen, which is IOFlow's
measured resolution of SFQ(D)'s trade between depth and fairness. A large write goes to the
device in units of at most `u`, the smallest transfer that reaches the device's bandwidth
(chunk-store.md §4). A small request arriving at a device a large upload has to itself waits at
most `(D_b + u)/C` for device rate `C`, and with `n` backlogged principals at most
`(D_b + n·u)/C` (research/27 §6.4). Control and durable completions keep §2.3's precedence. On a
laptop the log's flushes, the engines' compactions and the chunks share this one dispatcher,
which is the shared-device case audit §14.1 requires.

**Hierarchical start-time fair queueing, with state only for active principals** (research/27
D3). At the gateway's admission, at each device dispatcher and at each range's queue (§2.3), SFQ
runs recursively: tenants by contract weight, principals equally within a tenant unless the
tenant sets weights, and within a principal its own requests smallest charge first, which is
where size priority belongs. SFQ needs no knowledge of the server's rate, which varies on every
device and path (research/27 §3.2). A principal with nothing queued needs an entry only while its
last finish tag is ahead of virtual time; past that, forgetting it changes no future tag, and
evicting the entry whose tag is least ahead misstates one request's `l/r` at most, the slack
SFQ's bound already allows. The table is therefore bounded by admitted work, `Q = λ·d`, not by
the principals that exist (research/27 §8.1): at 10⁵ requests a second and 10 ms admitted delay,
about a thousand entries. The key is the authenticated principal, never a session token, which
agents rotate (research/27 §8.4). Depth stays three; H-WF²Q+ replaces SFQ at a level only where
that level's measured delay bound needs its tighter worst case.

**Classes by contract, not by size** (research/27 D4). A tenant's traffic carries a class from
its contract, Tectonic's Gold, Silver and Bronze: Gold gets an mClock reservation in device-cost
units and Tectonic's protections, lower classes ceding their turn and their in-flight slots while
Gold waits; background work (repair, cleaning, scrubbing, moves between storage classes,
restores) keeps the floors §2.6 and §3.3 derive. An upper class is fast only while its admitted
share is bounded: past some share Aequitas measured "priority inversion where delay in QoSh
exceeds that of QoSl". So each upper class admits a request with a probability controlled by its
measured latency against the class's target, additive increase and multiplicative decrease, and
downgrades the rest to the class below, telling the client so, rather than refusing them
(research/27 §5.4). With no contract configured, every request is one class and this layer is a
single node of the tree.

**Heavy hitters in bounded memory** (research/27 D8). Each gateway keeps a Space-Saving table of
`m` principals over a window: exact while the active principals fit, and otherwise finding every
principal above `N/m` of the window's `N` requests, counted within `N/m` (research/27 §8.2). It
finds a runaway loop, one principal re-issuing a request at machine speed, and any principal
over a tenant's cap; the fair queue then holds it to its share of its tenant's share and the
tenant's other principals are untouched. `m` is derived (§2.6). Rates of principals whose limits
span gateways use count-min sketches, which add cell-wise across gateways, cells and regions
(§4.5).

**Per-request cost is capacity** (research/27 D10). The CPU, allocations and metadata commands
a small PUT costs are measured and reported, since an agentic mix multiplies them; commands bound
for one range go as one entry a turn (§2.2). Packing small objects into shared blobs, as Tectonic
does, is evaluated against the ownership rules it needs (audit §14.4), not adopted by default.

| Step | What scheduling adds |
|---|---|
| Laptop | all of the above at one gateway, one device dispatcher and one shard: a tree of tens of KB, a cost table per operation from calibration, a Space-Saving table usually not full and so exact; the CLI on the native protocol and stock tools on the HTTP listener under one authority |
| Node | per-device cost models and budgets, many dispatchers; chunk devices chosen by two random choices on credits (§7) |
| Cell | node credits and service counters between gateways and nodes (§3.3), tenants' contracts divided among gateways (§4.5), hedged chunk writes where measured tails justify them (§5.4) |
| Region, fleet | a tenant's regional and global share divided by measured demand on longer intervals, summaries merged by addition (§4.5) |

Nothing a later step adds is configured or paid for at an earlier one: with one participant,
the cell's mechanisms reduce exactly to the local ones (research/27 §7.6).

## 3. Transport

Nodes, and clients through mantle's client library, talk over two layers, as the hecate
specification lays them out (07 §4.7): QUIC for everything stateful, and a separate plane of
sealed UDP datagrams for consensus control and membership. Neither is built.

The QUIC layer combines standard QUIC with slates' measured work, each layer taken from the side
that does it better (research 30 §6):

- **The wire is standard QUIC** (RFC 9000, 9001, 9002), through a vendored quinn-proto with rustls
  on AWS-LC (crypto.md). The standard brings what slates' private dialect lacks or departs from:
  connection migration and path validation, so a laptop that moves between networks keeps its
  connections; key update, stateless reset and connection IDs; loss recovery whose probe timeout
  doubles without a cap and whose bytes in flight count headers and AEAD overhead (RFC 9002
  §6.2.1, §B.2), where slates caps the backoff, counts payload alone and sends its first handshake
  retransmit at 1 ms; and a wire that standard tools decode.
- **slates' measured refinements are patches to that quinn-proto**: Copa as focal ports it, a pacing
  quantum of 1 ms of the rate with a floor of two datagrams (quinn's floor of ten is 1.5 s of
  burst at 64 kbit/s), RACK-style adaptive reordering tolerance (0.254 to 0.540 of capacity on a
  reordering path in slates' measurements, neutral elsewhere), and slates' path-MTU fixes; with
  quinn's 1,024-gap limit on a stream's receive buffer, which bounds a stream's window, derived
  into the window rather than left implicit (30 §4.2–§4.5). Each is offered upstream.
- **The application protocol is mantle's own, in slates' shape**: one connection per peer,
  bidirectional exchanges of a request and a reply, classes of message with a credit reserve for
  the classes above, absolute credits and typed refusals (§3.1–§3.3). Two of slates' choices are
  left behind: the class is set by the message's kind and the sender's role, never carried in a
  stream ID a peer chooses (audit §13.3), and bodies stream through reservations rather than
  being retained whole (audit §11.8).

The same protocol runs over TLS on TCP when a network blocks UDP, so mantle's client keeps every
operation and its semantics there, at the cost of head-of-line blocking on that path alone
(research 30 §4.9). focal-wire's transport core is the starting point for the application layer,
ported rather than depended on, since the crate is coupled to focal's domain (07 §4.8, §7.2 C),
and slates' unwired control-datagram codec is the starting point for the datagram plane (07 §4.7).
The network matrix of audit §13.6 qualifies the combination and could still reverse a piece of
it.

### 3.1 Classes of message

| Class | What it carries | How it travels |
|---|---|---|
| Control | Raft votes and pre-votes, heartbeats, the answers to appends and heartbeats, leadership transfer; SWIM probes | the datagram plane, when the message holds no entries and fits one datagram; ahead of everything |
| Replication | Raft appends carrying entries, and any control message too large for a datagram | one QUIC stream per metadata shard to each peer, above requests |
| Request, waited on | a gateway's commands to ranges and their answers, range reads and Name validations, and toward clients: HEAD, small PUTs and GETs, completions, refusals, progress frames | one QUIC stream per request, a share per tenant (§2.7) |
| Request, bulk | the bodies of large PUTs and GETs: chunk writes and reads, a GET's block streams | one stream per transfer or per block in flight, round robin within the level, below waited-on requests |
| Bulk | snapshots, repair, rebalancing, moves between cells and between storage classes, restores | one QUIC stream per transfer, below requests, with a floor (§3.3) |

The request class has two levels because a request someone is waiting on should not queue
behind the incremental bytes of a large body: RFC 9218 replaced HTTP/2's dependency tree, which
"proved to be complex, and it was not uniformly implemented", with a few strict urgencies and an
incremental flag, and slates measured strict classes best for its control tail where deficit
round robin "starved control and metadata" (research/31 §4.2, D11). Within each level, tenants
share by §2.7's fair queues.

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

0-RTT is off, between nodes and toward clients. "Disabling 0-RTT entirely is the most effective
defense against replay attack" (25 §5), rustls's default is already none (`max_early_data_size`
"The default is 0", 25 §5), and a first message on a new connection may be a mutation; RFC 8470's
rule for HTTP is the one for mantle's protocol, that a client "MUST NOT send unsafe methods ... in
early data" (research/30 §4.9, D15). Clients resume TLS sessions without early data, which skips
the certificate chain, several seconds of bytes at 8 kbit/s, with no replay exposure, since no
application data rides the resumption. 0-RTT for reads is reconsidered only with a measurement of
reconnect frequency and a per-stream signal of which requests arrived early.

Neither the native protocol nor the HTTP/1.1 listener promises order between a connection's
requests. S3 orders only requests whose client waited for the earlier answer, so a promised FIFO
would serialize independent requests for no guarantee a client is owed (research/31 §4.7). Order
is kept where S3 requires it: a GET's bytes, parts by number at completion, and one key's
operations by their commit (research/31 §4.1).

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

A stream's window has a second ceiling that is not memory. quinn closes a connection whose
stream buffer holds more than 1,024 non-contiguous pieces ("too many gaps in stream buffer");
focal reached it with a 10 MiB window and fell back to 1 MiB. With frames of at least `d` bytes,
a window `W` can be left with at most about `W/(2d)` pieces, every other frame lost, so a window
of at most `2,048·d` cannot reach the limit: about 2.4 MB at 1,200-byte frames and 18 MB at 9,000
(research/30 §4.5, D10). The stream window is the lesser of the BDP and that ceiling, and a
transfer whose path's BDP exceeds it travels on several streams of one connection, which removes
the per-stream limit and adds no bandwidth. Whether the vendored assembler should raise the
limit instead is open (§11).

The connection's credit keeps a reserve for every class above a stream's, so a bulk transfer
never spends the last credit a control or waited-on exchange needs: slates measured a control
ping waiting 68 ms of a 40 ms path for a connection credit a bulk stream had taken, before its
reserve (research/30 §6.1). quinn keeps one connection window, so the reserve is the node's own
accounting over it.

**Credits from storage nodes, and service counted across them** (research/27 D12). A storage
node's authorities grant each gateway credits per class on the responses they send anyway, as
Gimbal piggybacks credits on its completions, and revoke unused credit when their queues' delay
rises, Breakwater's grant and revoke with overcommit on speculated demand (research/27 §4.7,
§5.1). Each chunk write and command carries, for its tenant, dmClock's two counters: the service
the tenant received at other nodes since its last request here, and the part of it under
reservation. Each node advances the tenant's tags by them, so its schedule reflects the tenant's
share across the cell "without any synchronization among the storage servers", inaccurate by at
most one request at each other server (research/27 §4.1). On a laptop the loopback peer's credit
is the reservation itself and the counters are zero: no message and no state.

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

**Four clocks, each with its own reclaim** (research/30 §4.7, D11). The rule above is every
wait's in the transfer path, and focal's `carried` is its worked form: a wait is charged, each
period, what the connection sent and did not lose, and gives up only after a period in which
less than a datagram moved, or after the peer had the whole request and a period to answer it.

1. **Transport liveness.** A connection lives while acknowledgements arrive; QUIC's idle
   timeout, at least three probe timeouts [RFC 9000 §10.1], ends a dead one. It is derived from
   the path, never fixed: focal's 10 s fails a thin or long path, and RFC 9308 warns that
   "timeouts shorter than 30 seconds can make it harder to handle transient network
   interruptions". Keep-alives run more often than the NAT mapping lifetime the client measures,
   starting from RFC 9308's 30 s for the public Internet and lengthening where no rebinding is
   seen, since sending more often wastes "unacceptable power usage for power-constrained
   (mobile) devices" (research/30 §4.7, D8).
2. **Request progress.** An exchange waits while its bytes move, as `carried` does, within its
   caller's budget.
3. **Memory and work held by a stalled upload.** When a native upload makes no progress for the
   connection's idle timeout, or the node needs its reservation (least progress for longest,
   §4.3), the gateway closes its current run at the last whole segment, commits it as a
   checkpoint and releases the upload's buffers, coder and renewals (gateway.md §2.1). Its memory
   returns within one run commit; its data stays.
4. **Durable progress held by an abandoned upload.** Committed runs and parts stay until the
   client's declared resume horizon passes, the client aborts, or the bucket's lifecycle removes
   the upload: a policy over storage the operator sees and bounds, not a timeout.

A multi-day upload on a slow link therefore completes as long as each run commits, and an
interruption costs at most one run's replay.

**Retries and reconnects** (research/30 §4.7, D12). Reconnects and retried exchanges back off
exponentially from the measured probe timeout, capped by the idle timeout, with focal's equal
jitter, each pause "drawn uniformly between half of `delay` and the whole of it", so peers that
lost one node do not re-dial it in step. A re-dialing peer's new connection replaces its old one
under the same certificate, as slates' demultiplexer does, and presents the same identity,
revalidating its certificate and the ranges' epochs. A retry keeps its operation's identity
(§5.4). focal's fixed defaults, a 10 ms retry backoff, 5 s calls and two exchanges a peer, are
not taken: a 5 s call fails a healthy 128 KiB append at 64 kbit/s, which needs 16.4 s (audit
§13.5). Bulk transfers resume from their last verified slice (§6.3).

**Raft through a brownout** (research/30 §4.8, D13). Votes, heartbeats and their answers ride
the datagram plane, so a bulk transfer in loss recovery cannot hold them in its congestion
window (§3.4). The election base is ten measured tail round trips of the voters' paths, the tail
including the follower's durable acknowledgement and not only the network (§2.4), so elections
stretch with a collapsing path rather than firing on it. When the path's rate collapses, the
bulk queue's one-tail-round-trip bound collapses with it and the admitted bulk credit shrinks;
durable work already admitted is finished or checkpointed, never abandoned (audit §13.4). A path
that cannot carry the admitted append rate cannot sustain it, and the node reports the rate it
can (audit §13.5).

The network matrix of audit §13.6 is how these rules are qualified (§9).

### 3.9 The QUIC layer's laws

**Congestion control: Copa** (research/30 §4.1, D5). Copa at δ = ½, with focal's half stride on
its velocity, plugged into quinn's controller interface. It is "robust to non-congestive loss and
large bottleneck buffers" and "outperforms other schemes on long-RTT paths" [COPA], where a
loss-based law is held to about `MSS/(RTT·√p)`, 1.2 Mbit/s at 1% loss and 100 ms whatever the
link [MATHIS97]. Two independent grids chose it: slates' 57 scenarios, where it was "the only law
that never stalled and stayed RTT-fair", and focal's 30 paths over quinn. Its measured cost is
the thinnest links, a standing queue of about `1/δ` packets that took ping p99 to 678 ms at
64 kbit/s against NewReno's 289 ms; that cost is answered by the operating packet size and the
pacing quantum below, not by δ. One law runs on every path, since two laws on one bottleneck are
each other's competitor and Copa's competitive mode already answers a buffer-filling neighbour.
mantle's own qualification runs Raft on the datagram plane beside erasure-coded fan-in, repair
and snapshots over the matrix of audit §13.6, with CUBIC and BBRv3 run as candidates and the
selection rule written before the first run, as slates did; a loss there is a defect to diagnose
with its rows recorded, and reopens this decision only with that evidence.

**Pacing and the operating packet size** (research/30 §4.2, D6). What a control frame waits behind
on a thin link is the burst ahead of it. quinn's pacer bursts at least ten datagrams, 12 KB, which
is 1.5 s at 64 kbit/s and 12 s at 8 kbit/s; the vendored quinn-proto paces in a quantum of one
millisecond of the pacing rate, at least two datagrams and at most 64 KiB, refilled to one quantum
after idle so a returning sender never bursts an idle period's worth (draft-ietf-ccwg-bbr §5.6.3,
through slates). Strict priority cannot preempt a packet already queued, so a control frame waits
about `(q + 1)·8s/R` behind `q` queued packets of size `s` on a path of rate `R`; bulk packets are
therefore sent at `s_op = min(s_min, PMTU)`, the smallest size that still carries the admitted
bulk rate `b`, `s_min = (b·o_ip + R·o_q)/(R − b)` for UDP/IP overhead `o_ip` and QUIC overhead
`o_q`, and a control message's packet at its message's size. On a fast path `s_min` exceeds the
PMTU and the PMTU is the size. Where `b ≥ R` nothing is feasible: the node reports that the path
cannot carry the admitted load, and admission lowers `b`.

**Loss recovery** (research/30 §4.3, D7). RFC 9002's estimator, thresholds and persistent
congestion; the probe timeout doubling without a cap ("the PTO period being set to twice its
current value"), the idle timeout and not a cap bounding how long probing lasts; the handshake's
first retransmission at the RFC's `2 × kInitialRtt`, where slates' 1 ms queues copies behind the
first Initial on a thin path; bytes in flight counted as packets sent, headers and tag included.
The reordering thresholds widen from observed spurious losses, after RACK, forget after quiet
recoveries and bound their memory, as slates' does: a reordering path's capacity share went from
0.254 to 0.540 with no cost elsewhere. quinn's thresholds are fixed per connection, so this is a
patch to the vendored copy.

**Path MTU** (research/30 §4.4, D9). Datagram PLPMTUD as RFC 9000 §14.3 applies it, through quinn,
with slates' two measured refinements: a completed search's raise rechecks only the smallest size
that failed, where restarting cost 36 lost probes a raise, and a probe refused locally gives back
its packet number. The search's ceiling is the interface MTU the OS reports (jumbo frames in a
cell, macOS's 9,216-byte UDP cap on loopback), not quinn's default 1,452. Probe losses are never
congestion signals.

**Migration** (research/30 §4.9, D14). quinn's client migration is on: a laptop that moves between
networks keeps its connections, the client library rebinding its socket when the OS reports an
interface change, and the server resetting the path's congestion and RTT state as RFC 9000 §9.4
requires unless only the port changed. Servers do not migrate. Where the new network blocks UDP,
the client continues over TLS on TCP (§3, above), and the operation identity and upload state
being the server's (gateway.md §2.1) let either route finish what the other began.

**Keys over long transfers.** A multi-day upload sends far more than AES-GCM's confidentiality
limit of 2^23 packets under one key (RFC 9001 §6.6), so quinn's 1-RTT key update before the limit
is required and tested (research/30 §6.1).

**The vendored quinn-proto** (research/30 §6.3, D18) carries the pacing quantum, the adaptive
reordering thresholds and the PMTU raise rule as patches, each with its tests, offered upstream,
and gated by `cargo test --manifest-path vendor/Cargo.toml` (CLAUDE.md gates). What would
overturn quinn beneath mantle's protocol is a measurement: patched quinn losing to slates'
private dialect on the paths mantle must serve.

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
  signals and credits before it sends data. Its admission and scheduling are §2.7 and §4.5, its
  transport §3, and its operations, operation identity, resumable uploads and per-block GET
  streams gateway.md §2–§3; the client library and CLI that speak it are gateway.md §6.
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
progress for longest gives it up. On the native protocol that is a checkpoint, not a failure:
the upload's current run is closed at its last whole segment and committed, its memory released,
and the client resumes from the durable offset (§3.8; gateway.md §2.1). On the HTTP/1.1 listener,
which has no such offset, it is ended with `RequestTimeout`, S3's answer for a connection "not
read from or written to within the timeout period" (25 §3), and what it wrote is left to the
sweeps. On the listener's TCP the node also bounds a slow reader's unsent bytes with
`TCP_NOTSENT_LOWAT` and ends a dead peer with `TCP_USER_TIMEOUT` where the platform offers them
(research/30 §3.6).

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

**Two front doors, one authority** (research/27 §7.1, D5). The native listener and the HTTP/1.1
listener share one admission authority, the same charge, the same fair-queue tree and the same
shedding order (§2.7); a principal's share does not depend on the door, and splitting traffic
between them gains nothing, since the HTTP listener only translates into the native operation.
Overload is measured as sojourn: the authority refuses when its queue's minimum sojourn over an
interval exceeds a target derived from measured service time (CoDel's test, §2.6), because when
service times are dispersed "queue length is a poor indicator" (research/27 §5.1). The doors
differ only in what they can tell a client:

- **Natively, refusal is first a withheld credit.** The gateway grants each client connection
  credits per class from the authority's free reservation, revokes unused credit when sojourn
  rises and grants again as it falls, Breakwater's form, which converged from spikes of 1.4×
  capacity in under 20 ms (research/27 §5.1). A client sends a body only against credit, so a
  refused part costs no bytes, and its concurrency follows the server's grant rather than a
  setting. A request sent without credit gets a typed refusal naming its cause (the principal's
  share, a range splitting, a cell out of space) and, where the cause is a queue, a wait derived
  from its measured drain time. Responses carry the admission level, so the client library sheds
  locally before sending (DAGOR §4.2.4).
- **On the HTTP listener, refusal is `503 SlowDown` before `100 Continue`,** where a refusal to a
  client that did not wait for `100 Continue` carries `Connection: close` and the body is not
  read.

On both, an admitted body is never refused for load: it is slowed by flow control, QUIC's stream
credit natively and TCP's window behind the listener, since a large part refused rather than
slowed restarts from its first byte, 9.32 hours for 256 MiB at 64 kbit/s (research/27 §6.4).

**Shed by hashed principal, retries first, at the measured excess** (research/27 D6). Under
overload the admission level moves over a histogram of `(class, retry or first attempt,
h(principal, epoch))`, DAGOR's compound level with the attempt as a key: retries are shed before
first attempts, and among first attempts a deterministic subset of principals is shed so that
the admitted ones finish their multi-request steps, where shedding at random would let every
member of a swarm fail some step. Retries are the commonest sustaining cause of metastable
failure, more than half of 22 incidents, and "a policy with at most two retries will not amplify
the work more than three times" (research/27 §5.7); the gateway holds the retry ratio below
`ρ_max = C/λ − 1` (§2.6). The cut each interval is the measured excess `1 − C/λ_offered`, not
DAGOR's empirical 5% and 1%; the hash rotates on an epoch no shorter than the measured 99th
percentile of a principal's burst, so a step is not split across epochs. The key is the
principal, never the session: WeChat found users logging out and in to escape session-keyed
shedding, which agents with short-lived sessions do by construction (research/27 §5.2). The
attempt number comes from the client library natively and from `amz-sdk-request` behind the
listener; an HTTP request without it counts as a first attempt.

**A tenant's share across gateways** (research/27 D13). A cell's admission service, stateless
and restartable as DynamoDB's global admission control is, divides each tenant's contract among
the gateways serving it in proportion to their measured demand, DRL's FPS, refreshed on an
interval chosen from the measured rate at which demand moves and the error the contract
tolerates, since "a distributed limiter cannot be simultaneously perfectly accurate and
responsive" (research/27 §7.6). A gateway that cannot reach it keeps admitting from its last
grant (architecture §9). With one gateway the grant is the whole contract and no message is
sent. Per-range and per-node limits remain as ceilings (architecture §8; 09 §3.6). The region's
and fleet's division is architecture §8's.

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

**Hedging, within the principal's share** (research/27 D14; research/31 §3.8, D10). A chunk
read outstanding past the `q`-quantile of its measured latency is sent again to another copy,
or its block decoded from others; hedging after the `q`-quantile adds at most `1 − q` of the
reads, so `q` follows from the read amplification the devices' measured headroom absorbs, Dean
and Barroso's 95th percentile being the cited reference until measured. A degraded read costs
`data` chunk reads, and reconstructed reads are capped by the same headroom, with Tectonic's 10%
the cited upper reference. Chunk writes are hedged at the cell step by reservation, Tectonic's
`n + Δ` reservations with data sent to the first `n` that accept, which cut p99 by about 20% at
80% utilization (research/27 §6.3); `Δ` comes from the cell's measured tail and spare capacity.
Hedges are charged to the principal's share like primaries, so hedging under congestion cannot
deepen it (audit §13.5). With one device there is nothing to hedge to, and nothing is sent.

### 5.5 The laptop's path

On a laptop every request's destination is the node itself. The routing layer delivers it
through a loopback peer that skips the socket, TLS and serialization, and keeps the rest: the
reservations charged by the encoded length the frame would have, the class, the range's
fencing and the sessions. A command reaches the local shard's queue for its range; a chunk write
reaches the local volume's ticket. The drivers, the sessions and the ranges run the
same code on a laptop as in a cell.

### 5.6 Integrity across the path

The ends of the path are the client and the bytes on the device, and every check in between is
the end-to-end argument's "incomplete version", worth having because it finds a fault where it
can still be repaired (research/31 §5.1). Corruption comes from CPUs and memory as well as disks,
and replication copies it: RocksDB-level corruption at Meta, CPU and memory faults included, ran
"roughly once every three months for each 100PB", and "in 40% of those cases, the corruption had
already propagated to other replicas"; Google found "a few mercurial cores per several thousand
machines", one of whose AES faults was "self-inverting: encrypting and decrypting on the same core
yielded the identity function" (research/31 §5.2). A PUT's bytes are sealed once and coded once,
on one gateway core, before they fan out to every chunk, so a fault there is common to every copy
and passes every later check but an inverse (research/31 §5.2). Each boundary therefore has its
check, and every checksum is carried from where it was made, never regenerated over bytes that
may already be wrong (research/31 §5.4–§5.5):

| Boundary | Check |
|---|---|
| Client to gateway | QUIC's or TLS's AEAD; the S3 checksum over the plaintext, verified at the body's end before any commit |
| Gateway memory, before sealing | a CRC-32C per 64 KiB plaintext segment as it arrives, combined into the request's checksum; after sealing, the segment is opened on a different core and its plaintext's CRC compared (D13) |
| Sealing | that open, on another core than the seal's, which catches a self-inverting fault |
| Erasure coding | a random linear check of the parity against the data on another core: for a fixed random vector `r` over GF(2^16), `r·parity = (r·P)·data`, `r·P` computed once per code, about `1/m` of an encode for `m` parity chunks; a wrong parity escapes with probability at most 1/65,536 per check, and a second vector squares it (D14). A full re-encode replaces it where the coding pool's measured headroom affords one |
| Gateway to storage node | QUIC's AEAD; the chunk's per-64 KiB CRC table as the gateway computed it, verified and stored by the node (chunk-store.md §3.1) |
| Storage node to device and back | the record's header CRC, per-block CRCs, identity and incarnation, and the separate index copy (chunk-store.md §3) |
| Storage node to gateway | the stored CRCs sent with the bytes and verified by the gateway |
| Decode and open | the block's CRC-32C after decode; the segment's GCM tag at open |
| Gateway to client | the AEAD; the S3 checksum headers when asked; on the native protocol a CRC-64/NVME per block (gateway.md §3) |
| Range commands | a CRC computed where the command is built, at the gateway, and checked at apply on every replica (replica.md §2) |
| Engine | block checksums on read, per-key-value protection in memtables and write batches on (engine.md §3) |
| Caches | every cached row keeps the CRC it was read with and every cached block its tag or CRC table, checked on each use (gateway.md §5) |

A failed inverse check redoes the transform on another core and checks again; a PUT fails with
`500 InternalError` only if the second fails too, and nothing is acknowledged before the checks
pass (research/31 §5.7).

**Repair never spreads corruption** (research/31 §5.8, D19). Reconstruction that computes new
redundancy from a corrupt input is how parity pollution spreads damage, scrubbing "one of the main
causes" (Krioukov et al.). Repair verifies each source chunk's CRCs, decodes the block it rebuilds
and checks it against the Block row's CRC, writes the rebuilt chunk only then, on a core other
than the one that computed it, and never takes a cache as a source. A decode that fails though
every source verified tries another subset and is attributed, never repaired from.

**Which checksum carries what.** CRC-32C over 64 KiB blocks keeps Hamming distance 4 across the
block, so every error of three bits or fewer is caught and wider damage passes with probability
about 2^-32 per checked block; at an exabyte read a day and one damaged block in a million, about
one a year would pass, which is why the tag and the client's checksum stand behind it (research/31
§5.3). CRC-64/NVME's distance profile is published nowhere the research found, and the polynomial
Koopman lists as "Jones" is a different one, so no claim rests on its distances until they are
computed at 64 KiB and 8 MiB by a program that first reproduces Koopman's CRC-32C and "Jones"
figures (D20).

**Attribution and quarantine** (research/31 §5.6, D17). Every inverse check that fails, every
decoded block whose CRC fails while each source chunk verified, and every tag that fails over
chunks whose CRCs verified is attributed to the node and core that produced the bytes and recorded
with the core's ID. Each node's counts per core are compared with the cell's: a concentration on
one core removes it from the coding pool and the sealing path, a concentration on one node fences
it from writing until it is screened, which is Google's reading that "reports from multiple
applications that appear to be concentrated on a few cores might well be CEEs". No threshold is
chosen: the quarantine rule's thresholds follow from the false-positive rate the cell measures with
no faulty core (research/31 §8, T3). macOS cannot bind a worker to a core (§1.2), so there
attribution is to the node.

**Memory** (research/31 §5.6, D18). DRAM errors are mostly hard and repeat, and a correctable error
raises the chance of an uncorrectable one 9 to 400 times. The node reads the platform's
correctable-error counters where they are exposed (Linux EDAC; what macOS and Windows expose is
open) and treats a rising count as device health: its caches shrink toward nothing, its in-memory
indexes are verified again, and it is drained when the rate passes what the cell's history shows
precedes uncorrectable errors. A background verifier, paced as the scrubber is, recomputes each
cached row's CRC and checks each volume's index checkpoint against the live index, feeding the same
attribution.

## 6. Metadata ranges on the node

### 6.1 Engines

The production engine is mantle's Rust port of RocksDB 11.8.1 (research 24), one instance per
range replica with its WAL off, as ZippyDB and TiKV run one instance per shard (12 §6.1). The
port's file system is `hyper-block`'s device file (24 §2.2), so its flushes are the platform's full flush and
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
| Placement of a block's chunks | the Block range refuses two chunks on one volume (metadata.md §1) | the cell's volume table in the root range, published to every node in full on a loop (architecture §4); gateways choosing each chunk's volume from placement's candidates by two random choices weighted by advertised credit, never a global least-loaded order, which every gateway would read alike in a herd (research/27 §6.2, D11a: two choices give "exponential improvements" over one); among feasible devices, the one whose projected budget exhaustion is latest (chunk-store.md §9.3); copysets per media pool that satisfy the domain rule (04 R2; storage-classes.md §3); the Block range refusing chunks that share a domain |
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
| Scheduling | per authority the fair-queue table's size against its bound, the admission level, credits granted, revoked and wasted, refusals and downgrades by class, the heavy-hitter table, retries seen and shed |
| Caches | per cache its size, hit ratio, miss-ratio curve and chosen policy, entries dropped on a failed check |
| Integrity | inverse-check failures, failed decodes and tags over verified chunks, each by node and core; quarantined cores; correctable memory errors |
| Power and devices | power source, saver and thermal state and the work they deferred; each device's budgets spent and projected exhaustion; its scrub period and the bound that set it; storage classes' targets against what placement achieves (storage-classes.md §8) |
| Threads | the process's thread count from the OS against its derived bound (§1.2) |

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
simulated device that loses, keeps or tears unflushed writes (`hyper_block::sim`), engines on
the model engine and, once the port runs over `hyper-block`, on the port itself, which
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

The simulation also carries the faults and loads the 2026-09-30 designs answer for:

- **Logical clients in the millions** on one thread, which only records make possible, with
  injected correlated bursts; every queue within its bound, and wakes counted per queue: at most
  `K` plus the waiters admitted for `K` completions, never `K` times the waiters, and a fence
  waking each waiter exactly once (research/26 §8, tests 3 and 6).
- **The owner's scenario** (research/27 §10): a large multipart upload at its steady rate, then a
  spike of small PUTs from many principals arriving open loop, spread or on one prefix, with
  retry storms with and without the attempt header, a looping principal, a new principal per
  request, a slow path, repair running and a Gold tenant among the small writers; both client
  populations, the native library and stock SDKs through the HTTP listener, in one run. It
  checks the large upload's goodput during the spike against its weighted fair share, the small
  PUTs' latency against the same PUTs alone plus the bound of §2.7, recovery within one chain
  latency, every per-principal structure within its bound, and the two populations' shares equal
  within measurement. FIFO, round robin without the hierarchy and strict SRPT run as baselines
  in the same binary.
- **Faults at every protocol step** (research/30 §10): a proxy cuts each connection after every
  frame kind and at every byte-offset class, gateways are killed at each progress point from
  bytes read to answer delivered, storage nodes and range leaders lose power before and after
  their acknowledgements; every client ends committed once with the first answer, resumed with no
  committed run sent again, or refused with no effect, and once faults stop nothing is
  unsettled.
- **Corruption in memory and on cores** (research/31 §8, T1–T3): flips in the plaintext after its
  CRC, in a sealed segment, in a data chunk or parity after coding, in a node's receive buffer,
  in an encoded command, a memtable entry and every cache; a core that seals or codes wrongly,
  self-inverting included. Each is caught at its boundary before acknowledgement or service and
  attributed to its core, and a week without injected faults measures the quarantine's
  false-positive rate.
- **Caches under linearizability** (research/31 §8, T6): every cache on, gateways that restart
  warm or cold, leaders that change; the WGL checker accepts every history, and four broken
  variants it must catch within the first seeds: a cache served without validation, a validation
  joining a round already out, a negative entry served on a timer, a list page served without its
  range's last-write check.
- **Power changes mid-batch**: no acknowledgement before durability across a switch to battery or
  a thermal step, and every deferred job still meeting its deadline (research/28 §8).

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

Real-process tests count what the design bounds: the process's threads, sampled from the OS
throughout a measurement at a device's full reported depth and a `bench log` run with logical
replicas at a hundred times the driver count, never above §1.2's formula; the same peak at `R`
and `10R` logical clients; a pool asked past the budget refused with no thread started; and the
achieved depth reaching the depth asked on each platform's mechanism, or its shortfall reported
(research/26 §8, tests 1, 2, 4, 5). The thread count comes from `/proc/self/status` on Linux,
`proc_pidinfo` on macOS and a toolhelp snapshot on Windows, the last two in the OS-interface files
`scripts/check-contracts.py` lists. Network emulation is `tc netem` and namespaces on Linux,
dummynet on macOS and clumsy on Windows (research/30 §10); laptop energy is measured by
measurement.md §9's differential method.

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
| A. The node inside | tickets with wakers for the log and volumes; one-to-one wakes in place of every broadcast (§1.3); one issuer per physical device with io_uring, a completion port or the bounded pool, and the process thread budget (§1.2); measurement's depth by the same mechanisms (measurement.md §8); shards with the staged turn, deficit round robin and timers; the admission authorities and budgets; the dispatcher, charges and fair queues of §2.7; directories, `LOCK` and profiles; the device plan by class and its probes (chunk-store.md §2.1; measurement.md §9); power and thermal inputs (§1.8); startup, stop and rollback; the loopback peer; the many-group simulation host with logical clients as records | the model engine | the many-group simulation passes its invariants at thousands of ranges per device with every mutation caught, and at millions of logical clients with wakes counted; the thread-count tests of §9.2 hold; real processes driving tickets and shards over real logs and volumes, killed at random, lose no acknowledged update or chunk |
| B. A laptop that serves S3 (audit §17, third) | the native QUIC protocol and the HTTP/1.1 S3 listener under one admission authority (§4.5); the request path, bodies, errors and overload of §4; the client library and CLI with their journal (gateway.md §6); operation identity and resumable native uploads (gateway.md §2.1); per-block GET streams; the gateway's caches with Name validation (gateway.md §5); the integrity checks of §5.6; storage classes on one device (storage-classes.md §8); the drivers served in-process (§5); sessions; placement from local volumes; the key authority of §4.6; chunk reconciliation per volume | P1–P10 and P14 (the port opening, writing, flushing, recovering, reading and compacting, with shared budgets); P15 for baselines | the laptop row of §9.3, the owner's scenario at laptop scale, and the protocol-step fault tests with both client populations |
| C. A cell of several nodes (audit §17, fourth, first half) | QUIC transport with its classes, credits, laws (§3.9) and defenses on the vendored quinn-proto; the datagram plane; SWIM; invitations and joining; remote chunks; node credits and service counters (§3.3); tenant shares across gateways (§4.5); hedged reads and reservation-hedged writes (§5.4); snapshots over QUIC; voters across domains; the replica side of splits, merges and moves; core and node attribution across the cell | P12 (range deletions) and P13 (checkpoint, export, ingest) | 3- and 5-voter histories accepted under crashes, partitions and disk faults, in simulation and with real processes; a node lost for good replaced with no acknowledged write lost; the congestion-control qualification recorded |
| D. A regional cell (audit §17, fourth) | repair ordered by margin; rebalancing and drains behind the operation gate; device health and budgets in placement; adaptive device plans; media pools, cold pools, moves and restores between storage classes (storage-classes.md); the network matrix; the multipart and GET pipeline for massive objects (audit §16); the fast track as an experiment with its own proof (audit §5.6) | P9's cache measured in both regimes (23 §0 item 6) | the regional row of §9.3, at the largest cell the deployment declares, within its stated latency, resource and rebuild limits |
| E. Cells and the fleet (audit §17, fifth) | the cell map in the root range, the router, the mover between cells after its TLA+ model; key authority across cells; regional and fleet shares (architecture §8); zonal storage classes and directory buckets near compute; versioned upgrades and rollback (audit §8.7); the chosen geographic contract (audit §8.1) | none new | the fleet row of §9.3: growth, cell retirement, stale routes and region failover keep ownership, with measured recovery objectives |

Universal and FIFO compaction (P11) are not needed unless the cost model chooses them for a
range's workload (12 §6.6).

## 11. Open

- **Cores among pools.** Three pools sized to the granted cores oversubscribe under full load;
  whether dividing cores by measured demand does better is measured once phase A runs.
- **Quiescing idle ranges**, and the protocol that wakes one without weakening its election or
  read rules (audit §15.1; 06 §A4.3).
- **The datagram plane against RFC 9221's datagrams on the QUIC connection.** The separate
  socket escapes bulk's congestion control and carries RFC 8085's obligations itself (§3.4);
  the network matrix decides whether the separation pays for itself.
- **The per-stream window**, fixed per connection in quinn (§3.3): whether the vendored
  assembler should raise its 1,024-gap limit rather than a transfer using several streams.
- **IoRing on Windows 11** against completion ports (research/26 §10), and whether
  `os_sync_wait_on_address` (macOS 14.4+) can replace the per-thread semaphore behind `park`.
- **How often clients meet networks that block UDP**, which sets how much the TLS-over-TCP route
  carries (research/30 §11).
- **The validation's cost at a hot Name leader** under agent swarms, one point read per key per
  round, and whether follower reads with a leader-issued read index are needed before leases
  (research/31 §9).
- **The cell authority's private key** once a cell has many nodes: which nodes hold it, or an
  external issuer.
- **The idle engine instance's cost**, which sets the ranges a node can host (12 §6.1).
- **Follower reads and leases**, kept out until a deployment states its clock bound (replica.md
  §7; audit §5.5).
- **The fast track**: disabled until its application and persistence proof and its crossover
  measurements (audit §5.6, §11.5).
- **SWIM's indirect probers and suspicion timeout** for a cell's size, from SWIM's analysis of
  detection probability against load (06 §A8.2).
- **The geographic contract** (audit §8.1): which model of cross-region writes and failover
  mantle offers.
- **`x-amz-retry-after`** on `SlowDown` (25 §3).
