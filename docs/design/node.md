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
| Network runtime | hyper-rt's shards, one thread per core the process is granted (the owner's decision of 2026-10-05: a shared thread-per-core runtime, no tokio) | QUIC connections and the datagram plane (§3), SWIM, HTTP connections (§4), the drivers of each S3 request, the routing layer (§5), clients and connections as tasks |
| Metadata shards | one per granted core | range replicas: stepping, ticking, proposing, driving, applying, serving confirmed reads (§2) |
| Coding pool | one per granted core | erasure encoding and decoding of whole blocks, block checksums, the write path's inverse checks (§5.6) |
| Device issuers | one per physical device | every volume's writer, cleaner and scrubber and the device's Raft log writer, as state machines (chunk-store.md §4; raft-log.md §3); the device's dispatcher (§2.7). As built: every volume's reads, writes and flushes; on Linux held by the kernel through native AIO on the issuer's own thread, elsewhere dispatched to the device's pool; the writers, cleaners and scrubbers are still threads of their own (STATUS item 4) |
| Device pools | per device, at most the device's measured depth, only where the platform has no asynchronous interface the device can use (below) | blocking reads, writes and flushes the issuer hands them. As built: macOS and Windows (below) |
| Engine background | flush and compaction pools shared by every engine instance on the node (24 §4.7, engine P14), whose file I/O goes through the device's dispatcher | the engine's own background work |

With `C` granted cores, `D` physical devices and pools of `p_i` workers, the process's
long-lived threads are `3C + D + Σ p_i` plus the engine's pools, whatever the number of
clients, ranges or volumes (26 §4.7, DERIVED). They were `3C + Σ d_i + L + 3V` for `L` log
devices and `V` volumes, three threads per volume, 300 at 100 volumes (audit §14.1); the issuer
per physical device replaces them, which closes the question this record left open.

**Device I/O in flight, by platform, at every step.** The issuer keeps its device's depth in
flight through the platform's asynchronous interface where one exists: Linux's native AIO, one
context per device of its depth, durable writes followed, once they complete, by
`IOCB_CMD_FDSYNC` through the same context (the owner's decision of 2026-10-01: not io_uring,
whose attack surface was behind 60% of the kernel exploits Google's kCTF paid for, which Docker's
default seccomp profile refuses, and which a kernel can disable; hyper-raft
`docs/research/issuer-completions.md` §2, §5); overlapped I/O on one completion port on Windows,
with `FILE_FLAG_NO_BUFFERING` so the I/O stays asynchronous (26 §2.2–§2.3). io_uring drove
"about 1.2M IOPS" without polling, twice Linux AIO's on the same workload (26 §2.2), and stays an
operator's opt-in to be measured. macOS has no such interface for a full flush or more than 16
requests (26 §2.4); there the device's pool of blocking workers carries the depth, sized at the
smaller of the device's reported queue and its measured knee. Research note 26 proposed the native interfaces
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

*As built (2026-11-14).* On Linux a device's issuer holds its depth in the kernel
(`Issuer::start_device`; hyper-raft `docs/research/issuer-completions.md` §9): its one thread,
whatever the depth, hands every volume's reads, writes and flushes to one native AIO context of
the depth and reaps them, each completion ringing an eventfd its submitters ring too, so it waits
for both at once. A thread a transfer in flight had cost a node its stack in its floor (§1.4):
pods on CI runners measured depths of 1 to 316 on one disk, and one of 316 needed
2,173,380,739 bytes, 681,574,400 of them its threads' stacks, past its 2 GiB (kind-cell run
131575146397). macOS keeps the pool, and Windows keeps it until its completion port is built.

**A process thread budget.** Every pool draws from one budget, decided before any thread
starts, so devices times depth cannot add up past it. On macOS its ceiling is the OS's own
statement of what one process may run, `kern.wq_max_threads` (512 on the development machine),
the cap Apple's workqueue under GCD and Swift concurrency applies to itself (26 §1.5, §2.5);
on Windows the pools carry every device's depth until its completion port is built, and on Linux
they exist only where native AIO cannot carry the transfers, a measurement over a buffered file
(`mantle_disk::measure`), a device's issuer holding its depth in the kernel on its one thread
(as built 2026-11-14, above). All draw on the same budget, whose ceiling on Linux is the smaller
of `kernel.threads-max` and the soft `RLIMIT_NPROC`, and on Windows the 500 worker threads
Microsoft states as a thread pool's default maximum ("Thread Pools", Best Practices)
(`hyper_block::threads`). A pool that
would pass it is refused, naming the device and depth, before any thread exists (26
recommendation 4); the device then runs at the depth the budget leaves.

**The coding pool's cores.** Coding-pool workers are pinned one to a physical core where the OS
allows it (`sched_setaffinity` on Linux, `SetThreadAffinityMask` on Windows), so the write path
can run a transform's inverse on a different core from the transform (§5.6). macOS offers no
binding affinity, so there the inverse runs on another worker, on whichever core the scheduler
gives it, and a fault tied to one core is caught only when the two land apart.

The network runtime never blocks. tokio says why, of any runtime that runs many tasks on a
thread: "A blocking operation performed in a task running on a thread that is also running
other tasks would block the entire thread, preventing other tasks from running", and CPU-bound
work belongs in "a separate thread pool dedicated to CPU bound tasks" (25 §1). Replicas
therefore do not run as the network's tasks. Their work includes
engine reads that miss the cache and wait on the device, reads of log entries no longer in
memory, which are synchronous (audit §12.2), and applies that take milliseconds: completing an
upload of 10,000 parts applies in 6.3–6.7 ms (measurements/2026-09-29-metadata-apply.md). A
shard owns its replicas outright, so a `Replica` is touched by one thread and needs no lock.
Encoding a block under RS(9,6) needs a work space of 128 MiB (gateway.md §4) and is CPU-bound for
its duration, so it goes to the coding pool.

*As built (2026-10-20).* A node's ranges run in one task with its transport on a hyper-rt shard
(`crates/node`), their rows in mantle's engine (`mantle_range::Stored`) or the model. The
shard's owner states that it may wait on the device (`mantle_engine::store::permit_blocking`),
so the engine takes its synchronous paths there, as on a thread with no shard: its cache misses,
writes and maintenance wait on that shard and hold up its transport while they do, which the
liveness stream counts in its timer's granularity (the owner's own waits). Moving the replicas
to metadata shards apart from the network's, as above, is still to build.

*As built (2026-11-19), the engines' maintenance workers.* Each range's engine packs its full
memtables and runs its trunk's compactions on maintenance workers of its own (`derive::ENGINE_WORKERS`,
one; `process::engine`, `ShardDb::set_workers_within`), each writing through its own handle on the
store's file, so an apply on the node's shard hands that work out and takes the results back. Inline,
it was a quarter of the busiest shard's time under small PUTs: a macOS `sample` of the range leader
during `mantle bench doors --sizes 1 --kinds PUT,GET --load open:12000` (2026-11-19) found 24.8% of
shard 0's busy time in `pwrite` from `Trunk::step_inline` and the memtable's packing under
`Replica` applies, 36% on the followers' first shards. A rotation that finds no worker free waits for
one rather than write the branch itself (`ShardDb::rotate`), and the shard waits for its workers
where it must (a rotation without room, a checkpoint) on a count of their messages, since hyper-rt's
channel refuses a shard's thread a blocking take (`trunk::pool::Signal`; it had failed the first
checkpoint). With workers, applies on the shard wrote no page of 1,100 entries where inline they did
(`tests/engine_workers.rs`). A pool serves one engine's file (`trunk::pool::serve`), so the engines
cannot share the pool the table above states: a node holds a worker a range, five in a cell, and
their stacks and statements are counted (§1.4). A node whose allowance does not hold its need with
them runs none, its engines packing and compacting on its shard as before. *Open:* one pool serving
every engine on the node, a job carrying its engine's file; and the checkpoint's own synchronous
work on the shard, below (§6.2).

*As built (2026-11-19), the workers' extents.* A job takes its extents from the store's before it
goes out, a grant sized to what it reads, more than it writes. Given workers, the node suite's
followers fenced installing a snapshot, the engine's own error `LimitExceeded { what: "extents of
a store", limit: 256 }` twelve times in four runs: an install into the cells' store (256 extents
of four 4 KiB pages, 4 KiB memtables) that the shard alone grew to 180 extents filled it with
one worker. Ten compactions done but not yet applied held their whole grants, 130 extents for
the few they wrote, since a pass applies its tasks once all are back; and packings cut at the
root's pivots went into a bundle whose flush waited, 194 branches of an extent each. Now a
result gives back what it did not write as it is taken; a packing's branches go in flight where
their pivot's flush waits (engine-structure.md, Incorporation); and a packing or a compaction
goes out only where the store holds its grant beside what is held: else it waits while another
job may give extents back, and with none the shard runs it, taking extents as it writes them,
as without workers (`Trunk::dispatch_tasks`, `ShardDb::sort_off`, `ShardDb::hand_off_sorted`). The same install with its
worker then took 180 extents (`tests/engine_workers.rs`). So the store's size reserves nothing
for jobs in flight (`derive::engine`'s `engine.max_extents`, unchanged): a job never takes room
the store lacks. Reaching the limit is still the engine's refusal only where what the store
holds fills it, as without workers. *Open:* `STORE_BYTES_AN_OBJECT` was measured before the
workers (2026-10-31); the workers' layout is not the shard's, within a few extents of 256 either
way near the limit, and the transfer harness measures it again with them.

*As built (2026-11-17).* The node runs on hyper-rt's multi-shard `Runtime`, a shard for each CPU
the process runs at once (hyper-rt `Facts::cpus_at_once`; `process::runtime`), its task launched
on the first shard and the runtime shut down at its end (`process::run_on_first_shard`). That
task, with the node's ranges, log, transport and QUIC door, stays on the first shard; the S3
connections go to the others, passing over a shard the runtime's watchdog sees stalled and one
gone (§4.1). The replicas are not yet placed across the shards; §2.9 states how they will be, with
the log and the native door, and the order of building.

*As built (2026-11-17), the node's task Critical.* The node's task is spawned in hyper-rt's
Critical class (`coop::Class`; hyper-raft docs/runtime.md §3.15), and the listener and every S3
connection stay Normal, the class every other spawn takes: woken by a peer's frame, a timer or its
log, the task goes ahead of the connections queued on its shard, and each class with work keeps an
equal share of the shard's time, the lead the shard's quantum, so neither starves the other. What
it buys in heartbeat and commit latency under S3 load is not measured yet. Under `range_shards =
spread` the task of each shard past the first that runs ranges is spawned Critical alike
(2026-11-18, §2.9).

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

**Memory.** The node's floor is the smallest budget that admits one unit of work of every class
at once: one PUT's block window and its coding work space, one GET's block, one `Ready` of the
largest entry for each range with work, the engines' minimum write buffers, one transfer slot per
peer. That floor is computed from the settings, and the node refuses to start, naming the class,
when it exceeds its allowance: the operator's ceiling, else the limit the operating system sets
for the process, else physical memory. What remains past the floor sizes the volumes' indexes and
the engines' resident metadata.
Independent defaults, each safe alone, add up past a laptop's memory: the chunk store's pools
alone can keep 96 MiB per volume (audit §12.3). A laptop needs the smallest population that
serves its work (audit §6.3), and more is granted by stating a ceiling.

**Descriptors** (2026-11-19). A file, a socket, a pipe, a poller and an eventfd are each a
descriptor, and a process may hold only so many: its soft `RLIMIT_NOFILE` on Unix, on macOS at most
`kern.maxfilesperproc` besides, and on Windows 2^24 handles ("The per-process limit on kernel
handles is 2^24", Microsoft Learn, Kernel Objects; the C runtime's `_setmaxstdio` bounds only its
own streams, which Rust's std does not use). `mantle serve`, `mantle node`, `mantle derive` and
`mantle bench` raise the soft limit to the hard one as they start, the range an unprivileged
process may set (getrlimit(2)), on macOS to the smaller of the hard limit and
`kern.maxfilesperproc`, as its getrlimit(2)'s COMPATIBILITY note asks, and read it back
(`crate::descriptors`): the bound the node derives within, said as `descriptors.limit`. A systemd
user service starts at a soft limit of 1,024, under which `mantle bench doors --clients 2048` died
mid-run at `EMFILE`. The node holds `descriptors.fixed` whatever its operations: standard input,
output and error; each shard's poller, with its kick's eventfd and its timerfd on Linux; the signal
thread's pipe; each volume's file and lock and, on Linux, its issuer's eventfd; the node directory's
two locks and its log; a file kept and a name looked up at once; each range's store; the transport's
endpoint and plane, and two connections a peer, each a socket of its own where connected sockets
share the endpoint's port (Linux and the BSDs); and the S3 listener's socket, the connections it
answers `503 SlowDown` (`listener::REFUSING`) and the one past both it closes as it comes
(`derive::fixed_descriptors`). Each operation holds `descriptors.each` beside them: its native
client's two connections and its S3 connection (`derive::operation_descriptors`). The operations the
rest holds, `descriptors.operations`, bound `gateway.objects` where they are fewer than the memory
holds, said `bound`, and with it the S3 listener's connections and the native clients the transport
admits: a connection past them is answered `503 SlowDown`, `Connection: close`, or refused at
admission, never met by `EMFILE` at `accept`. A limit short of the fixed descriptors and one
operation refuses the start (`DeriveError::Descriptors`). `mantle node` runs the numbers it is told,
and its start, before a socket opens, refuses operations whose descriptors with its fixed ones pass
the limit, naming them (`derive::within_descriptors`).
`mantle bench doors` counts its own clients' sockets, its cell's pipes, the file it reads or
writes at a time and, under `--relay` and `--perf`, theirs, against the same limit before it starts,
and refuses past it, naming both (`bench_doors::descriptors_fit`).

A configuration key the node does not know is refused, as the S3 layer's readers refuse what
they do not know. Every derived value is said with the input or constraint that bound it (audit
§12.6): by `mantle serve` before the node runs, a `basis` line each (below, as built 2026-10-23),
and by `mantle derive CONFIG`, which says the same without serving (2026-11-14).
`mantle status DIR` is the supervisor's readiness check alone (§1.6; kubernetes.md §5).

*Planned for `mantle serve` (2026-10-21; research 64).* `mantle node`, the process the tests drive,
is told every number it runs with. `mantle serve` is told only what the first column above names,
measures the second, and derives the rest by these rules:

- **Every class at its floor.** The memory paragraph's floor is computed class by class from the
  structures each fills, and the settings are the floor's: the operations the memory holds past the
  data's wants (§2.8), a window of one block, an append out to a follower for each entry a range
  can hold uncommitted (`derive::in_flight`, §2.8), an entry of the range's largest in each append and each
  apply, one chunk ask to each node a block's width needs, the transport funding each peer's
  windows at QUIC's initial window beside the messages' room, each connection's reserve, the
  connections' state at the most connections the endpoint holds and the receive ring at its most
  past its first buffer (`bounds::least_transport`, `bounds::connections_state`,
  `bounds::ring_growth`, §3.3); its growth past them is a performance want (below). The node's
  allowance is the
  stated ceiling, else the limit the OS sets for the process (hyper-rt's `MemoryFacts::limit`:
  cgroup v2's `memory.max`, a job's limit, `RLIMIT_AS`), else physical memory, a ceiling past what
  the OS lets the process hold refused (`derive::allowance`, `DeriveError::Ceiling`; research 64
  §5). The floor counts its classes in order and the node refuses to start at the class whose
  bytes take the count past the allowance, the whole floor named beside it (`derive::floor`,
  `DeriveError::Memory`). What remains goes in rule D's order (research/87 §4; the coordinator's
  order of 2026-11-19): (1) the capacity wants, each volume's index and each range engine's
  resident metadata, whole while what remains holds them all, and when it holds less each the same
  part of its want, so the indexes and the engines fill together (`derive::shares`; the owner's
  choice of 2026-10-31); (2) the operations the node serves at least (`derive::served`: one at the
  most one is charged, and with an S3 endpoint one stock client's parts); (3) the transport's
  growth past its floor for the connections those operations open, each share from QUIC's
  initial window to a receive chunk, whole while what remains holds it; (4) every operation more
  that what remains past those holds, each costing its client's windows at a receive chunk, so
  each operation's windows hold a chunk and its datagrams land without a copy. No page cache
  takes a share: each engine's borrows the operations' idle bytes (below). The node's need
  (`memory.need`) counts (1) and (2) alone, so a node given its need starts with every capacity
  want whole, and the transport's growth and more operations take what a larger allowance
  leaves. The capacity wants: a
  volume's index a fragment at each of its blocks (`derive::index_wants`), and an engine the
  extents its rows take for the cell's blocks when each node's volumes hold what this node's do, an
  object a block, each block's chunks on as many nodes as the scheme chosen at the cell's domains
  (`derive::cell_blocks`); a node with no volume of its own counts the fragments of a volume of the
  bytes its cell's founders state theirs at (`cell.volume`, which `mantle init grant` writes from
  the cell file's `volume_bytes`, the most any founder's volumes state), at its own device's block:
  counting none, its engines wanted nothing, took what the operations left, nothing on CI's 7 GiB
  macOS runner, and it refused to start (2026-11-19); at the most store a range's engine took an object
  (`derive::STORE_BYTES_AN_OBJECT`, measured), each extent at the metadata measured at the densest
  rows, 89,208 millionths of an extent's bytes, and its allocator's own (`derive::engine_wants_for`,
  `shard_db::MEASURED_METADATA_PPM`, the owner's choice of 2026-10-23 over its statement for any
  keys, which holds a twentieth of the rows); an engine as many extents as its share holds
  (`derive::engine_within`). Each engine's page cache wants every page of those same extents, at
  the store's page size, since the store's bytes an object are its file's length at its most, its
  pages as filled and compaction's room counted, and the cache keeps live pages alone
  (`derive::engine_cache_wants_for`); its want, at most its store's pages, is `engine.cache_bytes`,
  the most it holds, and no share of the split: it borrows the operations' bytes no running
  operation holds (rule B for the caches, research/87 §4; `admit::cache_part`, `host::lend`).
  Admission holds `gateway.objects_bytes` for the running operations' charges (§2.8); the bytes
  none is charged are idle, and each range's engine on the node's shard holds its part of them,
  in proportion to the engines' wants, each the same `engine.cache_bytes`, so the same part, at
  most its want. Before admission charges an operation bytes the caches hold (an operation
  admitted, a get's or a copy's window widened once its version is read, a copy's put, a
  rebuild of the cell's repair), the caches shrink at once to their parts of what stays idle,
  in the same turn on the node's shard where both run: no operation waits on a cache, and the
  operations' charges and the caches' bytes are within `gateway.objects_bytes` at every moment.
  A cache shrinks in one bounded trim (`Cache::shrink`): its limit lowered, its pages past it
  freed by S3-FIFO's rule, each within its queue steps, and the chunks past the limit given back
  with the pages they hold, so its memory is within its part too; the pages dropped are counted.
  Once a turn the caches grow back to their parts of what the turn left idle, keeping their pages
  (`Cache::grow`), so operations that end and begin within a turn move no cache, and a part that
  moves by less than a page resizes none (`Engine::lend_cache`). A range on another shard
  (`range_shards = spread`) borrows nothing, its cache not on the node's shard, and a node that
  hosts no objects has no operations' memory to lend, so its caches hold nothing. With no cache
  every page a get or a one-row lookup needed was a `pread` on the node's busy shard: 100 repeated
  gets of 1,100 Name-sized rows read 100 pages and 100 lookups 100, and none with the derived cache
  (`derive::tests::a_range_opened_as_the_node_opens_it_reads_its_pages_from_its_derived_cache`);
  over random admissions, ends, reads and turns the charges and the caches' bytes stay within the
  operations' bytes, and a cache an operation's charge emptied reads 100 pages for 100 gets, grown
  back once it ended none
  (`derive::tests::the_caches_borrow_the_operations_idle_bytes_and_give_them_back_before_they_are_charged`).
  Its want is the largest, about 11 times an engine's metadata's: at an object a 4 KiB block on
  each node, five engines' caches want 2.2 times their volumes' bytes. Taken as a capacity want
  beside the index and the metadata (24f78978), it pulled them below their wants on a host holding
  less (a 16 GiB host with one 8 GiB volume: the index 469 MB of its 738 MB; a membership founder
  on CI's 7 GiB macOS runner a fifth of its index's and metadata's wants) and raised
  `memory.need` from 5.87 GB to 25.2 GB there; given a share before the operations past those
  served (f1db262e), the 16 GiB host ran one operation where it ran 13,765. Borrowing, it costs
  none. The transport's growth is its want past its floor
  (`bounds::chunked_transport`, said as `transport.want`): each share at a receive chunk, 65,527
  bytes, where its floor's share is QUIC's initial window, 12,000, below a chunk, so a node at its
  floor holds no receive chunk and copies each datagram it receives into a buffer of its own. With
  it each connection's window may grow to a chunk, which hyper-transport's auto-tuning takes as a
  whole window is read within two round trips (Chromium QUIC's
  `QuicFlowController::MaybeIncreaseMaxWindowSize`; RFC 9000 §4.2 lets a receiver raise its
  limits as data is consumed; §3.3), and each peer's pool holds a chunk for its window's unread
  data, so a datagram is cut from a chunk: the simulated cell of `tests/turn_allocs.rs`, serving
  the same writes and reads at the budget a node derives, asks fewer allocations and bytes of the
  allocator at a chunk each share than at its floor: 33,116 allocations and 16,163,710 bytes
  against 34,319 and 17,557,473 on macOS aarch64, the datagrams it carries cut from chunks, not
  copied. Every operation's windows hold a chunk, its client's connections' and its pool's, so
  each operation more costs them: on macOS a ladder founder's 32 GiB volume on a 96 GiB host of 32
  cores ran 58,965 operations where 71,522 ran at QUIC's initial window (36ad012b), the 16 GiB
  host 10,414 where 13,764, and a founder on CI's 7 GiB runner measuring four deep 27 where 31.
  On Linux each operation also holds its client's train in a receive's batch while the batch
  has room for it: a receive takes a train from each peer and client at once (recvmmsg(2) with
  `UDP_GRO`), within `UIO_MAXIOV`'s 1,024 (`bounds::io`), into a 64 KiB buffer of the
  transport's receive ring, and the plane's socket holds a buffer for it too, with their
  messages' headers: 133,520 bytes an operation more than on macOS below the batch's cap
  (`bounds::ring_growth`, hyper-rt `Io::held_bound`), so CI's founder runs 219 where macOS runs
  235 (c1589e30). Past the cap an operation adds no train on either platform, and what the
  operations hold beside their charges is counted at their count (`derive::operations`), so
  each byte charged is one the node holds: the first two run 62,502 and 10,999 on macOS and
  62,354 and 10,860 on Linux, on macOS the transport 10.04 GB and 1.77 GB past its floor, and
  every engine's metadata its want (`derive::tests`). Each operation had been charged the
  difference from one operation to two, so each past the cap was charged a train no batch holds,
  the plane socket's outbound slot, 65,592 bytes, and on Linux the ring's and the socket's
  buffers and headers too, 197,752, and each past a range's `pending_reads` its reads' state:
  the first two ran 59,412 and 10,396 on macOS and 54,591 and 9,264 on Linux (c1589e30), and
  4.42 GB and 0.61 GB on macOS, 11.10 GB and 1.63 GB on Linux, charged and never held, went to
  the engines' metadata, which no want took. CI's founder, whose 235 operations and eight peers
  make 243 trains, within the cap, runs as many as before. Where what remains past the capacity
  wants and the operations served holds less than their connections' growth, the operations are
  those served, and each share's window is past the initial one by the part over the shares
  (`bounds::limits`): CI's founder measuring 64 or 256 deep runs ten at the floor. What no
  want takes goes to the
  engines' metadata, each up to what its share of the node directory's device holds: an empty
  object, a delete marker and an upload in progress make rows that no volume bounds. An engine had
  wanted its device's whole share: on the box's 8 TB file system the five engines took 16.25 GB of
  the 16.3 GB a 16 GiB node had past its floor, for
  rows the cell never made, while each index took 33.6 MB, held 95,297 fragments and refused the
  rest (measurements/2026-10-28-transfer-scale.md). Longer keys hold more an
  extent, so the engine holds its share itself: a put that would pack its memtable while its
  metadata is at or past its share is refused, typed, as a file system with no room refuses a
  write; a delete still packs it, as one takes an unlink; and once compactions bring the metadata
  under, puts are taken again (`ShardDb::set_metadata_limit`; research/69), what it holds past its
  share bounded by its statement (`shard_db::resident_per_extent`). A share above a class's floor
  waits for its knee, where throughput stops growing as the share grows, measured at the scale
  step (§9.3): until then it has no basis.
- **Sessions are named by their owner and its run** (the node's id and its run record, §1.6),
  and a registration by its place among its run's, which rises with each: a later registration
  ends the session its owner's last one opened, whose process has ended, since a directory holds
  one process (§1.6's `LOCK`), or which the process let go holding nothing under it. The same
  registration delivered again is answered the session it opened, and an earlier one is refused,
  `Superseded`, so none delivered late ends a session opened after it whose commands may be out:
  a retry under a new session would apply one of them twice (`mantle_meta::session::register`).
  An identity the directory pins then holds at most one session a range, so `max_sessions` is the
  identities the configuration names, each once however many of its lines name it
  (`derive::nodes_named`: a rejoined founder's `seed` lines name the members its `peer` lines
  pin, and counted a line each its cell was 12 identities where its founders derive 9, its rules
  and settings not theirs, and its transport stated for seven nodes where it keeps four), and no
  session expires by time: a restart leaves none behind. A process refused `Superseded` is not its
  identity's newest, and its commands there are answered so. A client registers under its id and
  its start.
- **The floor's classes**, each the most its configuration lets it hold, beside the objects'
  (the `gateway.*` row below): a volume's buffers (`mantle_chunk::layout::most_bytes`: its writer's
  queue, two batches; the cleaner's relocation, a batch and the record that crossed it; the
  writer's two batches kept free, the batch it writes and its frame; the writer's, cleaner's,
  scrubber's and reader's reads, a batch kept free and a record read, the reader's its measured
  bytes; a checkpoint's largest frame thrice or recovery's window; each segment's state thrice),
  its index apart (`index::most_bytes`, from what memory remains); a range engine's buffers its
  configuration bounds (`mantle_engine::shard_db::bounded_bytes`: two memtables' arenas, reserved
  whole; a page buffer, a point read's page, a checkpoint page and an extent's run), each extent's
  allocator count, free or pending slot, map bit and map word apart, charged with the extent's
  resident metadata from what memory remains (`shard_db::measured_per_extent`), and its
  maintenance workers' pools, a compaction's cursors and two builders each at the range's longest
  keys and a packing's buffers for each worker and the active memtable (`derive::workers_bytes`,
  about 90 MB a worker), where the node runs them (`engine.workers`); and the threads
  the node starts (`derive::threads`: its own, which starts its shards and waits for the node's
  end, standard input's and output's, the log's two, the runtime's watchdog, a shard's for each
  CPU the process runs at once (hyper-rt `Facts::cpus_at_once`), each device's issuer, with a
  worker a read of its depth where a pool carries it (macOS, Windows;
  `Issuer::device_threads`), each volume's writer, cleaner and scrubber, each range engine's
  maintenance workers), each at
  Rust's default stack of 2 MiB, reserved whole; the runtime beside those stacks
  (`derive::runtime_bytes`: its own 867 bytes, its watchdog's; each shard, hyper-rt sizing every
  one for the largest role, the first's, or under `range_shards = spread` with an S3 listener a
  shard's past it that runs ranges, one task more, their task's launcher (`derive::shard_shape`, as
  `process::runtime` sizes them), at 11,875
  bytes, 206 a task slot, 84 a timer, 397 a readiness waiter and 16 a slot of its waiters' table,
  four slots a waiter, the bound of hyper-rt's power of two at or past twice them, so the runtime
  grows by the same bytes with each connection and the operations' count never passes the floor
  where a table doubles (CI run 44286448798: charged at the power of two, while the count charged
  each operation the difference from one to two, a joiner of three shards at 7 GiB counted a floor
  190,464 bytes past its allowance, three tables grown from 128 slots to 4,096), and 64 a control
  entry, as many as the host's
  calibration gives a shard (the `ring` basis line); the classes a shard's queue makes as a task
  takes Critical there, the first's for the node's task and under `spread` each's past it that runs
  ranges for their task (§2.9), 192 bytes and 9 a task slot; and with an S3 listener its lane to each
  shard past the first, 1,576 bytes and 88 for each connection the listener holds at once
  (`listener::tasks`). At ten shards and 256 connections served, 3.6 MB stated against 2.6 MB
  counted on macOS arm64 (3,624,377 and 2,560,629 bytes; under `spread`, every shard past the first
  running ranges, 3,649,963 and 2,586,205, 2026-11-18): a waiter's 397 holds Windows' table of its outstanding polls, 188 of
  them, derived from its sizes; a lane's 88, Windows' socket of 8 octets where a descriptor is 4);
  and each range's member
  (`derive::range_bytes`, hyper-durable's holds at the bounds hyper-raft's limits put on each:
  what it keeps of each member of its configuration; the entries not yet durable, a leader's
  uncommitted proposals or a follower's window and one entry past, at twice their bytes for the
  room mantle's encoder leaves a proposal; the entries kept ahead of a hole in its log; the
  messages that wait, in the queue and one a write out; the appends its window holds to each
  other member; the reads it is asked; its store's copies of its writes out in the log's form,
  and the page it reads the log by) and what the node holds inline for it (`node::slots_bytes`:
  its replica in its slot of each arena that holds it, 20,800 bytes on macOS arm64 whether the
  slot holds it or was emptied as it moved to a shard past the first, the arena made once at the
  ranges the node opens; and its entries in the node's tables and its shard's, at their types'
  sizes); and the transport beside its budget
  (`derive::transport_bytes_beside`: the endpoint as it opens with its TLS configurations, a slot
  of each exchange in use and of each receive chunk, and the compression of its certificate a
  server's first handshake makes; each connection's QUIC and TLS state and what QUIC keeps to
  send, at most its peer's window; the sockets, each at what hyper-rt states one holds
  (`Io::held_bound`); the plane's datagram and each peer's room and replay windows
  (`derive::plane_bytes`); and the liveness stream's estimator rings, one a peer and two of the
  node's own, each at its longest window (`derive::liveness_bytes`)). Each is pinned by a test of
  the running code (`tests/volume_memory.rs`, `tests/floor_memory.rs`, `tests/threads.rs`,
  `tests/shard_memory.rs` each runtime figure counted apart and the whole runtime and its lanes at
  1 to 10 shards, up to 256 connections and 1,024 control entries,
  `tests/range_memory.rs` over stores that hold nothing of their own, `tests/range_log_memory.rs`
  over the node's logs, `node::shard::tests::a_ranges_slots_hold_what_derive_counts` the arenas
  and tables at 0 to 4,096 ranges, `tests/transport_memory.rs` two endpoints at one, two and four peers and
  at the floor's budget and eight times it).
  *As built (2026-11-18).* What binds the three largest classes, read at the code that enforces
  each (CI run 44349745026: a founder of the membership suite, nine identities, refused at
  7,718,104,000 bytes on a three-core 7 GiB runner). **The ranges:** a member's entries are
  counted at the least an entry takes on the wire, as hyper-raft's limits count them
  (`Limits::derive`: `entries_per_message`, `unstable_entries`), for nothing bounds their count
  but their bytes. `range.max_inflight_msgs` (`derive::in_flight`, 19 at nine identities) counts
  the appends out to a follower, not their entries; each append carries up to
  `entries_per_message` of them (hyper-raft's outbox, `max_entries`); a leader's uncommitted bound
  counts bytes, and admits an entry of none whatever it holds (`Raft::admit_uncommitted`);
  `ask::ENTRIES_OUT` is what an asker keeps out of its own, which a leader does not count
  (`Node::propose_command` proposes what a peer's exchange carries, an entry of no commands among
  it); and a follower catching up is sent committed entries packed by their bytes, however they
  were proposed. **The log:** a group's retention is three `Ready`s of entries at the least a
  range proposes (`mantle_range::ready_held`), for the same reason. **The transport:** a lane's
  window of appends (`max_inflight_msgs` a range, the lane's `lane_window`, past which
  `send_frame` refuses) each at the largest message, the count and the bytes both binding. Its
  frames were charged to every peer; a range's messages go to the cell's other nodes alone
  (`Node::send` sends to a remote, a node of the cell), so a lane's window out is charged to
  those (`bounds::Stated::replicas`), and a gateway its connections and a frame a lane read in: the
  founder's transport from 1,095,918,972 bytes to 653,428,641 at 256 deep, its floor within its
  allowance with the issuer's 257 stacks (`derive.rs`'s
  `a_membership_founder_on_cis_macos_runner_starts_at_its_issuers_stacks`). Its ranges, 3.58 GB,
  and log, 2.18 GB, are unchanged: three founders on one 7 GiB runner each start, but their floors
  together pass it. What would bound a range's count, and so charge its entries at the lesser of
  that count by the largest entry and its bytes: a count hyper-raft enforces on a leader's
  uncommitted entries beside their bytes and states for what an append carries, in place of their
  bytes over the least entry; and a leader that refuses an entry of no commands, and a peer's
  entries past `ENTRIES_OUT` out. **The threads:** each stack is charged whole at Rust's 2 MiB,
  reserved, where a thread holds what it touches; a high-water mark measured for each kind of
  thread, or a stack size derived for each thread mantle spawns, would charge that. The issuer's
  workers, a read of the depth each where a pool carries the transfers, are most of them: 257 of
  269 at a depth of 256.
  **The lanes:** a peer opens the node a lane for each shard that runs its ranges (§3.2), and
  each shard past the first that runs ranges has its two lanes to the node's (§2.9); both are
  counted for the shards the node places its ranges over (`range_shards`), as the process opens
  the node with them (`NodeSettings::shards`): its first alone where unstated, so one lane a peer
  and none between shards, whatever the runtime's shards. They were counted for every runtime
  shard past the first, which runs no range where the ranges are on the first (`Node::spread`
  moves none): a founder of the repair suite's cells (five founders, nine identities) on the
  three-core 7 GiB macOS runner refused at 7,641,718,260 bytes with one volume measured 256 deep
  and at 7,610,350,228 with two measured 4 deep (release candidate bd4d1b41). Counted at one lane a
  peer, its transport's budget is the least the node's own transport takes there, 229,307,832
  bytes less, and its runtime holds no lane between shards, 71,030,400 bytes less with one volume
  and 126,418,560 with two: floors of 7,341,380,028 and 7,254,623,836 bytes (`derive.rs`'s
  `a_repair_founder_on_cis_macos_runner_starts_at_the_lanes_its_ranges_open`). Two volumes
  measured past 112 deep still pass that machine where a pool's worker carries each transfer.
  *Open:* a range's member is stated at the bounds hyper-raft puts on each queue, which mantle
  states as one `Ready` of the range's settings (`mantle_range::limits`): 52.9 MB a member at the
  floor, where three members held 11.0 MB at most through the shapes that fill each hold, and
  70.7 MB with their three logs against 425.1 MB stated. Asked of hyper-raft's owner: the core's
  and the shell's holds stated exactly as functions of their settings and pinned there; a byte
  bound on the entries kept ahead of a hole, which the core counts and only a leader's window
  bounds in bytes; each queue's bound stated apart, so an owner states its messages from its
  turns and its reads from its asks; and the log's depth exported, which mantle keeps as
  `derive::LOG_DEPTH` pinned to a live group. mantle's own encoder leaves a proposal up to twice
  its bytes of room (`mantle_codec::Writer` grows by doubling), which the statement counts and a
  length known before encoding would remove.
  *Open:* the transport's own structures are measured, where hyper-transport and hyper-quic state
  none as hyper-rt states a socket's: asked of hyper-raft's owner, an endpoint's and a
  connection's bound stated from their limits, as `Io::held_bound` is. hyper-tls compresses a
  certificate of a few hundred bytes at quality 11 with a window its message's length, 722,238
  bytes for a moment at a server's first handshake, measured at the longest certificate a node
  carries, 405 bytes, each field at its longest as `mantle init` issues it
  (`CERTIFICATE_COMPRESSION_BYTES`; 34.3 MB with brotli's 4 MiB window before); an endpoint and a
  connection are measured at it as well (`ENDPOINT_BYTES`, `CONNECTION_BYTES`). hyper-quic took
  QUIC datagrams, which no consumer sends, up to 1,250,000 bytes a connection by default; since
  hyper-raft c97fb1a (branch doors-step8) no endpoint offers to take any. aws-lc keeps keys in its
  own heap, which a counting allocator does not see.
  *Open:* the snapshot path holds whole images, the one class the floor does not bound yet: a
  range's image is every row scanned and copied (`Stored::image`), encoded again for its message,
  kept by the shell and by the core's log until it is installed, framed and copied again by the
  node to send, and taken whole by a receiver that sizes its buffer to the length its sender
  declares; installing decodes every row, and clearing a range scans every old one.
  An engine's builders, cursors and pools hold what is out at once, which its trunk bounds by its
  fanout and its height (engine-structure.md, Compaction, Flush and Bounds), and each kind's bytes
  are stated and pinned by counting-allocator tests at the range's densest rows and its longest
  keys: a builder at most its levels' pages (a page being filled reserves one payload, keys staged
  past what they share with the page's first, `branch::page_bytes`), its key buffers, payload, run,
  filter and lists, and its two tries, at most a level and a label a key byte
  (`branch::builder_bytes`, `fst::trie::builder_bytes`); a cursor an extent, four pages and its
  reads in flight (`branch::cursor_bytes`); at most `TrunkConfig::cursors_most` cursors and
  `builders_most` builders at once, each building a branch of at most `built_most` entries, a leaf's
  limit, stated together by `shard_db::pooled_bytes`; the branches' resident metadata, each
  branch's filters, tries, page counts, extents' numbers and record, at most the extents they hold
  times an extent's statement (`shard_db::resident_per_extent`: at the floor 63,698 bytes an extent
  at 8-byte keys and 87,430 at 2,132, the range filter's labels bounded by its keys' rises and those
  by the suffixes its leaves store, its nodes by its rises; research/68 §2); and the trunk's own
  records, its nodes, pivots and lists, as held (`ShardDb::memory`: buffers' capacities and
  records, exact against the counting allocator). Before the trunk bounded them, a node held 1,468 live
  in-flight bundles at 2,132-byte keys and scans opened each: 189.7 MB held for 32,000 rows. A node
  now takes a flush only below `fanout` live bundles a pivot, a compaction takes its inputs within a
  leaf's limit, the shard steps two compactions at once, and a read's path meets one part of each
  packing in flight; the same fills held 12.9 MB at 8-byte keys for 1.28 million rows (pools' peaks
  37 extents, 108 pages and four builders' lists, against 221 cursors and 5 builders stated at
  height 5) and 11.4 MB at 2,132-byte keys for 32,000 rows (39 extents and 114 pages, against 153
  cursors at height 3), each within its configured bytes, its pools and builders as stated, its
  branches' metadata as stated (5.81 MB held of 85.7 MB stated for 1,346 extents at 8-byte keys,
  4.3 KB an extent; 264 KB of 2.7 MB for 31 at 2,132-byte keys) and the trunk's records as held
  (`tests/engine_memory.rs`). At 2,132-byte keys that statement is 255 MB, its
  builders' tries counted by their depths (2.7 GB while they counted a label a key byte). Resident
  metadata per extent of data: 21% at 8-byte keys without values (1.25 bytes of filter and 2.3 of
  range filter a key), 3% at 16-byte keys with 100-byte values. Keys in pairs sharing all but a
  last byte, each pair parting from the one before at its first byte, made the range filter and
  the leaf index a third of the data at 2,132-byte keys, their labels bounded only by the key
  bytes a branch stores. They are bounded below the data now: a range filter's entries run at most
  16 bytes past the bytes every key of their branch shares and a leaf index's separators at most
  256, the leaves past that found through the branch's index pages, and a level costs its labels'
  bits; those pairs' two tries hold 1.15% of their data, and a branch's tries are stated from its
  key bound, entries and leaves (`branch::tries_bytes`; research/67). A compaction's point filter,
  made for its inputs' entries and halved only while its blocks were even, kept the bound's bits
  when its merge cut its keys: 40 bits a key at an eighth of its inputs, 312 KB for ten keys left
  of two million. It is made at the least count of blocks whose divisors each at most double the
  one before and folds exactly, once its keys are counted, to fewer than twice the blocks they
  need (research/68 §1).
  And `ShardDb::follow_machine` budgets a range's cache, writes and records from what remains of the
  machine: the budget follows one of `shares` of the machine's room (`port::memory::room_now`: on
  Linux `MemAvailable` less the larger of the kernel's background dirty threshold and its dirty and
  writeback pages, and less the process's own swap), looked at each rotation, each get cycle and
  once a cycle's worth of scans and merged entries has passed, or as an idle shard's workers report;
  it is capped at its share of the capacity less the range's own memtables and branch metadata, and
  moves by a tuning step, or at once when the machine needs memory back. Its tuner leaves the
  kernel's page cache, which the workers read their inputs through and which absorbs the store's
  rewrites, a share earned by the workers' waits on reads the OS did not hold, and the cache keeps
  its pages on huge pages (`port::mmap::PageBuf`; docs/research/65 §7-§9). Machine-sized, a billion
  rows completes on the VM at 64 GiB and at 96 GiB on the final build, recovery exact (at 64 GiB
  3.49 million puts, 18,174 gets and 10,207 seeks a second); at 96 GiB its gets and seeks 1.24 and
  1.26 of a fixed 32 GiB cache's and its fill 0.85, the same build at that fixed cache 1.06, 1.34
  and 1.13 of data-layer's (docs/research/65 §9). A node's ranges take no such budget: each
  range's engine holds the page cache its share derives (`engine.cache_bytes`, above) and no
  record cache. *Open:* kept memtables take the
  budget's idle part before any read misses, and with the memory moved between the regions cost a
  billion rows' fill 15% against a fixed cache (§9); and hyper-rt reads no process's resident memory
  on Windows, so a shard there leaves nothing free for what grows outside its account (§9). And the
  trunk's own records are counted as held, not stated: a node's pivots grow past the fanout while
  its split waits for its parent's pivot to empty, and nodes never merge, so their count follows
  the splits a range has seen, not the data it holds (research/68 §3).

*As built (2026-10-23).* `mantle serve CONFIG` reads a file of the first column's keys alone, each
once (`id`, `dir`, `quic`, `plane`, `authority`, `certificate`, `key`, `voters`, `domain`,
`root.max_nodes`, `root.max_volumes`, and, where stated, `memory`, the ceiling,
`durability.target` and `range_shards`, the shards its ranges take, `first` or `spread`, §2.9) and
its repeated `peer`, `range`, `root_key` and `volume = DIR SIZE` lines; a
key it does not know, a derived one among them, is refused at its line, as `mantle node` now
refuses one it does not know (`process::STATED_KEYS`, `process::KEYS`). It takes each volume's
device directory's `LOCK` first (§1.7 step 1; `storage::Locks`, handed to the volumes as they open),
then measures: the host's memory, the OS's limit and its cores and page (hyper-rt `Facts`); the node
directory's device, its unit and its file system's bytes (`mantle_disk::probe::identify`); each
data device's profile, kept in its `mantle/profile` and taken while the device's identity still
matches it, else measured now (`Plan::standard`) and kept (`mantle_disk::profile`, §1.6); and each
volume's layout once laid out (`mantle_chunk::formatted`). `derive::derive` assembles every value
`mantle node`'s configuration names from those, by the rules above and the table below, and each
range's store as it stands, read at the page a new store would take or the one its superblock names
(`Store::shape`): a store keeps the page and extent sizes it was made with, and the engine admits
the extents it holds however few its share now holds, since a store opens only at its sizes and
admitting its extents (`Store::open`). The node says each value on standard output before anything
opens, `basis KEY = VALUE (KIND: WHY)`, its kind one of stated, measured, derived, cited and bound,
a volume's under `volume.N.` (N its place among the `volume` lines from 1) and the floor's under
`memory.`, then runs as `mantle node` does. `derive::derive`'s values written as `mantle node`'s file
read back as exactly what the node runs with (`derive::tests`), and a cell of three `mantle serve`
processes from stated keys alone says every key's basis, enlists, serves objects, and, a node
killed and started again, takes its kept profile and its stores' and volume's shapes and says the
same values (`crates/mantle/tests/serve.rs`, a binary of its own: a process states one set of
message bounds, `classes::state_bounds`, and these nodes derive theirs). The scheme a gateway host
chooses is the durability model's under the rates `derive` sized its asks by, the yearly power-on
restart among them (`mantle node`'s `durability.burst` lines, which it took as none before).
*Open:* a range's engine holds its share of memory itself only alone (`ShardDb::set_metadata_limit`):
a range's members apply the same entries, and a refusal that turns on each member's own metadata,
which its compactions' progress moves, would let them diverge, so a replicated range must refuse a
put where its leader admits a command, before the entry is proposed. Until then a node's engines
are sized by their shares (`derive::engine_within`) and take no limit.

| Value (`mantle node`'s key) | Kind | Model and source |
|---|---|---|
| `id`, `certificate`, `key`, `authority` | stated | written by `mantle init` (§1.5); an id is 64 bits from the OS, as hyper-transport's `PeerId` holds, where §1.5 names 128 |
| `dir` | stated | the node's directory (§1.6): where `mantle init` writes its `config`, `identity/`, `keys/` and hyper-rt's calibration record, and where the node keeps its log, its run record and each range's engine store, under its own `LOCK` |
| `quic`, `plane`, `domain`, `peer` lines | stated | the operator's addresses and failure domain; each peer by the certificate its authority issued and where it listens, a name resolved at the start and again when its link ends |
| `voters`, `range` lines, `root_key` lines | stated | the cell's founders and founding groups, drawn with its authority, and the root key's generations (§1.5; encryption.md §2) |
| `root.max_nodes`, `root.max_volumes` | stated | the cell's bounds (architecture §11) |
| `volume` directory and bytes | stated | the devices that hold data |
| `volume` depth, read bytes, read gap, first-write penalty | measured | the device's calibration (chunk-store.md §7), kept in its `profile` (§1.6) and measured again when the device no longer matches it (§1.7) |
| `volume` depth taken | derived | the transfers the node keeps in flight on the device: the depth measured, or the deepest the allowance holds where less (CLAUDE.md §5, the device and the machine both), every class that holds a transfer at it: the issuer's workers where a pool carries the transfers, the volumes' reads and transfers, the transport's room for their exchanges and the ranges' for their reads; refused only where one at a time is past the allowance. `memory.need` stays at the deepest the queue lets calibration measure. A founder of the repair suite's cell on CI's 7 GiB macOS runner measured 240 deep was refused at its threads, 7,831,251,011 bytes (gates run 44596668868), and took 109 while each issuer worker counted Rust's 2 MiB stack; spawned at the 128 KiB hyper-block sizes them at by measuring their paths (`WORKER_STACK`, `derive::stacks`), its floor at 240 is 7,359,391,811 bytes and it takes the 240 measured (`derive::deepest_held`) |
| `engine.page_size`, the log's block | measured, derived | the device's write unit (hyper-block's `layout_block`), at least `MIN_PAGE`; the engine's page at least the least whose branch pages hold a row at the range's bounds (`mantle_engine::branch::least_page`, replica.md §2) |
| `chunk_bytes` | cited | `mantle_gateway::layout::CHUNK` |
| `durability.*` | cited, stated | the worst device class's annual failure rate an hour, since a block's chunks land on any device of the cell (`mantle_ec::durability`'s disk rate), with durability.md §5's yearly power-on restart taking a hundredth of the nodes; repair at the rate the cell states (`durability.repair_rate`, `mantle init --repair-rate`), else research 15's day until repair measures its own; the target S3's 10⁻¹¹ unless stated (`derive::rates_repaired`, `derive::target`) |
| `range.max_entry_bytes` | derived | `mantle_meta::wire::largest_entry_bytes` (replica.md §7) |
| `range.election_tick`, `range.heartbeat_tick` | cited | focal's 10 and 2 (§2.4), checked though ranges elect by suspicion, which takes no ticks |
| `range.max_size_per_msg`, `range.max_uncommitted_size`, `range.max_committed_size_per_ready`, `range.max_inflight_msgs` | derived | an entry of the range's largest each append, apply and `Ready`; as many appends out to a follower and entries uncommitted as a range can hold, `ask::ENTRIES_OUT` an identity of the cell, each asking through as many entries out at once (`ask::Asking`), and one of the leader's own, its term's first entry or a change of configuration (`derive::in_flight`; Ongaro 2014 §3.6.2, §4): no asked entry waits unsent for a follower's round trip and flush, where two out had held each frame to a confirmation of its own and one made every frame wait (raft-log.md §6; research/71, step 9) |
| `rules.*` | derived | the same on every member, from the cell's identities and the metadata layer's bounds alone (`derive::cell`): a session of each identity for good, as above; an entry carrying every command waiting that fits its bytes, so every command the node's operations wait on shares the next entry's flush (§2.8; replica.md §1; research/71, step 9: one command an entry committed one a Raft round, the cell's writes capped at 20 to 47 a second); their answers kept until the gateway acknowledges them, as many as a session's row holds within the largest row S3 admits, so no page grows, and one at the largest a node's command gets where it keeps no other (`wire::largest_answer_bytes`: a range's lineage at the longest route keys; a sweep sizes its checks to it), a command past them refused before it takes effect and asked again (research/87); a row's key and value at most the longest key S3's names give a row (`key::largest_bytes`) and the largest value a request S3 admits, or a session at its bounds, gives one (`wire::largest_row_bytes`: a version keeping S3's 8 KB of headers at their longest encoding), which every member's engine holds (replica.md §2); the node refuses to start on an engine that does not (`mantle_range::holds_rows`) |
| `log.*` | derived | `hyper_log::Config::derive` from the log's block, its largest entry with the shell's bytes, its groups (the ranges and the liveness stream's), a checkpoint's cadence and what a group holds uncommitted past it of one `Ready` each (`mantle_range::ready_held`: as many entries as its bytes make of the least entry a range proposes, 13 bytes, and the term's empty first one, and its bytes with each entry's 5-byte shell), a cache of one largest entry a group, and a file holding every group's retention at once beside the least file the log runs in: a `Ready` goes in greedy parts of a frame's room, any two in a row more than a room, each a segment at most (`derive::log`). Before, entries were counted at the shell's 5 bytes, 2.6 times too many, and the file was the least, which one group's retention filled: a write past it is refused `Full`. Its memory: each group's slots, a deque doubled past its retained entries at hyper-log's 56 bytes a slot, its handle's unapplied entries and its cache, one group's slots again while they grow, and ten segments of the log's own buffers (`derive::log_bytes`; `tests/log_memory.rs`: every group at its retention, 112.6 MB at the peak of a 225.9 MB bound) |
| `log.waits` | cited | `measured`: the log's writer waits for the replicas it just answered while that is expected to lower total latency, ended by the clock or a frame's worth of charged bytes, what a node runs (raft-log.md §3, hyper-log's `Waits::Measured`); `never`, a batch of what is queued when the writer looks, is the replica simulation's, whose members have one update out at a time |
| `engine.extent_pages`, `engine.max_extents` | derived | the least pages the store takes an extent, two; as many extents as each range's share of the metadata device's space holds (`derive::engine`) |
| `engine.cache_bytes` | derived | the most each range's engine's page cache holds, its want: every page of the extents its rows of `engine.objects` take (`derive::engine_cache_wants_for`), at most its store's pages. No share of the split: each engine opens with no cache (`process::engine`) and borrows its part of the operations' bytes no running operation holds, giving it back at once before one is charged it (`admit::cache_part`, `host::lend`, `Engine::lend_cache`; rule B for the caches, research/87 §4) |
| `engine.memtable_bytes`, `engine.fanout`, `engine.leaf_entries` | derived, cited | the least write buffer, one row at the range's bounds whole (`memtable::hashed::entry_bytes`); SplinterDB's fanout of 8, at which the engine's measurements ran (engine-structure.md §5–§6), until the fanout is measured against mantle's key counts as its §4 asks; a leaf holding every row one memtable gives it, the most a memtable holds being rows of the shortest key and no value (`key::least_bytes`; `benches/ranges.rs` sizes a leaf so) |
| `engine.workers`, `memory.need_workers` | derived, measured | the maintenance workers each range's engine packs and compacts on, so an apply on the node's shard writes none of their pages: one, the least a pool holds, Little's law's need for all five ranges at once at the measured load (the leader's shard 31% busy packing and compacting inline at 4,000 to 6,000 small PUTs a second, macOS `sample`, 2026-11-19; `derive::ENGINE_WORKERS`). Their stacks are among the floor's threads and their pools among its engines (`derive::workers_bytes`); `memory.need_workers` is `memory.need` with them, and a node whose allowance holds less runs none (`derive::derive`): a founder on CI's 7 GiB macOS runner holds no five past its need. `memory.need` stays the need without them, at which every want is whole |
| `transport_bytes` | derived | the transport's floor (`transport.floor`): what it holds besides QUIC's windows, the connections' state at the most connections it holds (`bounds::connections_state`) and the receive ring's growth at its most (`bounds::ring_growth`) among it, and each share at QUIC's initial window, below which its limits refuse (`bounds::least_transport`); and past it its want whole (`transport.want`, each share at a receive chunk, `bounds::chunked_transport`) where what remains past the capacity wants and the operations the node serves holds those operations' connections at a chunk, every operation more costing its client's windows at a chunk; short of it, the operations are those served and its budget is its floor and what remains, each share's window past the initial window by it over the shares (`bounds::limits`) |
| `volume.*` layout | derived | checksum blocks of the volume's block, at most CRC-32C's verified 16 KiB, so a read at the device's alignment verifies no byte it did not ask for (research 11 §8.2); the least segment holding a record of the largest chunk whole, smaller segments cleaning better (research 11 §7.3–§7.4); the standard layout, or the compact one where the standard leaves fewer than two segments (chunk-store.md §2); batches of every chunk waiting that a segment's room holds at a block each, within the records a batch's frame holds (`layout::most_batch_requests`), so they share one flush where batches of one flushed each chunk alone (research/71, step 9; research/87 §1), a segment's room in bytes, and one fragment a chunk, as the node puts chunks whole; as many fragments as the volume's share of memory holds at the index's measured peak (`index::most_bytes`) and the volume holds at a block each, the least a fragment takes standing for the chunk sizes research 11 §6.2 learns from; a volume laid out already keeps its block, segment and checksum blocks, and holds no more fragments than its log was laid out for (`Geometry::holds`); research 03 §15.9's week a scrub (`derive::volume`) |
| `gateway.window`, `gateway.objects`, `chunk_asks`, `command_asks`, `read_asks` | derived, measured | a window of one block, the floor, the least a put or a get holds and still moves (§2.6's `⌈R·T/B⌉` at its least), a put and a get each holding the window the node learns of its own puts and gets, and a copy its put's that of the node's puts and its get's as many blocks as that put drains while one is read, where the operations' bytes hold it (§2.8, `crate::window`); the operations' bytes, what the memory holds past every class's floor and the indexes' and engines' wants (`gateway.objects_bytes`; research/87 §4 rule D), each operation charged what it holds at its own size (`derive::Charges`), and a ceiling of as many operations as those bytes hold at the least one holds (`gateway.objects`), within which the host admits as many as it learns its knee is (§2.8, `crate::admit`); the asks the operations hold at once, each at its most under the schemes a block may take in a cell of the domains the volume table may name, the durability model's choice at each count of them (`derive::schemes`): a put's chunks at its width, or a get's spans and the whole chunks a failed span adds, twice its data less one, past the width of a code whose parity is less than its data less one (`put::most_chunks`, `get::most_chunks`); a put's command for each block of the largest body S3 takes, its renewals, and its file's write beside its blocks' (`put::most_commands`; gateway.md §2.2); one read each; and the node's enlistment (`derive::asks`). The memory an operation holds at its most: the larger of a put and a get of the schemes' largest blocks, the drivers' own (`put::most_bytes`: the segment, the block filling, each block going down with a code's parity and padded chunk, or a block coded with nothing filling beside it; `get::most_bytes`: the spans read, the whole chunks read in a failed span's place, the decoding, the cut bytes and the plaintext opened from them) and the host's, a put's chunks copied to send and a piece of body carried, a get's block taken; the coding's bytes are reed-solomon-simd's work, measured with the call's own (`Code::encode_bytes`, `Code::decode_bytes`), and its tables once a process (`mantle_ec::TABLE_BYTES`) (`derive::objects`) |
| `descriptors.limit`, `descriptors.fixed`, `descriptors.each`, `descriptors.operations` | measured, derived, bound | the process's descriptor limit, raised at its start and read back (`crate::descriptors`); what the node holds whatever its operations, and what each holds beside (`derive::fixed_descriptors`, `derive::operation_descriptors`); the operations the rest holds, `bound` where it bounds `gateway.objects` (the **Descriptors** paragraph above) |
| `memory.native` | derived | the most one native PUT or GET holds above the node's idle at `gateway.window`, the bound a body crossing the client, the host and the transport is held to (lean-doors step 5): the most an operation is charged (`derive::Charges::most`: its window of blocks at the largest block, `put::most_bytes` or `get::most_bytes`; a put's chunks copied to send for each block going down, one piece of its body, hyper-transport's `RECEIVE_CHUNK`, and its record of each block it wrote until its file names them, at the largest body a PUT or a part carries, `MAX_UPLOAD`, 5 GiB (`put::most_records`); a get's block taken), what one more operation adds beside its charge to the transport, the ranges, the listener and the runtime (the derivation's difference of the node at two operations and at one), and the host's records of it, its job and its boxed put or get (`node::record_bytes`) (`derive::native`). A put or a get admitted at a window it learned holds that window's blocks instead (`Charges::write`, `Charges::read`), at most `gateway.objects_bytes`; a get is admitted at the floor's window and takes the bytes for the window it learned once its version is read, before it holds those blocks; a copy holds its get's and its put's at once (`Charges::copy`), admitted at the floor's and fitted the same way once its source's version is read. A put's records (`mantle_gateway::put`'s `recorded` and `due`) grow with its body by design, a block's entry in each of two std B-trees: at most 212 bytes a block, each tree's node over the least entries every node but its root holds (`put::records_bytes`), and 1,056 for the roots; 138,008 at 5 GiB in blocks of one copy's 8,325,104 bytes, 646 of them. A put is charged its own body's (`put::records_for`, in `Charges::write`), so a node admits no more puts than it holds records for. Counted on macOS aarch64, about 146 bytes a block (`object_path.rs`'s `a_put_and_a_get_many_windows_long_hold_their_windows_bytes`, which holds each block more to the 212) |
| `gateway.bucket_quota`, `gateway.scan_rows` | cited, derived | S3's 10,000 (research 05 §10.4); the rows each step of a bucket's attempt passes or removes (`coordinator::Bounds`): as many as the largest entry a range takes (`wire::largest_entry_bytes`, `range.max_entry_bytes`) holds of what one row a collection removes writes, its key deleted at the longest a row's key is and an upload's part's file released, the queue's key and its holder (`wire::largest_collected_bytes`), so a collection's writes are within the bytes of the largest entry, and a probe passes as many rows a read (`derive::scan_rows`) |
| `gateway.handover_ns` | cited | HDFS's 60 s soft limit (research 22 §3.6), one of the bounds metadata.md §2 names for the same window, until the gateway measures research 22 §10.2's `D_b`: its retry budget for a step times a high quantile of one try, plus the clocks' offset; it bounds how long a leftover waits, not correctness |
| `runtime.probe_budget_ms` | bound | hyper-rt's probes stop at Wilks' 59 samples (hyper-rt runtime.md §10.2), measured once by `mantle init` and kept as hyper-rt's record, reused while the machine's key matches; the budget bounds a starved host only: 59 wakes at the platform's quantum, 3 ms on Linux, 10 ms on macOS, 93.75 ms on Windows (research 64; `derive::probe_budget`) |

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

*As built (2026-10-21).* `mantle node` hosts the ranges its configuration's `range = GROUP LAYER`
lines name, at most one of each layer, since each of a cell's first ranges spans its layer's whole
key space; each keeps its rows in an engine store of its own (`range-GROUP.store`) and shares the
node's one log, and the node says each one's status by its group. A cell of three nodes hosting a
Bucket, a Name, a File and a Block range on every node, voters one to a node, elects in each,
applies each layer's commands only in its own range (`WrongLayer` elsewhere), and serves on through
the kill (`SIGKILL`) of the node leading the most of them (`crates/mantle/tests/node.rs`). The
cell's root range holds its volume table (§6.6); the rest of its rows, the region's root range,
and `mantle init` making them are still to build.

*Planned for `mantle init` (2026-10-21).* `mantle init DIR` makes a cell of one in `DIR`: the
cell's authority (`mantle_cert::authority`, its key owner-only), the root key's first generation (32
bytes from the OS, owner-only), the founding ranges' groups (one each for the root range and the
Bucket, Name, File and Block layers, drawn from the OS), the node's identity (a key and the
certificate the authority signs it, `mantle_cert::node`), its devices measured into their
profiles (§1.6), and its configuration of stated values (§1.4). `mantle init cell OUT --nodes N`
makes a cell's founders at once, since each must pin every other's certificate before any serves
(§3.6's directory holds a peer by the certificate its authority issued): the authority and the root
key in `OUT/authority`, which the operator keeps, and a node directory for each founder naming the
founders as the founding ranges' voters, one to a node, with each founder's certificate and where
it listens; the operator moves each directory to its node. Clients of the native protocol are made
by the same authority (`mantle init client`), and the founders pin those the operator names.
Joining by invitation (below) replaces the static founders once the root range keeps the cell's
membership.

*As built (2026-10-24).* One command makes either, from the founders' statements given as repeated
`--node quic=ADDR,plane=ADDR,domain=NAME,volume=DIR:SIZE,memory=SIZE` flags, a machines file
(`--founders FILE`: the cell's `max_nodes`, `max_volumes` and `durability.target`, then a `[node]`
block a founder, a volume `volume = DIR SIZE`), or both, the file's founders first, as the owner chose
on 2026-10-24 (`mantle init OUT ...`; `crates/mantle/src/init.rs`). The cell's bounds on its volume
table are stated (`--max-nodes`, `--max-volumes`, or the file's), since architecture §11 leaves
their values open, and are required where a founder keeps volumes. One founder makes a cell of one
in `OUT`, its directory, which keeps the authority's key under `identity/` as a laptop's does; several
make `OUT/authority` (the authority's certificate and key and the root key) and `OUT/node-ID` for each
founder. Every id and group is drawn from the OS, nonzero and distinct, refused otherwise; each
certificate takes RFC 5280 §4.1.2.5's notAfter for no well-defined expiration until certificates are
rotated. Each founder's `config` holds its stated keys alone (§1.4), every path relative to its
directory, which `mantle serve` and `mantle node` read from the configuration's own directory, so a
directory moves to its host whole; `identity/` holds the authority's certificate, the founder's own
and its key, and each other founder's certificate, pinned by a `peer` line; `keys/1` the root key.
Keys are created owner-only and kept so: mode 0600 on Unix, and on Windows a protected DACL granting
the process token's owner and LocalSystem alone, as hyper-seal's check admits, each written whole
under a temporary name, flushed, renamed and its directory flushed, and checked owner-only after,
refused on a file system that does not keep it so (`crates/mantle/src/files.rs`). A cell of one is
measured now: the machine's calibration record in its directory, which `mantle serve` takes again
while the machine's key matches it (`process::calibration`), and each volume's device into its
profile under the device directory's lock; founders of a larger cell are measured at their first
`mantle serve` on their own hosts. Tested by serving from what it writes: a cell of one from one
`--node`, and a cell of three from a machines file's two founders and a `--node`'s, each enlisting
and serving objects (`crates/mantle/tests/serve.rs`). Native clients are made by `mantle init client`
and pinned by no node (below, 2026-10-26).

*As built (2026-10-25): the S3 endpoint's key and certificates.* `mantle init` gives the cell its
first account, twelve digits, and its one access key, an id of twenty characters of RFC 4648's
base32 alphabet and a secret of 240 bits as forty characters of base64, drawn from the OS
(research/84). The key is written on every founder, owner-only, as `keys/s3/ID` holding its account
and secret, and named by its `access_key = ID FILE` line; `mantle serve` reads each at its identity
step only from a file no one but its owner may read, refusing the start otherwise as it refuses the
node's own key, and says `s3 LISTEN REGION IDS` once it has. The endpoint's chain is ECDSA P-256
throughout, since the AWS CLI's transfer client and Windows' TLS verify no Ed25519 certificate
(research/84): the endpoint has an authority of its own beside the cell's, kept with it
(`s3-authority.der`, `s3-authority.key`), which issues each founder's endpoint its certificate and
key (`identity/s3.der`, `identity/s3.key`) for the DNS names its operator states (`name=HOST`), its
address unless it listens on every one, and `localhost` on a loopback address; serverAuth alone, the
key identifiers RFC 5280 asks, and 825 days of validity at most, issuing it again before then still
to build (`mantle_cert::endpoint`). The endpoint listens where the founder's `s3=ADDR` says, else
over TCP at its QUIC address, a stated port of 0 taking a TCP port of its own, and serves the
cell's region (`--region`, or `region` in the machines file, else S3's us-east-1); its stated names
are its config's `s3.name` lines, which its requests' hosts name buckets under (§4.2), and once
bound it says `s3-bound ADDR CONNECTIONS`. For the operator, `OUT/s3` holds the key as AWS's shared credentials
file, owner-only; the endpoint's authority as PEM; and, where a founder's endpoint has a port and a
name init can state, the config of the profile `mantle`: its region, that endpoint's URL, plain HTTP
on a loopback address and TLS elsewhere, the PEM as its `ca_bundle`, and path-style addressing.
Tested in `crates/cert` (each stated name and no other verifies, the extensions a strict verifier
asks, the validity's bound, the PEM, and the cell's own certificates the bytes they were), in
`init`'s tests (every founder's key the profile's, a request signed with it verified by every
founder and one signed with another secret refused `SignatureDoesNotMatch`, an exposed key refused,
every endpoint's certificate verifying under the profile's PEM for each name its clients dial), and
in `tests/serve.rs` (each node says the profile's key; an exposed one refuses the start at its
identity step).

*As built (2026-10-26): clients of the native protocol.* `mantle init` keeps beside the authority's
key a `cell` file of what a client needs: the authority's name, the account its clients act for
(the cell's access keys' account) and each founder's id and QUIC address. `mantle init client NAME
--cell OUT` draws the client's id from the OS, nonzero and no founder's, issues its certificate
under the authority (`mantle_cert::client`: its subject's common name `client-ID`, its organization
the account, `extKeyUsage` clientAuth alone, §3.6), checks it issued by that authority as a start
checks a node's, and writes NAME: `identity/` (the authority's certificate, the client's and its
key, owner-only as a node's) and `config` (its id, those files and a `node = ID ADDR` line for each
founder), which `mantle`'s object commands read (gateway.md §6). No node's configuration names a
client: a `peer` line naming one is refused at its line. Tested by a cell of three `mantle init`
made, whose configurations pin their nodes alone, served by a client it made, through the library
and the command line (`crates/mantle/tests/native.rs`).

*As built (2026-11-03): founders behind names, a gateway pinned, a founder carried whole.* A
founder may state its reach (`reach=HOST`, an IP address or a DNS name), the host its peers and
native clients dial on its QUIC and plane ports where it differs from the address it binds, as a
Kubernetes pod's own name under its StatefulSet's governing Service (kubernetes.md §2; research/78):
its peers' `peer` lines and the cell file's `node` lines name that host, a node resolves a peer
line's names at its start and again as it runs (§1.7), and a client resolves its `node` lines' when
it is made. `mantle init gateway NAME --cell
OUT` issues a gateway's certificate under the authority and pins it in each founder's
configuration (`peer = ID gateway identity/peers/gateway-ID.der`, its certificate beside the
founder's peers'), before any founder starts. `mantle pack DIR FILE` carries a directory as one
checksummed file, its keys marked, and `mantle install FILE TO` writes it back, each key owner-only
and the configuration last, keeping a directory that holds a configuration already
(kubernetes.md §3). Tested in `init`'s and `pack`'s tests (a founder reached at its name on its own
ports; a founder packed and installed whole, its keys owner-only, its gateway pinned, a changed byte
or a path out of its directory refused, a second install keeping the first).

**Joining.** An existing node mints an invitation (`mantle invite`): a one-time token recorded
in the root range, so the log decides whether it was used. `mantle init --join` sends the token
and a certificate request over the enrollment protocol, whose key the joining node keeps; the
authority signs a certificate whose subject it chooses itself, as focal's enrollment does
(07 §4.1). The new node is recorded in the root range with its failure-domain labels and
devices. It then hosts replicas as placement adds them: each as a learner, caught up, and made
a voter by one joint change (replica.md §6), until every range has `2f + 1` voters one to a
domain (§6.5). Until the cell has the domains `f` needs, the node reports the failures it can
survive, and placement uses the domains there are.

*Superseded (2026-11-14): joining by a grant* (docs/design/membership.md). No node holds the
authority's key: its holder, the operator or a supervisor like kiln, grants a node offline
(`mantle init grant`), a certificate naming `node-ID` and a grant numbered past the authority's
last, which the root range spends once in a window as wide as the cell's bound on nodes (RFC 4303
§3.4.3's anti-replay window), so a removed node's certificate, which never expires, needs no list
to revoke it (RFC 5280 §3.3). The granted node dials the members its grant names, asks the root
range to `Join`, and each range's leader adds it as a learner and makes it a voter once caught up,
one server at a time (Raft dissertation §4.1, §4.2.1). Leaving in order, judging a member gone,
removing it and the seam with repair are membership.md's.

**Restart** needs nothing but the node's own directories. It reads its membership, its peers'
addresses and the last cell map it persisted, and serves from them while the control plane is
down (architecture §4, §9).

Where the authority's private key lives once a cell has many nodes is open (§11); on a laptop it
is in the node's directory.

### 1.6 Directories

```
<node directory>/                     not on a data device (encryption.md §2)
  config                              what the operator stated (§1.4)
  identity/                           node key, certificate, the cell authority's certificate;
                                      the S3 endpoint's key and certificate (§1.5)
  keys/                               root-key file, owner-only, one per generation; s3/, the
                                      cell's access keys, owner-only, one a file
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

*As built, and planned (2026-10-21; 2026-10-23).* The node's directory holds its metadata today: the
shared log (`log`), its run record (`run`, §2.4 item 4), its readiness lock (`live`: taken
exclusively as the cell's root range enlists the node in its run and held until the process ends,
which releases it however it ends; `mantle status` asks for it shared without waiting, so a check
finds it held only while an enlisted process lives; kubernetes.md §5) and each range's engine store
(`range-GROUP.store`), and each volume's device directory holds `mantle/LOCK`, `mantle/volume` and,
under `mantle serve`, `mantle/profile`: the depth, read bytes, read gap and first-write penalty its
calibration measured, keyed by the identity facts that decide how the device performs (its model,
medium and interconnect, its blocks and transfers, its queue, write cache, FUA, zones, removability
and size, its file system's kind, block and size, and each member's), a hyper-block record written
whole, flushed and checked by its CRC-32C on read; one of another key, another layout version or
that does not read is measured again and replaced (`mantle_disk::profile`). The OS's names for
devices, which a restart may change, are not in the key, and firmware is not yet, since no probe
reads it. `mantle init` adds to the node's directory its `config`, `identity/` (the node's key and
certificate, the authority's certificate, each pinned peer's certificate under `peers/`, and the S3
endpoint's key and certificate), `keys/` (one root-key file a generation, and `s3/`, the cell's
access keys) and, for a cell of one, `calibration`, hyper-rt's record, which `mantle serve` and
`mantle node` keep there too and take again while the machine's key matches it. The directory's own
`LOCK`, held as the volumes' are, since its log and stores admit one process as a volume does, is
taken at the start's locks step and held while the process runs (2026-10-24). `meta/` on a metadata
device, `DEVICE` and `map` wait for the storage graph (§1.7 step 3) and the cell map (architecture
§4).

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

*Planned for `mantle serve` (2026-10-21; research 64).* **A start that fails** exits non-zero with
a typed error naming the step and what failed in it, not a sysexits(3) code, which its own manual
deprecates: the configuration (no such file, a line that does not parse, a key it does not know,
a value out of its bounds), the identity (a file missing, a key that is not PKCS #8, a certificate
its configured authority did not sign), a root key (not 32 bytes, readable by others than its
owner), a directory or device another process holds (`LOCK`), a device whose profile no longer
matches and cannot be measured again, the floor past the memory allowed (§1.4, naming the class),
an address in use, and a store, log or volume that does not recover (naming its file). **An
orderly stop** begins at SIGTERM or SIGINT (hyper-rt's signals; Ctrl-C, a console's close and
shutdown on Windows), as a container's stop sends TERM to its first process and KILL once the grace
period, 30 s by default, passes (research 64): the node answers new object exchanges `SlowDown`;
lets those in flight end within the stop's bound; transfers each range it leads to a voter whose
log matches its own (Raft §3.10, hyper-durable's `Replica::transfer`), and waits until that voter
shows it leads, or for an election timeout, after which the range elects as after a crash; closes
its connections and answers their late packets for three probe timeouts (RFC 9000 §10.2: a node
keeps the socket its peers know); then closes its logs and volumes and exits 0. The stop's bound
is stated beside the orchestrator's grace period; a second signal, or the bound passing, ends the
process as a crash would, which loses nothing acknowledged, but for the bound passing while only
those closing periods are left: the stop is done then and exits 0, as nothing it holds is owed to
the cell and a late packet draws at most a stateless reset (RFC 9000 §10.2's note), where one slow
round trip on a loaded host stretched a connection's three probe timeouts to 9.5 s, past the 10 s
bound with the hand-over's 1.5 s (windows-aarch64, diag run 41260378525).

*As built (2026-10-24; 2026-11-05).* A `peer` line may name where the peer listens, `peer = ID ROLE
CERTIFICATE QUIC PLANE`, each `HOST:PORT` with the host an address or a name. `mantle serve`
resolves each name as it starts, once its volumes are open and before its runtime runs, through the
platform's resolver (std's `ToSocketAddrs`: getaddrinfo, GetAddrInfoW, what hyper-rt's
`dns::resolve` wraps), taking the first address of the family its own socket binds, the only family
that socket sends to (`localhost` resolves to `::1` before `127.0.0.1` on macOS); an address of the
other family ends the start naming the peer. A name stands for whatever address its holder has now,
and a Kubernetes pod made again comes back at another (research/78): a name that does not resolve as
the node starts does not end the start, and the node looks each named peer up again as it runs
(`names`), whenever its link to the peer ends or, while the name does not resolve, at the peer's
backoff (`bounds::backed_off`, `bounds::pause`), one lookup at a time on hyper-rt's blocking pool
off the shards, saying `unresolved ID HOST:PORT WHY` and, where the name gives another address than
it last did, `resolved ID QUIC PLANE` as it dials there (`Node::learn`). `mantle init` writes each
founder's peers with the addresses the founders stated, but for a founder stated at port 0 or at no
address of its own: where it listens is known only once it binds, and its peers learn it on standard
input, as of a peer that moved.

A start's failure names its step (`Step`, `Exit::Start`): configuration, identity, locks,
devices, budgets, storage or network, as in `mantle: the start's locks step: …/LOCK: another
process holds this lock`. The identity (the node's key and certificate, the authority's, each
peer's and the root keys) is read before any lock, so a missing key file refuses the start without
touching a directory. Each key file, the node's TLS key and each root key's generation, is read only
through a handle whose file no one but its owner may read (`mantle_node::secret`), on every
platform, the rule hyper-seal's key files keep: on Unix no permission bit for the file's group or
others, as OpenSSH and PostgreSQL refuse a private key; on Windows no allow entry in its DACL for a
principal but its owner and LocalSystem, read from the open handle (GetKernelObjectSecurity), where
Microsoft's OpenSSH also admits the Administrators group. One readable by others refuses the start
at its identity, naming the file (`…/identity/node.key: is readable by others than its owner`).
`mantle init` creates its keys through the same module, owner-only from the moment each exists.
The node's certificate and each pinned peer's are checked against the stated authority as the
identity is read (`mantle_cert::issued_by`, RFC 5280 §6.1.3 (a) with the authority the trust
anchor: the certificate's issuer is the authority's subject, its Ed25519 signature verifies under
the authority's key, and its validity holds by this machine's clock), so a directory holding another
cell's authority, or a peer pinned from another cell, refuses the start naming the certificate
(`…/identity/node.der: issued by another authority than the cell's`) rather than failing every
handshake. `mantle init` dates each certificate from its own machine's clock, so a founder whose
clock runs behind the one init ran on is refused as not yet valid, as its handshakes would be.
A plane port or a QUIC port another socket holds is refused at the network step. The QUIC
listener shares its port with a socket for each peer (`SO_REUSEPORT`), and a bind with that flag
set before it succeeds on a port another process of the same user holds on Linux and macOS
(measured on Linux 6.19 and Darwin 25.4), the two processes then splitting the port's datagrams; so
hyper-rt-transport binds the listener first and sets the flag after (hyper-raft `ab2eb0a`), and a
held port refuses it `AddrInUse`, as a plain bind does on Windows.

The orderly stop is built as planned above. SIGTERM and SIGINT (Ctrl-C and Ctrl-Break on Windows),
and a `stop` line on standard input, each ask it; the first request begins it, to end by `stop_ms`
(stated; unstated, docker stop's default grace period for a Linux container, 10 s, the shortest of
the orchestrators' defaults, so a default stop ends before their SIGKILL), and a second request, or
the bound passing, ends the process at once, as a crash would: the node runs on a thread of its
own, which the process's own waits for, and told of such a stop it returns without waiting for the
node's teardown, its logs and volumes finishing no device work they began and every write they
acknowledged already durable (`process::start`; run on the process's own thread, the node finished
its in-flight writes and flushes as it was dropped, and a second request took more than a second to
end the process on a loaded runner). The node says `stopping`, answers
each new object exchange `SlowDown` and waits for those running to end; transfers each range it
leads to the voter caught up with it, else the one whose log holds most, which the core first brings
up to its own (`handing GROUP TO`); waits until its member learns another leads (`handed GROUP
LEADER`), through an election under way to its winner. A transfer the core gives up, its rounds
passed with this node still leading (Raft §3.10), it begins again, to another voter where there is
one, until one leads or the stop's bound passes: with four busy loops on two CPUs the core gave a
stop's every transfer up before its target campaigned, the ranges keeping their term and this node
as leader (fedora, Linux x86_64), and a range so left elects only after the node is gone, with no
leader meanwhile. Once every range it led is handed over it announces to its peers' liveness
streams that its run ends in order (hyper-liveness `Liveness::stop`), so they count its next run no
failure in their MTBF's evidence (§2.4 item 7) and send this run nothing more, where each heartbeat
to the stopped node asked a flush of its own. Then it takes no connection
(its TLS without the server side), dials no peer, closes each connection and waits until each has
left its closing state, three probe timeouts (hyper-quic's close timer); a redial its closed
connections ask is never made, so its waits count none due. It says `stopped` with the sleeps of
its loop that found every timer of its shard taken, and ends, its log and volumes closed as the
node is dropped. That count is none: the shard holds a timer for each wait the node's task holds at
once, its turn's, its transport's QUIC timer's and, once a stop is asked, the stop's bound's
(`process::TIMERS`). Counted two, a stop's every turn ended at once: its third sleep waited for a
timer, and each release woke the task (8,834 turns in a stop of 346 ms on macOS); and a
redial counted due while closing ended every wait at once without the task yielding (80,000 turns
in 3.7 s; diag/roll-stop-bound), each a CPU spent for the stop beside the nodes it hands its ranges
to. `mantle serve` runs on when its standard input
closes, as it does empty under a service manager; `mantle node`, its harness's child, ends with it.
On Windows a console's close, a logoff and a shutdown end the process as a crash would: hyper-rt's
handler returns TRUE as it delivers them, and Windows then ends the process (Microsoft's
`HandlerRoutine`: "the system terminates the process"); holding the handler through the stop is
hyper-rt's change. A test asks a node on Windows to stop by its `stop` line, as a console event
reaches only a process group sharing the sender's console.

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

Each replica belongs to one shard for its life in the process, placed by §2.9's rule when the node
opens it: the shard past the first holding the fewest ranges. A later balancer moves a replica to
the shard with the least measured work (§2.9). A shard holds replicas of ranges whose logs are on any of the node's metadata
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

The node as built gives every range on a shard one waker, its task's, and queues each range with
a write out at every turn, where its drive takes the answer if it came. A leader reading a page of
a lagging member's catch-up from its log's file (hyper-durable's `LogStore::catch_up`) is queued
the same way while the read is out (`shard::answer_due`): the read's answer wakes the task as a
write's does, and the drive it queues sends the page and opens the read of the next, so each read
starts as the one before it is answered, within the store's window (one read out for the group,
and for each member the run that holds its next index and the one after it, opened only once the
member's credit took a page of the first). Queued for its writes alone, a leader whose read was
answered was driven only by the next message into its range or its next tick, and the page
waited for it. A wake is an edge, not a
state: it ends whichever await of the task it finds, and the process's own awaits between turns
(a line it says) take some. A lone node pinned to one CPU had its enlistment's answer, and the
wake for it, arrive 0.2 ms before its wait began, while the process said a line; the wait then
slept to its idle timeout, 3.07 s, with the answer in hand, past the watch's patience (CI run
40061578598; traced on a one-CPU Linux pin). So before it waits the shard looks where every
answer lies (`Node::answered`): the liveness stream's write, each range's oldest write out, and
the volumes' puts and gets. It waits only when none has come, and nothing awaits between that look
and the wait's first poll, so an answer that comes after the look wakes the wait. A range's answer
taken ahead of its replica is kept in front of the store's next (`node::answers`), so the replica
takes its answers in the order the log gave them. The look costs one check of each range with a
write out; a waker that named its range (hyper-durable's `Owner::woken`) would spare even that.

The look missed a source: the S3 listener's door, whose requests and body pieces tell of
themselves only by a wake. A lone client's next PUT comes as the turn that answered its last one
ends, and where that turn moved a range's commit, as it does on the node that leads the Name
range, the process says a line: a request whose wake that await spent waited at the door for the
node's next due time, the liveness stream's next heartbeat or its idle timeout, a timer's period
rather than the request. So the waker every source
holds (the logs, the volumes, the door) is the task's wrapped in a flag (`node::Woken`): each wake
sets it before it wakes the task, and the wait takes it first, turning the node at once where one
came since the last wait looked. A wake spent between turns is kept, from any source, and each
turns the node once.

A range shard's task (`shard::run`) keeps the same flag. Its ranges' logs answer a write, a
compaction's write and a member's catch-up read through the task's waker wrapped in `node::Woken`,
and the turn takes the flag as it begins, before it looks at any source, and the wait takes it
first. Without it, a wake that came while the task waited for room in its lane to the node
(`given.send`) ended that wait and not the one for its next turn, which looks for its ranges' write
answers alone, and a page read for a lagging member slept in hand until the range's next deadline.
The runtime's channels cannot set the flag: a message into the shard and room in its lane to the
node wake the task by its word, which hyper-rt takes from the runtime's own waker as the task polls
the channel, not through a waker the shard holds. That loses nothing, since each is a state its
channel keeps rather than an edge: the wait polls its receive before its other arms, so a message
that came is taken whatever woke the task, and room is what the waiting send takes as it is polled
again.

A turn remakes nothing it keeps (research/71, step 5). The transport's events it takes, the
replicas it drives with what each drive said, and the Raft messages they give each keep their
storage across turns, emptied as they are handled, and the ranges' groups are looked up where the
node keeps them; each was a buffer made and dropped every turn, busy or idle. A plane message a
drain of the socket finds is taken where its datagram was opened, in the socket's receive buffer;
the batch a turn's wait received is copied into one buffer the node keeps and taken after it, since
the wait must end, and the stream hear of its end and of its own write, before any message is fed;
each message was a `Vec` of its own. `mantle serve` writes a turn's lines (its links, ranges, log,
movement, stop) into one buffer it keeps and says them with one write and one flush, where each was
a `String` and a flush. A cell of three at rest allocated 477,128 times in ten simulated seconds
and allocates 168,019 (`tests/turn_allocs.rs`).

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
7. **The cell's history.** The time the cell's nodes have run and their crashes, the runs that
   ended without an orderly stop, the detectors' prior evidence on the time between failures
   (`Settings::history`), kept with the cell's membership in the root range (§3.5, §7). An
   orderly stop is not counted: it needs no detection, which is all the MTBF prices.

*As built (2026-10-20).* A node's owner (`crates/node`) carries the stream for the ranges it
hosts, and they elect by suspicion over it; items 1, 2, 4, 5, 6 and 7 are built, 3 is not:

- (1) One task on a hyper-rt shard holds the shard's replicas in hyper-durable's `Owner`,
  attaches their pairs (`Owner::pairs`), takes each change to them (`Owner::believe`), and after
  measuring (`Owner::measure`) drives each range whose timing moved, since a core draws its
  campaign's delay only when driven: given its first timing with nothing else to drive it, a
  range never campaigned (its term stayed 0 on one CPU).
- (2) hyper-rt-transport's `PlaneSocket` stamps each datagram in the kernel where the platform
  does; the owner reads its clock, drains the plane's socket, and polls the stream at that clock
  only when the drain found the socket empty, so every heartbeat stamped before it was fed. A
  connection made to a peer keys its path, and the owner tells the stream the peer is reachable
  (`Liveness::reachable`): a peer never heard is judged from then, not over the time its path had
  no keys, and a stream backed off meanwhile sends at its next flush, not at its backed-off slot.
  Without it, founders started tens of seconds apart on one CPU, each measuring its devices first,
  suspected each other as soon as the third gave them evidence, and a survivor took the suspicion
  of its range's next leader into the term (the roll test, 2026-10-28).
- (3) A member's id is its node's, and a node keeps a pair for each node it is told of; placement's
  map is not built (§7). The bound on pairs (`Settings::max_peers`) is each hosted range's bound on
  the members its configuration names summed, and one range's more for the range moving its pairs,
  which attaches its new peers before it detaches the ones it left (`node::liveness_pairs`): ranges
  lagging the cell name members it has since written off beside their successors. With pairs for
  the cell's nodes less one, a founder started after a member was written off and its identity
  replaced held one range at the founding configuration and another at the cell's, overflowed them
  and ended (`TooManyPeers`, 2026-11-19).
- (4) The run is kept as hyper-block's `record` in the node's directory (`crates/node/src/layout.rs`).
- (5) `Driven::flushed` feeds `Liveness::on_durable`, and a stream that asks for a flush gets an
  empty update of a log group of its own (the top of the group space, since group 0 holds
  hyper-log's key records), one out at a time. A liveness write the log refuses for room
  (`LogError::Full`, every segment live at the file's quota, or its queue `Busy`) is owed, not
  fatal: the stream waits on its answer and asks no other meanwhile, so each refusal asks one
  compaction of every range through what its engine applied (`Node::compact_if_stalled`, which
  drives again only the ranges whose compaction started), and the node asks the log again once a
  range's write became durable since, the log's progress that may have freed room, or once every
  range applied all its log holds committed past what it had then, a start's replay done, which a
  compaction then frees. While it is owed the node heartbeats no peer, which suspects it as one
  whose disk stalls (hyper-liveness's flush proof), and `Status::liveness_waits` counts the
  refusals. A range stalled for room is tried on the same facts: once for each move of the log's
  durable writes or the ranges' applied entries, as a try with neither moved is refused the same
  way, and a node that tried at every turn never parked, so what it waited for could not arrive
  (docs/measurements/2026-11-07-full-log-restart.md). Those facts are enough only because a full
  log takes writes again once its groups compact: the log keeps room for the frame that frees a
  segment, so a compaction's frame and the sweep or tail's advance it makes due always fit
  (raft-log.md §5). A file of three segments, hyper-log's least, once answered `Full` for good
  after its groups compacted, and the leader whose log it was held its range still with nothing
  left to wait for (docs/measurements/2026-11-14-log-full-wedge.md). `Status::compactions` counts
  the node's rounds of compaction, each one a refusal for room answered, and `mantle node` says
  them with the stream's refusals in its `room` line. Every other refusal stops the node, as a fenced range stops it (§6.2): a log whose
  device failed a write makes nothing durable again, so the node could prove no heartbeat and
  serve nothing, and its peers would keep its connections up for nothing. The simulation (§9.1)
  found a node going on that way after its device failed a write. Stopping on a room refusal
  instead stopped a founder of the KIND cell 22 times in a row on 2026-11-07: its log was full of
  records its engines applied and had not made durable when its host stopped, a start's first
  liveness write found no room, and each start found the log as full again
  (`tests/sim.rs`, `a_node_whose_log_fills_with_live_records_waits_for_its_ranges_to_compact_and_starts_again`).
  A proof is taken in the turn after the log answers it, whatever else the node waits for. In the
  simulation (§9.1) the log answers a liveness write in no simulated time, and the harness wakes the
  node's task once the clock has passed the ask, as a device's completion wakes it through the
  write's waker (`run_node`). That wake is a sleep, and the simulated shards had two timers, both
  held while the node waited: its wait's own sleep and its QUIC driver's. A sleep that finds no
  timer free arms only once one frees (hyper-rt's `Sleep`, counted in `Counters::timer_waits`), so
  the answer was taken at the node's next wake of its own. On seed 1 of the loaded founder's test
  (`tests/sim.rs`,
  `a_node_that_just_joined_never_suspects_its_live_loaded_founder_and_suspects_it_cut_off`) the
  joiner, cut off from its founder, asked a proof 4.1 ms after the cut and submitted it in the same
  turn, the log answered it at once, and the joiner took it 443.7 ms later, at its stream's next
  wake, its shard counting one more sleep that waited for a timer at each proof. Its stream judged
  nothing meanwhile, and the wait, past the margin it held the founder to, gave the founder that
  margin again from its end (hyper-liveness, "A judge whose own proof stalls"; research/81 L3), so
  the founder was suspected 801 ms after the cut, past the 552 ms patience the joiner stated.
  Nothing of it was the log's ordering (raft-log.md §3) or the node's: the stream's ask was
  submitted in the turn that made it. The simulated shards now have the three timers a node's task
  waits on at once, as hyper-rt derives a shard's bound from its tasks' concurrent waits
  (`hyper_rt::futures::within`). Over the run the joiner's proofs had waited a median 8.5 ms and
  up to 1.5 s for the wake; now each is taken a timer tick, 100 µs, after it was asked, and the
  joiner suspects the cut-off founder 88.2 ms after the cut, within the 92.0 ms patience it states,
  which the test now holds it to. A node's real log wakes its task through the write's waker, which takes
  no timer (`NodeLog::write_liveness`).
  Proofs taken at once exposed an owed write that waited for good. A refused write was asked again
  only once a range's write became durable or the ranges applied more, and a node idle when the
  log refused it does neither. Nor does the log hold it a slot of its own: hyper-log keeps a waiter
  only for room in its queue, which `submit_waking` waits for already and refuses `Busy` only past
  the waiters it holds, and it frees a full file's segments at a sweep after its groups compact,
  which it tells no submitter of (raft-log.md §3, §5). The stream asks no other proof while one is
  out (hyper-liveness's `Liveness::poll`), so a node whose log refused its write as it went idle
  held its heartbeats for as long as it idled, and its peers suspected it as one whose disk stalls.
  On seed 1 of `a_liveness_write_the_log_refuses_for_room_is_owed_and_the_node_goes_on` the leader
  and a follower counted 2 of their 3 refusals while the clients ran; the third came after they
  finished, and the cell stood still 3.1 s until the test failed. An owed write is now asked
  again, whatever else moved, one of the stream's shortest sending intervals after its refusal
  (`Node::liveness_retry_at`, a deadline of the task's wait): the cadence at which the stream of a
  node that writes nothing else asks its proofs (this item: one per shortest interval among its
  pairs), since a heartbeat leaves only on a durable write made after the previous one was due and
  the stream asks one when none came in time (hyper-liveness's flush proof; research/81 L3, "A
  judge whose own proof stalls"). A proof so waits no longer than the stream itself waits between
  proofs, which the patience a node states already holds (`Status::patience`: its longest sending
  interval and its mean flush), and the log is asked no more often than the stream asks it: each
  retry refused asks one compaction of every range, as each refusal did. A slot in the log woken
  by the sweep that frees a segment would answer sooner; hyper-log has no such event to give.
  `an_idle_node_whose_liveness_write_the_log_refused_proves_itself_again` (`tests/sim.rs`) refuses
  an idle follower's next two writes: it counts both and takes its proof 0.2 ms after the second
  on seed 1, within the 4.8 ms patience it states (seed 2, on regional paths: 240 ms, within
  1.73 s). Without the retry it counted both, the first refusal's compaction having written, then
  took no proof, and the cell stood still 3.1 s until the test failed.
- (6) Each range's next wake and the stream's are the task's deadlines, and a wait that ends at
  or past the stream's wake is reported (`Liveness::on_wait`).
- (7) The root range keeps each enlisted node's history: the time it has run over all its runs and
  its runs that ended without an orderly stop (`root::Command::Lived`, a node stating its own
  alone, the greater of each total kept, so a statement applied late or twice takes nothing back).
  A node keeps its last orderly stop in its directory beside its run (`layout::Stopped`: the run,
  the node's time run then, and its stops), written whole once the stop is done, its log and
  volumes closed, and keeps one at a start that finds none, before it raises its run, naming the
  runs its counts begin from: none for a fresh directory; for one whose runs began before the
  record was kept, all of them, their ends unknown, so none is counted a crash. Runs are numbered
  from one, so the runs before a start that did not stop in order, its crashes, are its last run
  less its stops: exact however many statements crashes lost. At a start the process reads the root range's replica it hosts (an enlisting node hosts
  the root range), sums every node's history (`root::history`, at most the table's nodes), takes
  its own as its directory says, and gives the sum to its stream as `Settings::history`; it says
  what it gave (`history CELL_NS CELL_CRASHES OWN_NS OWN_CRASHES`). Once enlisted it states its
  totals, and again each time its run's time has doubled since the last statement was answered,
  so a crash loses at most half a run's time and a run of `t` states about `log2(t/first)` times,
  the first statement's wait the unit. An orderly stop is no crash: it hands its leadership over
  and needs no detection, which is what the MTBF prices (hyper-raft `docs/timing.md` §2.2, the
  crash term of `U`). Without it every start began with none, and a restarted node's MTBF was
  `2T` over the seconds it had watched: on a busy host hyper-timing's configurator then plans a
  false suspicion every few seconds a pair (MTBF 2 s: 2.1 to 4.8 s; an hour: 9 to 18 minutes;
  `docs/measurements/2026-10-25-roll-on-a-busy-host.md`). A node that enlists nothing keeps no
  history of its own and starts from the cell's where it hosts the root range.

A range hosted by no node still elects on ticks with focal's counts, as above (replica.md §3). SWIM's place (§3.5) does not
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
| Laptop | all of the above at one gateway, one device dispatcher and its ranges on one shard (§1.2, as built 2026-11-17): a tree of tens of KB, a cost table per operation from calibration, a Space-Saving table usually not full and so exact; the CLI on the native protocol and stock tools on the HTTP listener under one authority |
| Node | per-device cost models and budgets, many dispatchers; chunk devices chosen by two random choices on credits (§7) |
| Cell | node credits and service counters between gateways and nodes (§3.3), tenants' contracts divided among gateways (§4.5), hedged chunk writes where measured tails justify them (§5.4) |
| Region, fleet | a tenant's regional and global share divided by measured demand on longer intervals, summaries merged by addition (§4.5) |

Nothing a later step adds is configured or paid for at an earlier one: with one participant,
the cell's mechanisms reduce exactly to the local ones (research/27 §7.6).

### 2.8 No fixed counts: admission, batching and sessions

The owner's rule for goal 16 (2026-11-12): "No fixed counts in admission, batching or sessions:
only learned and data-derived values." Research/87 holds the sources and the plan. Each count
becomes:

- **The answers a session keeps: bounded by bytes, reached by refusal.** A session keeps an
  answer while its row, with that answer, fits a row (`Rules::max_value_bytes`, the largest S3
  admits, so no page grows). A command whose answer would not fit is refused before it takes
  effect, `Answer::AnswersFull`, its writes undone and its session kept, and so is each later
  command of that session in the entry. The gateway asks them again in its next entry, which
  acknowledges the answers before them. A refused command applied nothing and kept nothing. A
  copy reaching the log later is its first application. Once applied, its answer is kept until
  acknowledged, and after that a copy is `Repeated`. So a command takes effect once at its
  serial, and no bound the gateway reaches ends its session. Expiry for room did end it, which
  left a lost reply's commands to the files that recognise copies (research/87 §1: the
  dissertation's §6.3, RIFL's §4.2). A gateway never reports `AnswersFull` as a refusal: past its
  deadline the command is `Deadline`, which may still apply, once.
- **The commands an entry carries: what waited, within its bytes.** One entry out a range, as
  then, carries every command waiting that fits `max_entry_bytes`, LogCabin's double buffering,
  whose batches "dynamically [adjust] to load" (research/87 §1). Since 2026-11-17 a node keeps up
  to `ask::ENTRIES_OUT` (two) entries out to a range at once, as many as its log has frames in
  flight, each carrying what waited when it was made, and each asked, answered and given up on
  its own (§6.6).
- **The operations a node admits: learned.** The limit is the point of greatest power,
  Kleinrock's throughput over delay, which by Little's law rises below Denning and Buzen's
  saturation point and falls past it, so its maximum is the knee. The node finds it as BBR probes
  its pipe, from above. It starts at the ceiling its memory was split for, where a fixed limit
  stood, and halves while power does not fall a fifth, counting only phases its limit bound, each
  judged against one that ran wholly under the limit before it: a phase in which no operation was
  turned away measured its clients, not its limit, so a limit no client reaches stays where it is.
  Halving on such phases drove a closed ladder's limit down to its first step's one client, and it
  climbed back a quarter a cycle while the ladder doubled. Then it cycles eight phases about a base: a
  quarter past it, a quarter below, six at it. At each cycle's end the base moves to the limit of
  greatest power among the base's phases and the probes its limit bound; a probe in which no
  operation was turned away moves nothing. A phase measures only after the operations admitted
  until its first completion have completed, and then 267 completions, enough to tell a quarter's
  change in power from chance at 99%. BBR's own estimate, the windowed least latency times the
  windowed most throughput, fell to one or two operations with sizes mixed (research/87 §3).
- **The memory the operations hold: bytes, each operation its own.** An operation is admitted
  only where the bytes it holds at its own size fit beside those of the operations running, within
  `gateway.objects_bytes` (`derive::Charges`, `crate::admit`). A write is charged at its body's
  length, which its head states before its fields are read: its first segment and first block,
  the largest of its own, each chunk of a block copied to send, and a piece of its body, under
  the scheme that holds the most of it (`put::bytes_for`); the driver reserves its segment at the
  same length. That piece is the one its body is read into: the node's task reads a native put's
  body, and an admitted operation's fields and what it drains of a body, into memory of the
  operation's own that its charge holds (`node::read_piece`; a put's feeding's storage, which each
  piece is copied into from the one piece the node's task reads with, `Node::piece`, §2.9 step
  6c), never a piece of the transport's budget, which every connection's windows, receive chunks,
  heads and frames share (§3.3). A body
  read a piece of that budget at a time waited, while other traffic held it, on no ask of its own:
  the host held nothing of it, so vouched for nothing, and the budget freeing woke nothing; its
  client, judging the exchange once a period, gave it up (the node suite's 20 MiB put
  `Lost(Stalled)`, about one run in ten on ubuntu-24.04-arm, 2026-11-18; `tests/put_body.rs`).
  Every operation's charge holds a piece, a write of no bytes the least one is, so each reads its
  own; one answered at once is charged nothing and drains its body through the transport's piece
  for its owner's reads. A get is charged for its window, and a copy for its get's and its put's at
  once (below); every other operation the most any holds. A request turned away for bytes says
  nothing of the limit; one turned away by the limit tells it its clients ask more. The data comes first (research/87
  §4, rule D): the memory past every class's floor is the indexes' and engines' wants first, each
  taken whole, and what the memory holds past those is the operations', with at least one at the
  most one is charged, a copy's get and put at once (`derive::most_charged`). Which of the derived
  rules (A, B, D) stays is §4's experiment. That share holds first the bytes for what the node
  serves (`derive::served`): the most one operation is charged, and, where it serves S3, each
  of one stock client's ten connections a part of its own, the AWS CLI's and boto3's 8 MiB
  (`listener::STOCK_PART`), at the most a part is charged, every block of it going down at once
  (`derive::stock_part`); then as many operations more as fit at what one costs: the least it is
  charged, a write of no bytes, and what one more operation adds to the node's other classes, its
  native client's connections and kept exchange, its pooled exchange, its chunk asks' exchanges
  and each range's state for its read, the derivation's own difference between one operation and
  two (`derive::operations`). A share short of the bytes for what the node serves still serves as
  many operations as one stock client asks where it holds them at the least, the bytes past them
  its parts'. That count is the limit's ceiling `C` (`gateway.objects`), and the node admits as
  many native clients. What remains of the share is `gateway.objects_bytes`. Where
  a fixed ceiling of 8 operations each charged 42 MB stood, a node now holds thousands of small
  writes at once, and the learned limit, not the ceiling, decides how many run. With only one
  operation of the largest blocks held and the rest at the least, a pod given its need admitted
  one of boto3's 8 MiB parts at a time, its last part waiting at the door past botocore's 60 s
  (kind-cell, CI run 43790965629).
- **No request waits at the door for good.** The door takes its requests in the order they came,
  so one that could never be admitted would hold every request behind it. A write is charged at
  the widest window from the floor's to the one learned whose charge the operations' bytes hold
  (`host::fitting`): the floor's always fits, since those bytes are at least the most one
  operation is charged, so every request is admitted once the operations before it end.
- **Native clients draw their exchanges from a pool.** A client's connection keeps one exchange of
  its own, as mantle-client asks one at a time, and the rest it opens come from a pool the node
  sizes to its learned limit (hyper-transport's `StreamCredit::Pooled`, Breakwater's credit, Cho
  et al., OSDI 2020): a client past the pool is held at its own end by QUIC's stream limit, where
  it costs the node nothing, rather than sent to be refused. A peer node's connection is raised to
  its streams once its role is known. So a client costs the node its own connections and the
  exchange it keeps, never a peer's streams, and the transport's floor grows by the same bytes a
  client (`bounds::Stated::clients`, `pool`).
- **The blocks a put holds going down and a get holds being read: learned.** A put's body waits
  while its window is full, each block's chunks stored and its Block command written before the
  next may go. By Little's law the blocks that keep the body coming at its client's pace are the
  time a block takes from going down to recorded times the body's rate, over a block's bytes
  (`crate::window`). The node measures both from its own puts: each block's time to recorded
  (`Put::take_committed`), and how fast the first block of a longer body filled, which no window
  holds back, so it is the client's pace (`Put::take_filled`); each smoothed with RFC 6298's gain
  of 1/8. A put is admitted with that window, at least the floor's one block and no more than its
  body's own blocks, and charged for it (`derive::Charges::write`). A get is the same law turned
  round: its client waits while its window is read, and the blocks that keep the body going at the
  client's pace are the time a block takes from its row asked to its plaintext opened times the
  rate the client takes the body, over a block's bytes. The node measures both from its own gets:
  each block's time to read, in the host's time (`Get::tick`, `Get::take_read`), and how fast the
  client took a block the host held whole once the client's room for it was full, which no window
  holds back: on a native exchange the stream's flow-control window, smaller than a block, so
  every block's writing waits on its client (`host::Drain`); on the S3 door, whose reply's channel
  holds whole blocks, the connection's socket, which the connection times as it writes each piece,
  from when the socket first took less than it was offered or refused an offer whole to the
  piece's last octet written, and tells the node through the door (`listener::io::Drained`,
  `door::Doorway::drained`), so a client that drains faster than the reads is paced as one slower
  is. A Windows socket takes all of a send while it is under its quota, which follows TCP's ideal
  send backlog up to 16 MB, and one more past it while it holds a single send, then refuses
  (research/72 [8, 9, 10]): offered whole blocks, it took a get's first two at once and never a
  part, so no plain connection's client on Windows was paced, and once refused it took a whole
  block for the little room its client made. A plain connection there offers its
  socket a record's plaintext at a time, as a TLS one does on every platform, and reads a refusal
  as full (`listener::io::offer`). Timed from the reply's channel
  instead, a block held behind the one its connection had just begun, a client faster than the
  reads never filled the channel and its gets kept the window they were admitted with. Each is
  smoothed with the same gain. A get's object's size is known only once its
  row is read, so it is admitted at the floor's one block and charged for it, its window of blocks
  at the largest block and the block taken to be written out (`derive::Charges::read`). Once its
  version is read it widens to the window learned, no more than the blocks its range is cut into
  at the smallest block, and the widest whose charge the operations' bytes hold beside the
  others' (`host::reading`, `host::fitting`), taking those bytes before it holds the blocks
  (`admit::Admission::took`). Charged the window learned at the door instead, a small get held a
  large one's bytes until its row was read. A copy is the same law with its put as its get's
  client. Its put holds the window the node learned of its puts, and drains that many blocks in the
  time one takes to be recorded, so its get holds that window times a block's time to read over a
  block's time to be recorded, no more than the blocks its span is cut into at the smallest block
  (`Windows::copy_get`). The drain is taken from what the node learned of its puts, not timed from
  when the put's room was full: a put faster than the reads never fills it, the blind spot the S3
  door's gets had. A copy's put's body comes at its get's pace, not a client's, so its put gives the
  node's puts its blocks' times to be recorded but not how fast its first block filled, which would
  be its source's reads (`Put::take_filled`); its get gives the node's gets its blocks' times to
  read. Its source's size is known only once its version is read, so it is admitted at the floor's
  one block for each, charged for its get and its put at once, its put's of the largest body a copy
  writes (`derive::Charges::copy`, `host::charged`), within the most one operation is charged. Once
  its version is read its put's window, then its get's beside it, widen to the widest whose charge
  the operations' bytes hold beside the others', the bytes past its admitted charge taken and those
  its span does not need given back (`host::copy_windows`).
- **A native request past the limit waits, as the S3 door's does.** One the host has no room for,
  or that comes behind requests already waiting, is parked with its body unread, QUIC's flow
  control holding its client's bytes, and taken in the order it came once room frees; each turn it
  waits it is vouched to its asker as a held job is, and at its client's deadline, or a stock
  client's patience where it gave none, it is answered `SlowDown` (research/77 D1, D2). At most as
  many wait as the transport holds exchanges. It was answered `SlowDown` at once, which mantle's
  client took as a throttle and waited out over one to twenty seconds.

Since 2026-11-18 no count in admission, batching or sessions is fixed: each above is learned from
the node's own operations or derived from the bytes it bounds (research/87 §2). The window's one
block, `gateway.window`, is the floor the windows are learned from, §2.6's `⌈R·T/B⌉` at its least:
the least a put or a get holds and still moves its body.

### 2.9 A node's ranges, its log and its native door across its shards

The owner's decision of 2026-11-18: a cell serves at least 10,000 small PUTs a second, and a node
spreads its ranges, its log and its native door across its shards. Run r20 (a three-node cell on
one Mac Studio, M3 Ultra, 32 cores; a release build of lean-doors `aef9efae`; a closed ladder of
1-byte native PUTs) served 1,290 PUTs a second at 512 clients and 2,284 at 1,024, at 0.69 and
0.74 ms of server CPU a PUT, the three nodes summed, and collapsed to 913 at 2,048, answered
SlowDown. At 0.74 ms a PUT, 10,000 a second is about 7.4 cores across the cell, about 2.5 a node,
where each node ran its ranges, its log's submitters, its transport and its QUIC door on its first
shard (§1.2's note of 2026-11-17; audit 2026-11-12 §6: one core a node caps a cell well under
10,000 PUTs a second). The sources are research/95.

**A range's shard.** A range is placed when the node opens it, on one shard for its life in the
process: the shard past the first that holds the fewest of the node's ranges, the first of those
tied; a node of one shard holds every range on it (`node::spread`). The rule counts; it picks no
number, and it is a function of the configuration's order of ranges and the shard count alone, so
nothing on disk names a shard and a node started again with another CPU count places afresh at no
cost: the log is the device's and a range's rows are its own engine store's (§6.6), neither a
shard's. Redpanda does the same in kind: "every partition replica is assigned to a CPU core on a
broker", each partition a Raft group, with "an even distribution of a topic's partition replicas
across all cores in a broker", placed again "after broker startup, if core count increases or
decreases" (research/95 §2); ScyllaDB balances its tablets "across shards in each node" (§3).
Counting rather than hashing a range's group to a shard: a node holds few ranges, five in a cell
of three (§6.6), and five groups hashed uniformly onto 31 shards share one with probability
1 − (31·30·29·28·27)/31⁵, about 29%, where counting gives each its own whenever the shards past
the first number at least the ranges, and otherwise holds every shard within one range of every
other. The first shard is passed over because it carries the node's one UDP endpoint, its plane,
its liveness stream and the S3 door's accept (below), as the S3 connections already pass over it
(§4.1). A range moves between shards only by a later balancer, on measured work, as Redpanda's
`core_balancing_continuous` and ScyllaDB's in-node tablet balancing do (research/95 §2–§3): §2.1's
least measured work is that balancer's rule, and a move hands over the range's replica, its log
handle and its engine whole, which nothing builds yet.

**The log: one per device, fed by every shard.** A node keeps hyper-log's one log per device
(raft-log.md §1), and a range on any shard submits its updates to it through its group's handle.
hyper-log already serves this: the log is "a handle to its owner, which any number of threads may
share by reference", its owner and its I/O thread are "two threads, whatever its number of groups
or callers", and a submitter hears its answer through its own waker, which wakes the range's shard
wherever the owner runs (research/95 §7). Its writer gathers every update that arrived while the
last flush ran into one frame, so one flush commits every shard's ranges: group commit's rule that
one write and flush commits the group (Helland et al., TR 88.1 §2; DeWitt et al., SIGMOD 1984,
§5.2; research/71). A log for each shard is refused: each flush would commit one shard's ranges
where one commits them all, a range's history would be tied to its shard so a changed CPU count
moves logs, and each log runs two threads of its own. The hand-off costs one message on the
owner's bounded channel and one wake a range's turn, not a command's, since a turn submits one
update carrying all its commands' entries (§2.2). Research/71 measured that concurrency, not one
writer, reaches a device's flush rate (btrfs: 124 flushes a second from one writer, 466 and 478
from 6 and 16); one log gains that by keeping more than one frame's flush on the device at once,
sized to the device's measured knee, a change to hyper-log (below), which keeps one flush
committing every group. TiKV split its work the same way: Raft groups driven on a pool of threads,
messages routed to each group's mailbox, and log writes handed to a separate writer pool of one
thread by default (`raftstore.store-io-pool-size`; research/95 §4).

**The transport and the native door: one UDP endpoint, which routes.** The node keeps one QUIC
endpoint and one plane socket, on the first shard. The endpoint keeps every connection's packet
protection, acknowledgements, congestion and streams, and hands each Raft message to the shard
that holds its range (the message's header names the range, §3.2) and each native request to a
shard past the first to be driven, the one with the fewest requests running, as an S3 connection
is placed (§4.1). Endpoints on every shard sharing one port are refused for now: spreading one
port's unicast datagrams over sockets is stated on Linux alone (`SO_REUSEPORT`, socket(7)); macOS
states duplicate bindings and delivery of multicast and broadcast to each, and Windows calls the
sockets sharing a port "indeterminate" (research/95 §5), so it is no portable path (CLAUDE.md §7).
And a kernel spreads by address, which QUIC lets change under a connection, so a socket on the
wrong shard must still route by the connection. QUIC allows that routing: an endpoint chooses its
connection IDs by "an implementation-specific (and perhaps deployment-specific) method" that lets
packets be routed back to it, but no observer outside it may correlate the IDs of one connection
(RFC 9000 §5.1), and a packet is matched to its connection by its Destination Connection ID
(§5.2). So the step past this one, if the first shard's share of a PUT's work is then measured to
bind, writes the shard that holds a connection into the IDs the node issues, under a key, never in
the clear; whichever socket receives a datagram hands it to that shard, which holds the
connection's state; and on Linux, sockets sharing the port through `SO_REUSEPORT` spread the
receiving. Both are hyper-transport's and hyper-quic's to build.

**Requests across shards.** A native PUT's Block, File and Name commands go to three ranges that
may be on three shards. The shard that drives the PUT sends each command to its range's shard on
that shard's command lane and waits for the answer in a slot of its own, which the range's shard
fills and whose waker it wakes: Seastar's explicit message passing between cores, where nothing is
shared (research/95 §1). The commands keep the order the gateway gives them (gateway.md §2), each
sent once the one before it is answered; this record changes no order. A node's own commands and
reads (§6.6) take the same lanes.

**Every queue between shards is bounded**, each sized so it refuses for room only past a bound
the node already states, and each refusal typed (CLAUDE.md §2):

| Queue | From, to | Bound | At the bound |
|---|---|---|---|
| Raft messages in | the endpoint, to a range's shard | the frames the transport admits from the peers at once: a lane's window for each peer (`lane_window`) | dropped and counted, as the network may drop it; Raft sends it again (§3.2) |
| Raft messages out | a range's shard, to the endpoint | the same window a peer (`Remote::waiting`) | dropped and counted, as now |
| Commands and reads | the asking shard, to a range's shard | the commands and reads the node holds at once (`command_asks`, `read_asks`) | refused, as `Node::command` and `Node::read` refuse one past them now (`NodeError::Asks`) |
| Answers | a range's shard, to its asker | one slot an ask, taken when it is asked | cannot be refused |
| Log updates | a range's shard, to the log's owner | hyper-log's bounded owner channel | `LogError::Busy`: the range waits for room (§2.2's stall) |
| Native requests | the endpoint, to a driving shard | the exchanges the transport admits (§2.8's pool) | parked, then `SlowDown` at the client's deadline (§2.8) |
| Chunk puts and gets | a driving shard, to the endpoint | `chunk_asks` | refused, as `Node::put_chunk` refuses now |
| A native reply | a job, to the endpoint's shard | one answer, the least that moves a reply (`door::Replying::FIRST_ANSWERS`), a get's piece given only once the last is written whole (6c) | held in the job's door, its client held by its stream's flow control |

A shard that ends takes its ranges with it, which nothing else holds, so the node stops and its
next start recovers them from the log (§1.7), as it stops for a lost task now.

**What stays on the first shard, and why.** The QUIC endpoint and the plane's socket, one UDP
endpoint each, the portable path (above); the liveness stream, whose heartbeats arrive on the plane
and whose writes go to the log like a range's (§3.5); the node's membership and its own statements;
and the S3 door's accept, which hands each connection on (§4.1). With one shard, everything.

**The order of building.** Each step is an hour's work, unit-tested and merged before the next:

1. The rule, and the node's table of ranges by shard, every range still on the first shard (as
   built, below).
2. A shard's ranges driven by a task on that shard, with their own owner (`Owner`) and the log's
   group handles; first, whether hyper-log's group handle and hyper-durable's shell may cross
   threads, which, if not, is a hyper-raft change before this step.
3. The Raft lanes in and out between the endpoint and the ranges' shards, routed by the table.
4. The command and read lanes, and their answers.
5. The rule over the runtime's shards, which places the ranges, and `lanes_per_peer` (§3.2) the
   shards that run ranges, at most the ranges the node hosts.
6. Native requests driven past the first shard, in seven parts, each an hour's work, unit-tested
   and merged before the next. With one shard, or none past the first that `NodeSettings::shards`
   names (`range_shards = first`), every request stays on the first shard, as now.
   - *6a, the rule and its count* (`node::drivers`; `host::start_object`, `Node::step_host`,
     `Node::job_lost`): a native request the host admits is placed on the shard past the first
     with the fewest requests running, the first of those tied, as an S3 connection is placed
     (`listener::shards::least`), and counted there until its job ends; over the one shard the
     node's task drives every request on, so each is placed on the first, where it runs.
   - *6b, a job's asks as values* (`host::ask_put`, `ask_get`, `ask_command`, `asked`,
     `take_answers`, `drop_abandoned`, `host_due`; `Node::command`, `Node::read`,
     `Node::put_chunk`, `Node::get_chunk`): each ask a job makes of the node, a range's command or
     read or a chunk's put or get on a volume, the volume's node named by the node's table, is one
     value served by one function of the node's, refused `NodeError::Asks` past `command_asks`,
     `read_asks` or `chunk_asks` as now and asked again at the job's next turn, so a lane can carry
     it. Every job still on the first shard.
   - *6c, the native door as the S3 door's* (`host::read_fields`, `feed_put`, `write_fields`,
     `drain_job`, `send_held`, `write_get`; `Door`, `Local`; `crate::door`): a native request's
     fields are read on the first shard, then its operation is run by a job whose body comes on a
     bounded channel the node's task fills from the exchange, at the depth a body's pieces need
     (`door::Depth`), and whose answers the node's task writes to the exchange from a channel at the
     depth a reply needs (`door::Replying`), as an S3 connection does for its job (`Door::Local`).
     The node's task keeps the exchange: its vouching (`Endpoint::working`) and its end
     (`Node::job_lost`). What the node's task does for the exchange is the exchange's half of what
     the job does now: a body's pieces read as `feed_put` reads them and given as an S3 connection
     gives them (`door::Piece`, its end carrying the trailer's checksum), and a reply's head, fields
     and bytes written as `write_fields` and `write_get` write them. So it may land as two: the
     body, then the reply.
   - *6d, a host of its own* (`host::Job` and the host's `impl Node`; `Node`'s `jobs`, `abandoned`,
     `gateway`, `moved`, `slowed`): the jobs and what they hold move from `Node` into a `host::Host`
     that the node's task holds and steps, its asks (6b) served by the node, its doors (6c) channels
     and its puts placed by the volume table the node last read (`Node::table`, `host::Table`), so a
     shard past the first can hold one. Admission, parking and the put window stay the node's, one
     authority for its bottleneck (§2.5), told of each job's end and each put's samples of its
     blocks (`Window::committed`, `Window::filled`); the bytes moved and the `SlowDown`s by cause
     are each host's, summed where `Node::moved` and `Node::slowed` are read. The largest part,
     which may land as two: the state, then the steps.
   - *6e, a driving shard's task and its lanes* (a module beside `node::shard`, made as
     `shard::Lanes` is): a task on a shard past the first holding a `Host`, started through that
     shard's control queue as the listener's takers are, Normal as the S3 connections beside it.
     Into it, each request handed, its operation and its door's ends, at most the exchanges the
     transport admits at once (`parked_most`), each request handed holding one; and each volume
     table the node reads, the latest superseding the rest, as a far root range's rows reach the
     node (`Out::Rows`). Out of it, its jobs' asks, the host holding each kind to the node's bound
     for it (`command_asks`, `read_asks`, `chunk_asks`) and refusing one past it `Asks`, asked again
     at the job's next turn, each with a slot for its answer made as it is asked
     (`hyper_rt::sync::oneshot`), so the answer cannot be refused; the node's task answers `Asks` in
     the slot of one it has no room for now, its other asks holding the room (the table above). And
     each job's end with its put's samples, the host's counts, and each held job's beat, by which
     the node's task tells its exchange's asker its work is held (`Endpoint::working`, research/77
     D1). Production starts none. Tested on a two-shard runtime: a request handed is driven there,
     its asks served by the node's task, its end given back; a full lane refuses, typed; a shard
     whose task ended stops the node, `NodeError::ShardEnded`.
   - *6f, requests placed by the rule* (`host::start_object`, `Node::spread`, `process::serve`,
     `derive::shard_shape`, `node::shard_lanes_bytes`): each native request admitted, its fields
     read, is handed to the shard 6a's rule picks among those `NodeSettings::shards` names past the
     first, passing over one hyper-rt's watchdog sees held past its limit (`Lane::takes`); one a
     lane refuses runs on the first shard, its place in the count moved there. `process::serve`
     starts each driving shard's task; `derive::shard_shape` and the memory floor count it and its
     lanes. Tested: requests spread by the count, and one a lane refuses runs on the first, counted
     there. No node test runs the object host (each opens its nodes with no gateway), so the native
     suite (`crates/mantle/tests/native.rs`) run under `range_shards = spread` is the end-to-end
     test, and the ladder the measure.
   - *6g, a far range's commands from the driving shard* (`shard::In::Propose`, `shard::Refusal`,
     `Node::propose_asked`, `Asking`): a command or read of a range this node leads on a shard
     past the first goes from the driving shard to that range's shard on its command lane, its
     answer in a slot of the asker's (the table above); a range a peer leads, or one on the first
     shard, still through the node's task and its transport. How the node's session with a range
     (`Asking`, §6.6) is kept once its commands come from several shards is the first thing 6g
     settles.
7. In hyper-raft: hyper-log's frames in flight past one, sized to the device's measured knee.
8. In hyper-raft: connection IDs that name their shard, and datagrams routed by them.

*As built (2026-11-18), the rule* (`crates/node/src/node/spread.rs`). `Spread` holds each range's
shard, by its place in the node's table, and each shard's count of ranges, and places each range by
the rule above as the node opens it (`Node::open_on`), over the one shard the node's task runs
every range on (`NonZeroUsize::MIN`), so each range's shard is the first, where it runs. A count
past a `usize`'s refuses the range, `NodeError::Full`, placing nothing. `Node::shard_of` says a
range's shard and `Node::ranges_on` a shard's ranges in the order the node opened them, which
step 3's routing reads. Tested (`node::spread`): the rule places each range on the shard past the
first with the fewest, the first of those tied, never on the first while another shard exists, and
every range on the first where it is the only one; a cell's five ranges take a shard each from six
shards to 32; every shard past the first stays within one range of every other, from one shard to
nine and one range to forty; the table says each range's shard and each shard's ranges in its
order; and a count past a `usize`'s is refused with nothing placed.

*As built (2026-11-18), step 2's check: a range may move to the shard that drives it.* A range's
replica (hyper-durable's `Replica` over the node's `Answers` of hyper-log's `GroupStore`, with its
`RangeMachine`), its group's handle (`GroupLog`) and the arena of a shard's replicas (`Owner`) are
each `Send`, and the log (`Log`) is `Sync`, over the process's file (`DeviceFile`) and engine
(`Stored`); `node::tests::a_range_may_move_to_the_shard_that_drives_it` holds this at compile time.
None of the first three is `Sync`: a `Cell`, a `RefCell` and a reply port's `Receiver` keep each to
one thread at a time, which a range moved to its shard once and driven by that shard's task alone
never needs. Nothing in them is tied to the thread that made them (research/95 §7): hyper-log's
owner and I/O threads are its own, which `Log::open` spawns; a group's handle reaches the owner
over its bounded channel and hears each answer through the waker its caller gave; and hyper-durable
spawns no thread, each owner thread (a shard) holding one `Owner` driven with the wakers its
embedder gives it. hyper-log keeps each thread's free reply ports in a pool of that thread's,
taking one where a call is made and keeping it where its wait ends; a range's calls are made and
end on its shard, so a shard's pool holds its own ranges' calls. So step 2 needs no change in
hyper-raft.

*As built (2026-11-18), step 2a: a shard's task* (`crates/node/src/node/shard.rs`).
`Node::spread_to` starts the task of one of the node's shards on the runtime shard a holder names,
through its control queue (`hyper_rt::runtime::submit_to_holder`, as the S3 listener's lanes start
their takers), and moves there each range the table places on it before its first drive. A
replica with a write out is refused, `NodeError::WritesOut`, and nothing moves: each write records
the waker of the task that submitted it (`GroupLog::submit_waking`), so its answer would wake the
shard it left. The task holds its shard's replicas in an `Owner` of its own, made with the task's
waker, one slot a range, and that shard's deadlines; it drives each queued replica once a turn as
`Node::drive` does, and waits for the first of its lane, its earliest deadline (at most the idle
timeout) and any wake, a log's answer among them. The two talk over two bounded hyper-rt channels
(`hyper_rt::sync::channel`), each holding the exchanges the transport admits at once (the node's
`parked_most`). Into the shard: a replica moved; a Raft message (`Taking::checked`), dropped and
counted when the lane is full, as the network may drop it; a proposal (`Node::propose`); a change
of the stream (`act_on_liveness`), whose lane full stops the node, `NodeError::LaneFull`, as a range
that missed it believes what the stream no longer does; what the stream believes of a peer a range
came to share its group with; and what it measured (`Node::measure`: its granularity, its log's
mean flush, each peer's echoed round trip), from which the shard derives each range's timing by
hyper-durable's `Replica::measure` over those values, and which a full lane leaves to the next
turn's measurement. Out of it: Raft messages, sent by `Node::send`; applied answers; flushes, for
the stream's `on_durable`; a range's peers when they change, which the stream attaches and
detaches, telling the range its belief of each new one, as `Owner::pairs` does; a range's expected
election, each pair charged the mean of its ranges', as `Owner::measure` does; and why the shard
stopped (a fenced range). The node's shard sends without waiting; the shard waits for room out,
its next turn held. `Node::turn` takes at most a lane's capacity from each lane
(`Node::take_shards`), and its wait's look (`Node::answered`) polls each lane's receive, so what a
shard gives, or its end, wakes the node's task; a lane whose shard's task ended stops the node,
`NodeError::ShardEnded`, and its next start recovers the ranges from the log. A proposal a far
range refuses is lost as one its leader drops, its asker's patience ending it, until step 4 answers
each ask in its slot. Until step 5 (below), production started no shard's task. Tested (`node::shard`): a one-member
range moved before its first drive to the second shard of a two-shard runtime is driven by that
shard's task and answers a proposal; a replica with a write out is refused a move; a full lane
refuses, typed; and a shard whose task ended, its lane's end dropped or its runtime shut down, is
reported ended once what it gave is taken. Left: a far range's reads, snapshots and status (2b),
and its membership, stop, repair and the host's reads of it (2c).

*As built (2026-11-18), step 2b: a far range's status, reads, answers, compaction and
snapshots*
(`crates/node/src/node/shard.rs`, `Node::seen`). What the node reads of a range's member, its
term, leader, leading, commit, applied, stall, writes, fence, held reads, timing and whether its
detectors trust a quorum (`shard::Seen`), is read from the replica where the node's own task
drives it, and where another shard does, as that shard reported it after the range's latest drive
that changed it (`Out::Seen`, and taken when the range moves). Reported rather than asked: the
node reads it on its hot path (`is_leader` for each command it replies, `leader_of` for each
redirect), and a report costs the shard a compare per drive where an ask would cost a round trip
per read; so `status`, `is_leader`, `applied`, `patience`, `leader_of`, `judged_quorum`,
`caught_up` and `applied_total` serve far ranges unchanged. A peer's read of a far range is asked
over the lane (`In::Read`: its context and its query) when the range leads as last shown; the
shard asks its replica (`Replica::read`), keeps the query while a quorum confirms it, at most a
lane's capacity of them, answers it from the range's engine once the range applied through it,
and hands the answer back by its id (`Out::Served`), which the node replies as `serve_reads`
replies its own; a read its replica refuses, the shard holds no room for, or whose range stopped
leading before it was confirmed (its core then never gives it back) comes back refused, and the
node refuses or redirects it as `ask_read` does. An answer a far range applied is given to the
command waiting for it as it comes (`answer_command`), and the turn's drive replies it. A round
of compaction (`Node::compact_if_stalled`), asked when a range anywhere stalls for room in the log,
as its shard last showed it, or the stream's write is owed, runs on each shard with that shard's
waker (`In::Compact`, `shard::compact`, the node's own ranges compacting with the same function),
since one log holds every range's entries and room any range frees serves each; a shard's round
that started one is counted (`Out::Compacted`). A snapshot a far range sends leaves its lane as
any message does, `Node::send` opening its exchange, and one arriving for it is stepped there as
any message is; the end of one it sent, which `report_snapshot` tells its member, is held on the
far range, at most one a member, a later end superseding an earlier, and handed to its shard at
each turn while the lane has room (`Node::report_far`, `In::Reported`): a member that missed it
would wait in hyper-raft's snapshot state for that peer for good (`handle_snapshot_status`). Each
new lane message keeps the lanes' bounds. A full lane: a read is refused, its asker asking again
after its pause, as a busy leader refuses one; a round of compaction is asked again at the next
turn; a snapshot's end is held for the next turn; the shard's reports and served reads wait for
room, as all it gives does. Tested (`node::shard`): a far range's leading, term, leader
and applied entries are seen from the node's shard; a read is served from its engine through the
lanes, and one of a range the shard holds not is refused; a far range's applied answer reaches the
command waiting for it; a round of compaction runs on the range's shard, its write answered
there; and a leader on one shard compacted past a learner on another sends it a snapshot out of
its lane, which the learner installs from its own, the snapshot's end reported into the leader's,
which then sends it entries. Left for 2c: the host's own reads of a far range (`Node::read`), and
its membership, stop and repair.

*As built (2026-11-18), step 2d: a peer's requests of a far range, a lost peer, and the owner's
handles* (`crates/node/src/node/shard.rs`, `Node::take_request`, `Node::unkey`). A peer's command
or read is read by what the node's member of its range shows (`requested` over `Node::seen`), so
a far range that leads as its shard last showed it takes it as the node's own do: its body is
proposed over the lane (`shard::propose`, `In::Propose`) or asked there (`shard::ask_read`), and
a range shown not leading redirects. A full lane refuses a proposal `LaneFull`, as the range's
replica would refuse it, which a peer hears as `Refused` and asks again after its pause and the
node's own entry pauses; a shard that ended stops the node from a proposal as from a read. A
peer's lost connection (`Node::unkey`) is told to each shard whose far ranges share their group
with the peer (`In::Unreachable`), which reports it to each such range (`report_unreachable`): its
leader gives up the window it sent that peer ahead of its answers and probes it again (hyper-raft's
`MsgUnreachable`, etcd raft's `ReportUnreachable`). Held on the lanes, each peer once, while the
lane is full, and told at the next turn's take (`Lanes::tell_lost`), as a snapshot's end is:
dropped, the leader would wait on that window until the peer's answers after a new connection undo
it, and holding costs one entry a peer, at most the node's peers. A range that moved keeps its
handle in shard 0's owner, which then names nothing; the handle is a slot and a generation that
wraps at `u32`, so that owner (`shard::Here`) keeps the handles that left and its insert never
gives one of them, taking a replica out and in again past each (at most once a handle), refused
typed (`NodeError::Aliased`) past that. Tested (`node::shard`): a peer's command and read of a far
range are served through its lanes, and a full lane refuses a proposal; a lost peer is told once
its lane has room, each once, and a far range's shard takes one and goes on; and no insert gives a
handle a range that left holds. Left: the membership's reads and steps of a far range (the root
range's rows, configurations, transfers and caught-up checks), its replacement, its repair, and
the host's table reads (2e).

*As built (2026-11-18), a far range's configuration, its steps toward the members, and its
replacement* (`crates/node/src/node/shard.rs`, `crates/node/src/node/members.rs`). What the
membership reads of a range's configuration (`Node::configuration`, whether the root range keeps a
member as a learner, whether any range names a member) is read, for a far range, as its shard
reported it after the drive that changed it (`shard::Configured`, `Out::Configured`, kept on
`Far`), as its status is (2b): the configuration it applied, whether a change of it is pending,
and, while it leads, each learner's progress: the commit it last said its storage holds, by which
the root range's leader keeps a removed member a learner until it holds its removal (membership.md
§4 step 5), and whether it is caught up, by which a learner is made a voter (§6). A voter's
progress is not reported: it moves with every entry, which would add a report at nearly every
drive under load, and only a hand-over's heir reads it, which the shard chooses. The root range's
leader's duties read whether it leads as its member shows it (`Node::is_leader`). Each turn, for
each far range shown leading, with no change pending, no hand-over under way and no replacement,
the node works the target out from the cell's rows and what the range showed, and hands it to the
range's shard (`In::Toward`) only where a step is due by what it showed (`Toward::step`, each voter
taken as caught up), so a range at its target, or one waiting on a learner, costs its lane
nothing; the shard steps it from what its replica shows then (`shard::step_toward`, by which the
node steps its own). A full lane drops it: the target is worked out afresh at the next turn. A
replacement of a far range (`Node::replace`) is held on `Far` until its lane takes it
(`In::Replace`), at the next turn's take while the lane is full, as a snapshot's end is, since one
dropped would never progress; the shard keeps it, steps it at each of its turns while the range
leads there (`shard::step_replacement`, by which the node steps its own) and hands it back done
(`Out::Replaced`), which `Node::take_replaced` reports as the node's own; one the node was told
before the range moved goes with it. The shard's reports wait for room, as all it gives does.
Tested (`node::shard`): a learner added through a far range's lane is seen in the configuration it
shows, with its progress; a voter is added to a far range and removed from it through its lane,
node 2 learning, made a voter once caught up, then removed, the test standing in for the node's
turns; and a far range's replacement, told before the range led, progresses to done and is handed
back. The root range's rows the membership reads (its members, their presences, the grants and
the nodes holding volumes; `members::Rows`) are read once for each entry it applies: by the node
from the root range's engine where its own task drives it, and, where another shard does, by that
shard, which hands them over at the range's first drive and after each drive in which it applied
an entry (`Out::Rows`), kept on `Far` until the node's next read (`Node::read_members`), the latest
superseding the rest; a shard knows the root range by its machine's layer (`RangeMachine::layer`).
The read costs what the node's own does, once a drive rather than once a turn, and one lane
message. Tested too: a far root range hands its rows over at its first drive and as of each entry
it applies, and a name range hands over none. The host's volume table of a far root range
(`Node::table`) is asked of its shard rather than handed over after each entry, as the table holds
every volume of the cell and the host reads it far less often than the range applies: `In::Table`
asks it once the range shows it applied past the table the node holds, one ask at a time
(`shard::ask_table`), and the shard answers from the range's engine as of the entry it applied
(`Out::Table`: at most the table's census of volumes, the read the node's own path makes). The node
reads the table it holds meanwhile, read before the range moved (`Node::spread_to`), and builds
the next from the answer at its next read (`Table::of`); a full lane leaves the table unasked, asked
again at the next read. Repair's reads of a far Block range's rows (the next block, the next block
on a volume, a block's places; `shard::BlockRead`) are asked of its shard one at a time
(`In::Block`, `shard::read_block`) and answered from its engine (`Out::Block`: one block, or one
block's places). Each answer is taken once, at the turn it comes, and let go at that turn's end if
the walk took it not, so none is read a turn past its arrival. The walk waits at the read
meanwhile, its answer waking the node: a block whose places are asked is held as a deferred block,
and the retiring of empty volumes keeps the service from idling until it went through every one.
A full lane leaves the read unasked, asked again at repair's next turn. Tested too: a far root
range's table is answered as of the entry it applied, not asked while the range applied nothing
past it, and asked again and answered with the volume an entry enlisted once it applied; repair's
three reads of a far Block range answer as its engine answers them on the node's shard, one at a
time; and a full lane refuses the table and each read, `LaneFull`, unasked.

*As built (2026-11-18), step 5: the rule over the runtime's shards* (`Node::spread`,
`process::serve`, `derive::shard_shape`, `bounds::Stated::lanes`). A node's process places its
ranges over its runtime's shards that take tasks (`NodeSettings::shards`: its first and each past
it a holder names), and moves each range placed past the first to its shard after `Node::open` and
before the node's first turn (`Node::spread`, each shard's `Node::spread_to`); a shard placed no
range starts no task. A refusal stops the start, typed, at its network step: a shard placed ranges
with no holder (`ShardEnded`), a replica with a write out (`WritesOut`, which no replica has
before its first drive) or a shard with more ranges than its lane holds (`LaneFull`, which the
sizing below rules out); its ranges would be placed where nothing drives them. A shard's task
states, as the node's own shard does, that it may wait on the device (`permit_blocking`): its
replicas apply through the engine's synchronous calls (§1.2). Sizing, derived: hyper-rt sizes every
shard alike, so a shard's tasks are the larger of the first's (the node's task, its launcher and
each S3 connection, which falls back to it) and another's (its ranges' task, the listener's taker
and each connection handed to it), both the connections and two with a listener, and its timers
the larger of the first's (the node's task's and each connection's) and another's (its ranges'
task's wait and each connection's). A lane holds the exchanges the transport admits at once
(`parked_most`), at least four a range (each peer's two connections, each two streams a range,
§3.3), so never fewer than the ranges on any one shard. The memory floor's runtime counts each
shard past the first that runs ranges with its two lanes (`node::shard_lanes_bytes`: a ring place
and a room place for each value, at the types a process runs). `lanes_per_peer` is a lane for each
shard that runs ranges, at most one a range (`node::lanes`), and the transport's messages' bytes
count, for each lane it may open, a window of frames out to each node its ranges replicate to
(`bounds::Stated::replicas`; a gateway is sent none) and one frame in from each peer; the node's
task still sends every range's
messages, its own and those its shards hand back, on its one lane, so the lanes a peer may open
bound what a peer sends, not what this node does. A word of the stream (a change, or its belief of
a peer a range came to share its group with) a shard's lane has no room for is held, in order, and
told at the next turn's take, at most a lane's capacity held (`Lanes::tell_stream`): the cluster
suite over two shards found a lane full of Raft messages under a cut leader's return stopping the
node, `LaneFull`, where the stream's change met it. Tested: the cluster suite runs each node over
`MANTLE_NODE_SHARDS` shards of a hyper-rt runtime, the node's task on its first and its range
placed on its second, checked on open, and the same laws hold over one shard and two; `node::shard`
holds a stream word through a full lane and tells it in order; `node::spread` counts the shards
that run ranges as the rule fills them.

*Measured, and placement held to the first shard (2026-11-18).* Ladder r40 (lean-doors
`ec7519e1`, `adas-mac-studio.local-2`, closed ladder of 1 B operations through the native door,
`mantle bench doors`) placed each node's ranges over its shards against r28 (`54fda898`, every range
on the first shard): HEAD rose from 29,391/s to 63,189/s best (128 clients), but PUT fell from
2,779/s best at 1,024 clients to 641/s at 128, the ladder ending at 256 with 86 PUTs left
unanswered after `SlowDown`, and GET from 8,551/s to 2,190/s. A far range's proposal or read the
range refuses (a full lane, a range not leading) is dropped and left to its asker's patience
(step 2b, step 4's slots not yet built), and a range's shard also serves S3 connections while it
waits on the device (`permit_blocking`). So `process::serve` places a node's ranges over its first
shard alone (`NodeSettings::shards` one) until a far range's refusal is answered and the ladder
measures placement over the shards past r28; the cluster suite still runs nodes over two shards.

*As built (2026-11-18), the setting* (`process::RangeShards`, `range_shards`). Where a node's
ranges go is a mode its operator states, so a ladder measures both arms without a change to the
code: `range_shards = first`, every range on the node's first shard (`NodeSettings::shards` one),
or `range_shards = spread`, the runtime's shards that take tasks, as step 5 placed them. It is
`first` where unstated, as ladder r40 measured placement over the shards slower than r28 (above),
until a ladder measures `spread` past it. Both `mantle serve`'s file (`process::STATED_KEYS`) and
`mantle node`'s (`process::KEYS`) take it, any other word refused naming the key; `mantle serve`
says it as a basis, `basis range_shards = first (cited: …)` where unstated and `(stated: …)` where
stated. `mantle bench doors --range-shards first|spread` states it in each founder's file of the
cell it makes (`transfer::Cell::make`, `first` by default and for `mantle bench transfer`), and
its `doors run` line says it. Tested (`process::tests`): the key reads `first` and `spread`, is
unstated where the file names it not, and refuses another word; `derive::tests` reads it back from
the bases as the node runs with it.

*As built (2026-11-18), step 4's smallest form: a far range's refusal answered at once*
(`crates/node/src/node/shard.rs`, `Node::refused_far`). Each proposal the node makes is given an
ask (`Node::propose_asked`), kept with the command waiting on it (`Commanded::ask`) and carried
over the lane with the entry (`In::Propose`). A proposal the far range's replica refuses (it leads
not, its lead is being handed over, its uncommitted entries are at their bound, or the shard holds
it not) is answered by its ask in the turn that took it (`Out::Refused`, `shard::Refusal`), no
longer dropped to its asker's patience: the node takes the command waiting on that ask and answers
it as `propose_command` and `propose_own` answer the node's own replica's refusal, a peer's command
`Malformed` for an entry past the range's bytes, else `Refused` while the range shows this node
leading and redirected while it does not, and the node's own entry refused for what it is, or
paused and asked again. A refusal no command waits on any longer, its range having stopped leading
meanwhile (`Node::reply_commands` answered it), answers nothing. A read was already answered so
(step 2b: `Out::Served` refused, as `ask_read` refuses one), and a full lane refuses either at
once on the node's shard. The answer rides the shard's out lane, which waits for room rather than
dropping, so it cannot be refused; a turn gives at most one refusal for each proposal it took, a
lane's capacity and the one that woke it, so what a shard gives stays bounded. Tested
(`node::shard`): a proposal to a far member that leads not is refused by its ask at once, and one
past the range's bytes refused for what it is, the command waiting on each taken by its ask alone;
and a read asked of a far member that leads not comes back refused at once.

*As built (2026-11-18), the lanes' counts* (`Node::lane_counts`, `process::lane_counts`). So a
ladder shows where placement's time goes, `mantle serve`'s `counts` answer says, since the node
opened, the sends a full lane refused by the kind of what was sent (`lane_full_KIND`, each kind of
`shard::KINDS`, named by one match over `In`'s variants, so a variant added fails to compile until
it has one), the proposals and reads far ranges refused (`far_refused_proposals`,
`far_refused_reads`; a refused proposal was counted proposed too) and the stream's words held for
want of room (`stream_held`); `mantle bench doors` says each phase's growth of them, every server's
summed (measurement.md §12). Tested: the answer says them by name (`process::tests`), the harness
reads them back (`transfer::cell`) and sums each server's growth (`bench_doors`), and a full lane's
refused proposal is counted under its kind alone (`node::shard`).

*Open (2026-11-18): S3 connections beside a node's ranges.* Under `range_shards = spread` a shard
that runs ranges also takes S3 connections: the listener hands each to the shard past the first
with the fewest open (`listener::shards::least`), passing over one the watchdog sees held past its
limit, and a range's task there waits on the device (`permit_blocking`). Neither rule is changed
yet, for three reasons. Ladder r40's fall was the native door's, whose requests the node's task
drives on the first shard (step 6 not built), which S3 connections never reach, so neither rule
explains it; the HTTP door's ladder over `spread` is the measure that decides. Holding S3 off the
shards that run ranges trades those device waits for the S3 capacity those shards would serve, in
a share the host decides, five shards of 31 past the first on 32 cores but five of seven on eight,
so it is a rule only once that ladder measures both. And the reverse, ranges kept off the shards
that serve S3, is no rule: ranges are placed once, at open, and connections at each accept, so only
the listener's rule can follow both. Next, the HTTP door's ladder over `spread`, S3 over every
shard against S3 over the shards with no range where any remain (`listen`'s holders less those
`Node::ranges_on` names), chooses the listener's rule.

*As built (2026-11-18), a range shard's task Critical.* A shard's task that runs ranges past the
first is spawned in hyper-rt's Critical class, as the node's is (`run_on_first_shard`; hyper-raft
docs/runtime.md §3.15), so woken by a Raft message, a proposal or its log, its ranges' Raft and
apply go ahead of the S3 connections queued on its shard, which stay Normal. A submission through
a shard's control queue takes Normal, so `Lanes::start` submits a launcher that spawns the task
Critical there and ends (`shard::submit_critical`); a shard that refuses it drops the task and its
lanes' ends, which the node reads as the shard's end, as before. The launcher holds a slot while it
spawns, so `derive::shard_shape` counts it beside the task on a shard past the first under
`spread`: with an S3 listener that shard holds one task more than the first, and every shard is
sized so; and the floor counts the classes each such shard's queue then keeps
(`derive::CRITICAL_BYTES`, `CRITICAL_SLOT_BYTES`). Under `first` no shard past the first runs a
range, and neither changes. Tested: the task is spawned Critical and what it spawns Normal
(`node::shard`); the floor holds the runtime with every shard past the first running ranges
(`tests/shard_memory.rs`). What it buys in commit latency under S3 load is the `spread` ladder's
to measure.

*As built (2026-11-18), step 6's first part: the rule and its count* (6a,
`crates/node/src/node/drivers.rs`). `Drivers` holds the native requests running on each of the
node's shards, numbered from its first, and places each request the host admits, taken at once or
from those parked (`host::start_object`), by the rule of 6a: the shard past the first running the
fewest, the first of those tied, and the first where it is the only shard, as
`listener::shards::least` places an S3 connection. The job keeps the shard it was placed on
(`Job::driven`) and gives its place back as it ends: at the turn it is over (`Node::step_host`),
or as its exchange ends unanswered (`Node::job_lost`). A request answered at once (refused,
malformed or slowed) and an S3 door's operation are placed nowhere. The node holds it over the one
shard its task drives every request on (`NonZeroUsize::MIN`), so each request is placed on the
first, where it runs, until 6f places them over the shards `NodeSettings::shards` names, passing
over one hyper-rt's watchdog sees held past its limit, as the listener's lanes do
(`Lane::takes`). Bound: an entry a shard, and each count at most the host's jobs, each request
placed being one; a count past a `usize`'s places nothing, and the request is answered `Failed`,
one the host cannot run. Tested (`node::drivers`): a node of one shard drives every request on
it; a request goes to the shard past the first running the fewest, the first of those tied, never
the first while another exists; one that ended gives its place back to the rule; every shard past
the first stays within one request of every other, from one shard to nine and one request to
forty; an end where none runs counts nothing; and a count past a `usize`'s is refused, nothing
placed. The host places and gives back inside a `Node`, which no unit test builds; the native
suite (`crates/mantle/tests/native.rs`) runs both end to end.

*As built (2026-11-18), step 6's check: what a driving shard is handed may cross threads.* What
6b to 6f hand between the endpoint's shard and a shard that drives native requests is `Send`: a
request's operation (`object::Op`), and its door's request, pieces and answers (`door::Request`,
`Piece`, `Answer`), as the S3 listener already hands them across; a job's asks, a range's command
(`wire::Command`) or read (`read::Query`) and a chunk's bytes (`Bytes`) and key (`ChunkKey`); their
outcomes (`CommandOutcome`, `ReadOutcome`, `ChunkOutcome`); and the volume table puts are placed by
(`host::Table`). The gateway's settings, its root keys among them (`GatewaySettings`), are `Sync`
too, so every shard's host may share one;
`node::tests::a_native_requests_work_may_cross_to_the_shard_that_drives_it` holds this at compile
time. A job's drivers (`Put`, `Get`, the bucket's `Coordinator`) need not cross: a request is
handed over once its fields are read, before its work is made (6c, 6f), so they are made on the
shard that drives them. So step 6 needs no change in hyper-raft for what crosses.

*As built (2026-11-18), step 6's second part: a job's asks as values* (6b,
`crates/node/src/node/host.rs`). Each ask a job makes of the node is one value, `host::JobAsk`:
a range's command or read, its group and its command or query, asked as a gateway asks one, until
the range answers or its budget passes without a leader (`Wait::Request`), as every job asked
before; or a chunk's put or get on a volume, of the node the volume table names as holding it,
within the job's patience. `Node::ask` serves it by `Node::command`, `Node::read`,
`Node::put_chunk` or `Node::get_chunk`, as the job called them, and says what the job waits on
(`Pending`); `Node::outcome` takes that outcome once answered, which `take_answers` gives the job
and `drop_abandoned` drops for a job that ended, and `Node::has_outcome` says whether it came
(`host_due`). The bounds are as they were: each kind held to the node's (`command_asks`,
`read_asks`, `chunk_asks`), one past it refused `NodeError::Asks` and asked again at the job's
next turn. The S3 door's key lookups (`host::step_lookups`) are no job's asks and keep the node's
read. Every job is still driven on the first shard. Tested: the host's and the node's unit tests
pass over it unchanged, as nothing a job asks or is answered changed, which no test can show red;
`node::tests::a_native_requests_work_may_cross_to_the_shard_that_drives_it` holds the value
`Send`, so a lane may carry it (6e).

*As built (2026-11-18), step 6's third part, its first half: a native put's body as the S3 door's*
(6c, `crates/node/src/node/host.rs`). A native request's fields are read by the node's task as
before (`read_fields`); once they name a put (`Op::Put`, `UploadPart`, `Append`), its job's door
becomes `Door::Fed`: the job takes its body from a bounded channel (`host::Pieces`) as an S3 job
takes the listener's, through one function both doors' puts are fed by (`take_body`, which
replaced `feed_put`'s read of the exchange and `feed_local_put`), and the node's task keeps the
exchange's half (`host::Feeding`, `Node::feed_bodies`, after the host's step at each turn): it reads
each piece the transport delivers as `feed_put` read it, gives the object's bytes as
`door::Piece::Bytes`, keeps the trailer's value past them, and gives `Piece::End` carrying the
trailer's checksum once the transport verified the body whole (`body_complete`), as an S3
connection gives a body's end; the bytes read with the fields stay the job's carry, the trailer's
among them the feeding's. Bounds: the channel holds one piece, the least that moves a body
(`door::Depth::FIRST`), and a piece is read only once the job let go of the last, so while the
job's driver takes nothing, nothing is read and the stream's flow control holds the client, as when
the job read its exchange; an end the channel has no room for is held, one, and given once the job
takes the last piece; one feeding a native put, each one of the host's jobs. Copies: a piece is
copied once out of the transport's reservation, into storage of the feeding's own (`BytesMut`)
that the piece the job takes is a view of (`Bytes`), where the job copied it into the bytes it
carried; then once into the driver, as before. The storage is made at the first piece as large as
what is left of the object, at most a piece, and reused once the job let the piece go, reclaimed
whole (`try_reclaim`), so no piece allocates past the first, as no piece did; what a put asks
once more is its channel and the job's end of it in its door. The transport gives no
owned bytes of a body: `Endpoint::read_body` copies QUIC's chunks into its owner's reservation, so
handing QUIC's own chunks over, a copy fewer, is a change to hyper-transport, not made here. What
the job did is kept: a job that lets go of its body, refused or done before its end, drops its
channel, the node's task reads and drops the rest, and the job's reply goes once the body is read
whole (`drain_job`); an exchange that fails tells the job `IncompleteBody` on its channel, and its
end (`Node::job_lost`) lets both go; the job's vouching, its patience and its `SlowDown`s are as
they were, and its reply is still written to the exchange by the job. A feeding that moved makes
the node's next turn due at once (`host_due`), so the job takes what it was given there: a native
put's body moves a piece a turn, where its job read as many as its driver took in one, the S3
door's pace at its first depth. Tested (`node::host`): a body's pieces reach the driver in order and
whole, its CRC-32C trailer holding, and a trailer that does not hold is refused (`BadChecksum`); a
full channel holds the stream's bytes, the next piece read only once the job let go of the last,
into the same storage, and an end held behind it given once it is taken; a stopped or reset stream
ends the job `IncompleteBody`, and a job that let go has the rest dropped; and 63 pieces past the
first ask the allocator for nothing, as the job's own read of them asked nothing (0 and 0
allocations, 0 and 0 reallocations), each a view of the one storage. No node test runs the object
host (each opens its nodes with no gateway), so the native suite (`crates/mantle/tests/native.rs`)
is the end-to-end test, and the ladder its measure. Left then, 6c's second half: the reply, the
drain and the exchange's end (below).

*As built (2026-11-18), step 6's third part, its second half: a native reply as the S3 door's*
(6c, `crates/node/src/node/host.rs`). Every native request the host takes gets its door as it is
taken (`host::native_door`, in `start_object`): the job's ends (`host::Native`) and the node's
task's (`host::Replies`, held in `Node::replies`). The job gives its reply on a bounded channel as
an S3 job gives its connection `door::Answer`s (`host::Told`): a reply with no bytes past its fields
(`Told::Reply`, every refusal and every operation's end but a get's), its head and fields encoded
by the job (`Reply::encode`) with its `SlowDown`'s cause; a get's held reply (`Told::Head`, from
`send_held`), its head carrying the range's length (`getting.total`); a get's pieces as its driver
gave them (`Told::Bytes`); and its end (`End`, or `Failed` past its head). The node's task writes
them, after the host's step and the bodies' feeding at each turn (`Node::write_replies`): a
`Told::Reply` once the body's feeding ended and the rest of the body is read whole and dropped
(`drop_body`), as `drain_job` read it, then `Endpoint::reply` with the fields' length, the
`SlowDown` counted for its cause as it goes, the fields, and the exchange's end; a get's head with
the fields' length and the range's, then its fields and each piece by `Endpoint::write_body_bytes`,
kept by QUIC by reference, then `end` or, for `Failed`, the exchange refused. `drain_job`,
`write_get` and `send_held` give to the door and write nothing; `write_fields` and `Work::Replying`
are gone. Vouching goes the same way: a held job says so on its door (`Native::held`), and the node's
task tells the exchange's asker at its next write (`Endpoint::working`). The bytes a native job moves
(its body's pieces taken, its range's pieces given) it counts in its door, and the host's step takes
them as a value into `Node::moved` (`Node::turn_job`), so a native job touches nothing of the node's
but its door; the fields the node's task writes and the body it drops it counts itself. The S3
door's depth is sampled only by S3 jobs, which run on the node's task (§4.1), as before.

Bounds. The channel holds one answer, the least that moves a reply (`door::Replying::FIRST_ANSWERS`),
and a job gives its fields or a piece only once the node's task wrote the last whole: the two share
the count of what is given and unwritten (`host::Handed`, atomics, as `door::Learned` is shared with
the S3 connections), so a get holds one piece taken from its driver past its window, the one
`derive::Charges` counts ("a get's block taken and written out"), as when its job wrote its
exchange; a deeper channel at the learned reply depth would hold pieces past that charge. An answer
the channel has no room for waits in the job's door (`Native::pending`), at most its end behind its
last piece, so a full exchange holds the job, and the client, whose stream's flow control it is,
holds both: nothing grows. `Node::replies` holds one a native request the host took, each let go
with its exchange (`job_lost`), or once its reply is written whole or refused, so at most the
exchanges the transport holds; one the node's task lets go tells its job (`Handed`'s `gone`), which
ends. The node's stop waits for every reply written as for every job ended (`step_stop`). A job's
reply channel that cannot be made refuses its exchange `Budget`.

Copies. None added: a get's piece reaches `write_body_bytes` as its driver gave it, where the job's
own write copied it into QUIC's buffer (`write_body`), so a piece is copied once fewer; a reply's
fields are moved into `Bytes`, not copied.

Pace. A get's pieces do not go one a turn: a piece is given only once the last is written whole, so
where one is written whole and its exchange takes more, the node's task asks the job for the next
within the same turn (`Wrote::More`, `Node::write_replies`), as many as the exchange takes, at most
a window's (`GatewaySettings::window`, the pieces the driver holds read at once) and its fields and
end past them, a turn whose rounds ran out making the next due at once (`Node::replied`, `host_due`).
So a large get takes the turns its job's own writes took: each turn writes what its driver has ready
as far as the stream's credit goes, the stream's window at most (`stream_window_ceiling`, about
2.3 MB), and its end in the turn its last byte is written. A reply with no bytes past its fields goes
in the turn its job ends, as `drain_job` wrote it; the job now ends as it gives it, as an S3 job ends
as its answers are taken, the drain and the write the node's task's.

Tested (`node::host`): a native get's reply reaches its exchange whole and in order, its head's
length the fields' and the range's, in the turns its exchange's credit takes (5,000 bytes a turn: the
reply's bytes over 5,000), and in one turn where the exchange takes everything; a driver failing past
the head refuses the exchange; a `SlowDown` waits for the request's body, then goes whole, counted
for its cause; a full reply channel holds the job, which takes no piece from its driver while one is
unwritten, and an answer past the channel's room waits in its door in order; a held job is vouched
for through the node's task, once each time it says so; a stopped stream refuses the exchange and
ends the job, and a job that let go leaves nothing to write; and 63 pieces past the first reach the
exchange each in its own storage, asking the allocator for nothing (0 allocations, 0 reallocations).
The host and node unit tests pass unchanged. No node test runs the object host, so the native suite
(`crates/mantle/tests/native.rs`) is the end-to-end test, and the ladder its measure.

What 6d needs first: the host's step (`step_host`, `turn_job`) and its jobs move into `host::Host`
with their own counts (`moved`, `slowed` by cause, both now applied as values), its asks served by
the node as 6b made them; `Node::write_replies`' ask for a get's next piece becomes the node's task
stepping the host's reply step for one job, and `read_fields`, still a job's step that reads its
exchange, becomes the node's task's before the job is made (6f), as `drop_body` already is.

*As built (2026-11-18), step 6's fourth part, its first half: the host's state, and its doors kept*
(6d, `crates/node/src/node/host.rs`). The jobs the node's task runs, the bytes their work moves and
the doors of native requests are a `host::Host` the node's task holds (`Node::host`). `Node::moved`
sums the node's task's count (fields read, bodies dropped, a trailer's value, a reply's fields
written, chunks sent and received) and the host's (a body's pieces taken and a get's given, on
either door). The `SlowDown`s by cause stay the node's: since 6c its task counts each as it writes
the reply that carries its cause (`Opening::slowed`), and it writes every native reply wherever its
job runs, the S3 door's counted by its listener. Admission, parking, the put window, the rule's
count, the key lookups and the asks' outcomes stay the node's, `abandoned` among them, which repair
shares; the node's step of the host (`step_host`, `turn_job`) still reads each job's end and tells
admission and the rule, until the steps move (the second half, below).

The doors kept (`host::Doors`). A native request's door, its reply's channel and what its two
halves share (`Handed`), was made as the request was taken and dropped as it ended, even for one
refused at once: 5 allocations (2,792 bytes) for the channel and what its halves share, a sixth
boxing the job's half (`Native`) and a seventh at its first answer, its queue of held answers. Now
the host keeps them: a request takes a door kept, one made only where none is; each half is given
back as it is let go, the job's as its job ends (`turn_job`, `job_lost`), the node's task's once its
reply is written whole or refused or its exchange ended (`write_replies`, `job_lost`); and a door
both of whose halves are back is emptied of what its job gave that the node's task never took, its
words cleared, and kept with its storage, its queue's among it. A kept door closes neither end of
its channel, so its words say what the ends' drops said: `Handed::gone`, the node's task let go,
which the job's `flush` reads before it sends; and `Handed::ended`, the job let go, read before the
channel, so the node's task's write, finding the channel empty, ends the reply rather than waiting
on it. A half dropped still closes its end, as before. Bound: the doors kept are at most the
exchanges the transport holds at once (`parked_most`), as each door out is of an exchange the node
holds, one given back past them let go; a half given back first waits for its other, then still out
in a job or a reply of a request the node holds. A put's body channel (`host::Pieces`, `Feeding`) is
still made for each put: kept the same way, its ends' drops would become the door's words too, in
the body's code (`take_body`, `Feeding::send`), which upload-stall changes now.

Tested (`node::host`): past its first request, 64 requests each take the one door kept, whichever
half comes back first, the job's end given and written through it, and ask the allocator for nothing
(0 allocations, 0 reallocations), where making a door asks it 5 times; a door given back with an
answer the node's task never took is emptied, its next request's job not told its reply is gone nor
its node's task that its job ended; and a host holding two exchanges keeps two doors, the third
given back let go. The host and node unit tests pass unchanged.

What the second half needs: the steps move into `impl Host`, each served by the node through one
interface a shard's task can drive later (its asks, their outcomes, the volume table, the ranges'
groups, the wall clock, a local door's pacing, the put window's samples and each job's end, its
admission's charge, its shard and its asks let go); the node's step of the host takes the host out
of `Node` for its turn (`std::mem::take`, allocating nothing) and hands it back; `write_replies`' ask
for a get's next piece steps one job of the host; and a job's fields (`Work::Fields`, `read_fields`)
are read by the node's task, served as one of those asks until 6f reads them before the job is made.

*As built (2026-11-18), step 6's fourth part, its second half: the steps* (6d,
`crates/node/src/node/host.rs`). The job steps (`Host::step`, `turn_job` and every job's step:
its asks, its answers, its reply given to its door and its end) are `impl Host`'s, each generic
over one interface, `host::Serve`, which the node implements and a test's stub implements too: a
job's ask and its outcome (`ask`, `outcome`, as 6b made them), the asks a job let go (`abandon`),
the volume table (`table`), the ranges' groups and their count (`group_of`, `ranges`), the room
and bound for commands (`command_room`, `command_asks`), the wall clock (`wall_ns`), whether room
at a volume or in the transport was freed (`freed`), the node's own count of bytes moved
(`moved`), a native job's fields read from its exchange (`read_fields`, the node's task's until 6f
reads them before the job is made), the S3 door's pacing samples (`took`), the put window's
samples (`committed`, `filled`), and each job's end with its admission's charge, its shard and the
asks it let go (`job_ended`), by which the admission learns its latency, the stream pool is sized
again and the rule's count is given back. The node's task lends the host out for its step
(`Node::lend`): `std::mem::take` leaves the default host in its place, which holds nothing and
allocates nothing, and the host is put back whatever the step came to, an error among them; the
node drops the outcomes of asks whose jobs ended and steps the S3 door's key lookups first, as
before. `write_replies`' ask for a get's next piece steps that one job through the same lend. The
gateway's settings are one `Arc` made with the node and shared with its host, not made per turn.
The node keeps admission, parking, the put window, the rule's count, the key lookups, the asks'
outcomes and `abandoned`, which repair shares, each exchange's fields read, body fed and reply
written, and `host_due`, `hosts` and `job_lost`, which read the host between its turns.

The doors kept (6d's first half) are at most one an operation the host runs at once
(`GatewaySettings::objects`), within the exchanges the transport holds (`parked_most`), and the
memory floor's records of a native operation (`host::record_bytes`, `derive::native`) count one
door kept beside the operation's own: its place in the host's keep, its job's half boxed and what
its halves share. A put's body channel (`host::Pieces`, `Feeding`) is still made for each put:
pooling it changes `take_body` and `Feeding::send`, which upload-stall's body reading rewrites
(`43357e2d`, a body read into the piece its admission charged); it is pooled once that merge lands
(below).

Tested (`node::host`): the host and node unit tests pass unchanged; a host driven only through
`Serve`, by a stub that answers each ask at the host's next turn, steps a put and a get to their
ends: the put of an empty object takes its body's end from its door, asks its Name range to commit
it and is answered `Done` with the version written, the get reads its current version, an empty
one, makes its driver over it and is answered `Done`, and each job's end is told with its charge,
its shard and no ask let go; and the lend asks the allocator for nothing (0 allocations, 0
reallocations), the host given back with its jobs, its doors and its gateway. The stub drives the
S3 door's jobs, whose steps are a native job's: a native door's exchange (`ExchangeId`) is made
only inside hyper-transport.

What 6e needs first: the host's wait split from the node's (`Node::host_due` reads the host's
answered asks, its puts' renewals and its held jobs' bounds beside the node's lookups, feedings and
replies), so a shard's task waits on its host's alone; and a `Serve` of the driving shard's over
its lanes, each ask sent with its slot (`hyper_rt::sync::oneshot`) and its outcome taken from it,
the host holding each kind to the node's bound for it, the latest volume table handed in held for
`table`, the ranges' groups handed with the task's start (a range's layer and group do not change
in a process), `read_fields` refused (a request is handed with its fields read, 6f), `took` none
(the S3 door's jobs stay on the first shard), and `freed`, `committed`, `filled` and `job_ended`
each a lane message, as is a chunk's piece moving a job's work (`Host::chunk_moved`), which the
node's task sees on its transport.

*As built (2026-11-19): a turn steps the jobs that have work, at one read of the wall clock*
(`crates/node/src/node/host.rs`). A turn of the host stepped every job it held (`Host::step`,
`turn_job`, `step_job`), and a put's step read the wall clock for its renewals
(`Serve::wall_ns`, `SystemTime::now`): profiled with macOS `sample` at about 5,500 1-byte PUTs a
second on a three-node cell on one Mac Studio (2026-11-19), the clock was 14% of each node's first
shard, 98% of it under `step_job`, about 6% of a core a node, which at 25-50 ns a read is on the
order of a million steps a second where each node answered under 2,000 PUTs: a put waits most of
its life on its chunks and its commits, and every turn of the node's task stepped it, found its
asks out and read the clock. Now:

- **A job rests while it waits on its asks alone** (`Job::rests`, `Job::resting`): a put whose
  bucket row is out, or one not over whose driver takes none of its body now (`Put::wants_body`)
  and holds no request for want of the node's room (`Putting::waiting`) and no chunk for want of a
  volume's (`Put::waits_for_room`). Its turn would feed it nothing, ask nothing and answer
  nothing, so a turn passes it by, keeping its keys (`Host::rekey`) for what its bound moves.
- **What can give it work wakes it as it comes** (`Host::wake`): an answer to one of its asks
  (`answer_job`), a reply given it, a refusal, `SlowDown` at its bound or a failure
  (`reply_job`), room offered to a chunk of its (`offer_room`), and its put's renewal coming due
  (`Host::step`, read from the earliest renewal kept, `renewals`). These are the events a driving
  shard's task already waits on (`Host::due`): an answer, a renewal, a beat or bound. Nothing else
  moves a resting put: its body is not read while its driver takes none (`take_body`), and its
  requests are drained by the step that made them (`step_put`). A job its door could move, a
  body's bytes or a reply's room, and every job but a put, is stepped at every turn as before.
- **The wall clock is read once a turn** (`Host::step`) and passed to each step (`turn_job`,
  `step_job`, `step_put`, `step_copy`, `ask_put`), as the shard's clock already was: a turn lasts
  microseconds, so a renewal is ticked and a placement weighed at most a turn late, as each bound
  read by the shard's clock is. `write_replies` reads it once for its turns of jobs, and only
  where one steps. A put's
  own time as it is made (`start_put`), a completion's and a copy's are still read once each, at
  one read an operation.

Tested (`node::host`): 64 puts of an empty body, each committed by one Name command a stub of
the node answers 32 turns after it is asked, are stepped twice each, 128 steps over 33 turns,
where every turn stepped every put, 2,112 steps (33 a put); the wall clock is read 33 times, once
a turn, where each put's step read it, 2,112 reads; and each put is answered `Done` with its
version, no wake lost. A put waiting on its body is still stepped at every turn, as are gets,
copies and the other operations: their doors' events (a body's bytes, a reply's room) reach the
host by no ask of its own, and waking each by its exchange is the next step.

*As built (2026-11-20): an S3 job waiting on its connection rests, its connection naming it*
(`crates/node/src/door.rs`, `crates/node/src/node/host.rs`). A get and a put waiting on its S3
connection, for the body's next piece or for room in its reply, was stepped at every turn of the
node's task, every job a turn: the listener's connection turned the node's task as it sent or took
(`Submitter::wake`) but named no job, so the host could only look at every one. At 10,000 GETs and
10,000 PUTs a second from up to 2,048 clients a cell, most of a job's life is spent so. A job's
step makes progress only from these sources, so each now names the job it concerns, as epoll's
ready list holds only the descriptors whose I/O moved (research/99 [1]) and explicit event
delivery costs a server by its events, not by the connections it holds (research/99 [2]):

| Job | What moves it | How it is woken |
| --- | --- | --- |
| any | an answer to one of its asks, a reply or refusal given it, `SlowDown` at its bound | `answer_job`, `reply_job` (§ above) |
| put or get whose chunk waits for room | room offered | `offer_room` |
| put | its renewal due | the earliest renewal kept (`renewals`) |
| S3 put taking its body | its connection sends a piece or the end | the request's name (`door::Mark`) |
| S3 get, or S3 last answers | its connection takes an answer, freeing room | the request's name |
| any S3 job | its connection lets go of it, answered or broken | `door::Named`'s drop names it |
| native job | its exchange's body or room | not named: stepped every turn, as before |

- **A request is named, once until the node's task takes the name** (`door::Mark`). The listener's
  `Submitter::submit` names each request (a `u64` count, unique for the door's life), and its
  connection names it on each turn of the node it makes: after sending a piece of the body, after
  taking an answer. A name already given is not given again until the node's task lets it go, so
  the marks hold each request at most once: a bounded channel of twice the door's connections
  (`door::doorway`), one request in flight a connection and one more whose job outlived its
  exchange. A name with no room sets `missed`, and the node's task wakes every S3 job, as a native
  mark with no room visits every reply (`Marks`). The connection's hold on the request
  (`door::Named`) names it once more as the exchange ends, answered or broken, so a job waiting on
  a connection that went is stepped and finds its door gone, not left to rest unbounded; it says
  the connection went first (`Mark::gone`), so a job stepped before the connection's channels
  close, as they do just after, rests on them no more and finds them closed at its next turn.
- **The node's task takes the names before the host's step** (`Node::take_door`,
  `Host::take_marks`): each name's job found in one probe (`Host::named`, slots by name kept with
  the job table, one a job, as `exchanges` is), its name let go, then woken. A job's name is let go
  before the step that serves it, so what its connection does after names it again: no event is
  lost between the name's taking and the job's step, only stepped twice. A name given before its
  job is pushed is let go as the job is (`take_local`), whose first turn steps it.
- **A job rests only once its step found nothing more** (`Job::rests`): a put whose driver wants
  its body found its door's channel empty (`Pieces::dry`, set by `take_body`) with nothing carried
  over; a get, or a job's last answers (`Work::Answering`), found its reply's channel full
  (`Local::pending` held); and, on either door, a get reads its version, or has no bytes to give
  while its reads are out. This is edge-triggered epoll's discipline, waiting for an event only
  after a read or write returned `EAGAIN` (research/99 [1]): an event that came before the step is
  seen by it, one after names the job. A job holding a request for want of the node's room, or a
  chunk for want of a volume's, is stepped at every turn, as nothing names it when room frees.

- **A turn's answers are taken for the jobs they are for** (`Host::step`): each answered ask's job
  found by its slot (`asks`), each once, where every job's asks were looked up in the node's
  answers at every turn, a probe for each ask in flight.

Bound: the names a turn takes are at most the marks' room; the host's slots by name, one a job,
are bounded by its job table, and a name whose job ended finds none and is dropped. The answered
jobs a turn takes are at most the asks answered, in a vector kept from turn to turn.

Tested (`node::host`, `door`): 64 S3 gets of an empty object, each reply's channel one answer and
its connection reading none, are stepped as they ask their version and as it comes (64 and 64),
then not at all for 32 turns, where every turn stepped every get (2,048 steps in those 32 turns,
measured with the old rule), then 32 and 32 as half the connections and then the rest take the
head and name their gets: 192 steps over 36 turns where every turn stepped every get, 2,272; each
answered its head and end. 64 S3 puts whose bodies come 32 turns late are stepped as they are told
`Ready` (64), not at all for 32 turns (2,048 steps with the old rule), then as each connection
sends the end and names its put (64) and as each commit is answered (64): 192 steps over 35 turns,
where every turn stepped every put waiting on its body, 2,240; each answered `Done`. 64 gets whose
version read is answered 32 turns after it is asked are stepped twice each, 128 steps, where every
turn stepped every get, 2,112. A connection names its request once until the name is let go, again
as it lets go of the request, and a name past the marks' room is missed and read once.

Open: a native job's door events name its exchange to the node's task (`Native::mark`), not its
job to the host, so native jobs waiting on their exchange, and every job on a driving shard
(woken by its shard's `Notify`, which names nothing), are stepped at every turn; so are commands,
bucket and admin operations, single reads, batch deletes, completions and copies. And each turn
still walks every job for its cheap checks: whether any waits for room (`offer_room`), its bound
and vouch (`bound_jobs`), and its resting flag and keys (`Host::step`'s pass-by, `rekey`), where
the earliest-deadline heap (`beats`) could find the few due and a ready set the few woken.

*As built (2026-11-20): a native job waiting on its exchange rests, the node's task naming it*
(`crates/node/src/node/host.rs`, `crates/node/src/node/driving.rs`). The open part of the note
above, for the native door. A native job's body and reply go through its door's channels, which
the node's task moves (6c): it gives a put's body piece by piece as its exchange's bytes arrive
(`Feeding`), and takes and writes a reply's answers as the exchange takes them (`Replies`). It
woke a driving shard's task at each of those moves (`Replies::wake_job`, `Feeding::wake_job`, the
shard's `Notify`), but named no job, so the host that runs the job, the node's own or a driving
shard's, stepped every native job at every turn its door could move it. The node's task now names
the job at those same moves, the other way to the job's own marks (`Native::mark`), by the rule
the S3 door's connections name theirs (research/99 [1], [2]):

| Native job | What moves it | How it is woken |
| --- | --- | --- |
| put taking its body | the node's task gives a piece or the end | `Feeding::send` names it (`Caller`) |
| put taking its body | the node's task lets go of the body, its exchange failed or read whole | `Feeding::let_go` names it |
| get, or last answers | the node's task takes an answer from its reply's channel, or writes one whole | `Replies::write` names it |
| any | the node's task lets go of its reply, its exchange ended or refused | `give_replies`, `Replies`' drop |

- **Each native request is named as the node's task takes it** (`Native::name`, a `u64` count of
  the node's host, unique for its life, kept too in what its door's halves share,
  `Handed::name`), carried with the job's half of its door to whichever host runs it. The node's
  task's halves of the door hold the caller of that host (`Caller`, one a host, shared by its
  doors): the node's own host's at first, a driving shard's once the job is handed there
  (`Lanes::caller`), the node's own again where the shard's lane refuses it (`Node::hand_off`). A
  driving shard's caller carries the shard's word its task waits on, which each door held before,
  so one pointer a door both names its job and wakes the task that steps it. A door kept for the
  next request lets its name and caller go (`Doors::keep`).
- **Named once until the host takes the name** (`Handed::called`, beside the job's own mark word
  `marked`): a bounded channel of twice the exchanges the host's jobs come on, the transport's for
  the node's host and the lane's room for a driving shard's (`Calls::new`), one name a job and one
  more where its door is kept and taken by a new request first; a name with no room sets `missed`,
  and the host wakes every native job, as a mark with no room visits every reply.
- **The host takes the names first in its step** (`Host::take_calls`), each one's job found in one
  probe of slots by name (`Host::called`, one a job), its word let go, then woken: a move before the
  word is let go is seen by the step, one after names the job again, so none is lost and at most
  one step repeats. A job handed to another host has its word let go as that host pushes it
  (`Host::take_handed`), its first step there seeing every move before.
- **A native job rests on its door** (`Job::rests`) only where its door holds a call
  (`Handed::calls`) and its reply is not let go: a put whose driver wants its body found its
  channel empty (`Pieces::dry`), nothing carried over; a get, or a job's last answers, whose reply
  has an answer waiting for room in its channel or a piece the node's task has yet to write whole
  (`Native::written`), as a get gives its next piece only then. A native job whose door holds no
  call is stepped at every turn as before.

Bound: the names a step takes are at most the calls' room; the host's slots by name, one a job,
are bounded by its job table, and a name whose job ended or went to another host finds none.

Tested (`node::host`, each count without calls measured by the same test run with none, the rule
before): 64 native gets of an empty object whose exchanges take none of their fields for 32 turns
are stepped as they ask their version, as it comes and as the node's task takes each head (64 each),
then not at all for 32 turns, then 32 and 32 as half the exchanges and then the rest take their
fields whole: 256 steps over 39 turns where the rule before took 2,432; each answered `Done` with
no bytes, its exchange ended. 64 native puts whose bodies come 32 turns late are stepped at their
first turn (64), not at all for 32 turns, then as the node's task gives each body's end (64) and as
each commit is answered (64): 192 steps over 35 turns, where the rule before took 2,240; each
answered `Done` with its version. The node's task names a job once until its host lets the name
go, again after, afresh on the host a job is handed to, and a name past the room is missed, every
native job woken, the word read once.

Open: commands, bucket and admin operations, single reads, batch deletes, completions and copies
are still stepped at every turn, and a job on either door still is while it reads its fields or
drains its body; and each turn still walks every job for whether any waits for room
(`offer_room`), its bound and vouch (`bound_jobs`), and its resting flag and keys.

*As built (2026-11-20): a turn reads the jobs due, waiting or woken, and no other*
(`crates/node/src/node/host.rs`). The last open part of the notes above. Each turn still read
every job three times for its cheap checks: whether any waited for room (`Host::step`), its bound
and vouch (`bound_jobs`), and its resting flag and keys (the step loop's pass-by, `rekey`), so a
turn cost by the jobs in flight however few moved, the naive poll research/99 [2] §1 names. Each
check now reads only what an event or a deadline puts before it, as epoll returns its ready list
alone (research/99 [1]):

| Check | Before | Now |
| --- | --- | --- |
| bound, vouch, moving | every job, every turn | the jobs due in the earliest-deadline heap (`beats`) |
| room freed | every job, at each freeing or beat | the waiters for room (`rooms`), first come first |
| resting flag | every job, every turn | the jobs awake (`ready`), first woken first |
| renewal due | every put's renewal, once one was due | the renewals due (`renewals`) |

- **Every word to an asker is a deadline in `beats`** (`Job::beat_ns`): a held job's bound, its
  beat while its chunk waits for room, and on its native door its next vouch, at once where it was
  never vouched for or its work moved since it last was, else half a beat after (`Job::due_ns`,
  `Job::vouch`); one at work holding nothing out, on its native door, half a beat after it was last
  told or admitted, where its bytes moved since (`Job::moving`); and a get past its bound whose
  chunk read waits for room, at once, its driver ended then. Before, the held job's vouch as its
  work moved and the moving job's word were read at every turn and in no deadline, and a held job's
  first vouch keyed half a beat late: each is now keyed at the first turn the walk would have said
  it, so none comes later. Each key is set as the job's state moves (its turn's end, an answer
  taken, a piece of its chunk moved, its bound pass) and is never later than one set at a later
  turn on the same state, so a resting job keeps its own (the oracle, `Host::audit_keys`).
- **A pass takes the due alone** (`Earliest::due`): the heap read from its root level by level,
  each entry due naming its children and none past the turn's time naming any, as no child's key
  is less than its parent's; at most twice and once more as many entries as are due, not every
  entry (Cormen et al., 3rd ed., §6.5). Nothing in the bound pass ends a job, so each slot it took
  names its job through the pass, and each is keyed again as it is bounded.
- **Room goes to its waiters** (`rooms`, `Host::offer_room`): each job whose chunk waits for room
  is kept keyed by when it began to wait, which it keeps while it waits, and room freed, or the
  beat, is offered to those alone, the first to wait first, as a first-come queue serves. A job
  answered is keyed before room is offered, so a chunk refused for room in the turn's answers is
  offered room that turn, as the walk did.
- **The jobs awake are a set, not a flag to scan for** (`ready`, `Host::wake`, `Host::rest`): a
  job is there exactly where it does not rest, keyed by its wake, counted (`woken`); waking it
  keeps the key it has. A turn steps the jobs awake as its steps begin, the first woken first, each
  once: a job that does not rest after its step is woken again after them, for the next turn, as
  every job that does not rest is stepped at every turn; one that ends leaves its slot to the job
  moved into it, whose entry names that slot (`Earliest::swap_remove`).

Bound: each set holds one entry a job, bounded by the job table; a pass's scratch vectors are kept
from turn to turn (`due`, `offered`), at most one entry a job.

Tested (`node::host`): 1,024 S3 puts each committed by one command answered 32 turns after it is
asked are read 1,024 times as their turn first comes, not at all in the 31 turns between, and
1,024 times as their answers come: 2,048 reads, where the walk read every put three times a turn,
3 × 1,024 × 33 = 101,376 (counted from the three walks, which read every job whatever its state);
each answered `Done` with its version. 64 native puts, half held on an ask of their own and half
at work holding nothing, their work moving every second to eighth turn, turned 64 times a quarter
of a vouch's interval apart beside a twin host running the walk: each job's last vouch is the
walk's at every turn, 1,110 vouches each, and the pass read the 1,110 jobs it told and no other,
where the walk read 4,096.

Open: commands, bucket and admin operations, single reads, batch deletes, completions and copies
still do not rest, nor a job on either door while it reads its fields or drains its body: each is
stepped at every turn, now kept awake rather than found by a walk. A driving shard's hand-off still
looks for jobs handed from the slot it reached (`Host::handing`), each job once a turn where the
node drives shards.

*As built (2026-11-20): every kind of job rests on its asks* (`crates/node/src/node/host.rs`). The
first open part of the note above. Commands, bucket and admin operations, single reads, batch
deletes, completions and copies were stepped at every turn while their asks were out, each step
finding its asks unanswered and asking nothing: a HeadBucket whose row came 32 turns after it was
asked was stepped 33 times. Each now rests after a step that found nothing to do (`Job::rests`), by
the edge-triggered rule (research/99 [1]), woken by the events that already wake a put or a get: an
answer to one of its asks (`answer_job`), a reply or `SlowDown` given it at its bound
(`reply_job`; its bound is keyed in `beats` while an ask of its own is out, `Job::beat_ns`), room
offered a copy's chunk (`offer_room`) and a copy's put's renewal (`renewals`). None of these steps
reads its door, so none rests on it: an exchange that ends ends its job by its own path
(`Node::job_lost`), and an S3 connection that lets go names its job (`door::Named`).

| Kind | Rests where, after its step |
| --- | --- |
| single read, command, bucket create or delete, operator's request | an ask of its own is out: its step asks nothing while one is |
| copy reading its destination's row, its upload's, its source's version or its mark | its read is out |
| batch delete | an ask of its own is out and none of its deletes was refused for the node's room at that step (`Deleting::held`): its deletes are out to its share of the node's commands, or all asked, and only an answer frees the next |
| completion | an ask of its own is out, no request of its driver waits for the node's room (`Completing::waiting`), and its driver has no outcome to answer |
| copy running its get and put | an ask of its own is out; no request of either driver waits for the node's room, nor a chunk for a volume's; its put has no outcome and its get none but a whole end its put was given; and its get holds no plaintext its put could take: none ready, or what it carries its put takes none of now |

A job whose ask the node had no room for, or whose step found work, does not rest and is stepped
at the next turn, as before; a job with no ask out never rests, so a resting job's bound is always
keyed.

Tested (`node::host`, 64 jobs of each kind on the S3 door, each ask answered 32 turns after it is
asked; each count of the rule before measured by the same test with these kinds kept awake):
HeadBucket, 128 steps, two a job, where the rule before took 2,112; DeleteObjects of three keys,
one delete out at a time, 320, five a job, where 8,256; CompleteMultipartUpload of one part whose
upload is gone, its Complete asked again as a retry, 256 where 6,208; CopyObject of an empty source,
its row, source and commit, 256 where 6,208, and of a source gone 192 over 65 turns, 64 × 65 =
4,160 before; DeleteBucket of a bucket gone, 128 where 2,112; DeleteObject, 192 where 4,160; and
ListPrincipals, 128, 64 × 33 = 2,112 before. Each answered its reply: `Done` with its three keys
deleted, `NoSuchUpload`, `Done` with the version committed, `NoSuchKey`, `NoSuchBucket`, `Done`.

Open: a native job reading its fields (`Work::Fields`) is stepped at every turn until they are
whole: the node's task marks its exchange's feeding and reply as the exchange's body is ready
(`Event::BodyReady`), not its job, which waking the job there would close. A job draining its
reply (`Work::Draining`) takes one step to its last answers, which rest as before.

*As built (2026-11-20): a turn's hand-off reads the handovers alone, and a command refused room is
asked again* (`crates/node/src/node/host.rs`).
- A native request whose fields are read for a driving shard (`Work::Handing`) is kept among the
  handovers (`Host::handovers`, keyed by when it was first keyed so, as each job is keyed,
  `Host::rekey`), and the node's task hands those alone, the first first (`Host::handing`,
  `Node::hand_off`), where a pass looked at every job at each turn the node drives shards. Bound:
  one entry a job. Tested: of 2,048 jobs, every other one handed, the turn reads the 1,024 it
  hands and no other, and a turn with none to hand reads none, where the pass looked at all 2,048
  at each turn.
- A command whose bucket's row came while the node held as many commands as it keeps was made,
  refused by `Node::command` (`NodeError::Asks`) once its value was given up, and never asked
  again: holding no ask, its bound was keyed in no deadline, and on the S3 door nothing ended it.
  It now asks for room first (`Serve::command_room`, the check `Node::command` refuses by) and
  keeps the row until there is some (`Commanding::row`), stepped at each turn meanwhile as every
  job refused room is. Tested: a DeleteObject whose row comes without room asks its delete at the
  first turn with room and is answered `Done`; by the rule before it had no reply in 1,000 turns.

*As built (2026-11-20): a native job reading its fields rests until more of them arrives*
(`crates/node/src/node/host.rs`, `crates/node/src/node.rs`). The open part of the note on every
kind of job resting. A native job's first steps read its request's fields from its exchange
(`Node::read_fields`), on the node's own host, before its work is made or handed to a driving
shard; `Job::rests` said no for it, so a request whose fields came in more than one arrival was
stepped at every turn until the last came, each step reading nothing.
- **The transport says each arrival after a read that took what had come**, the edge-triggered
  contract of epoll(7) (research/99 [1]). hyper-transport's `Core::read_body` clears the exchange's
  `ready_sent` at every read of a body arriving, and each STREAM frame QUIC takes for the stream
  says `Readable` (hyper-quic's `StreamsState::received`), on which `Core::read_step` says
  `BodyReady` where `ready_sent` is clear, then sets it. A request's fields are its body's first
  bytes (`object::Head::decode` holds the fields, the put's length and the trailer to the body's),
  so its exchange is reading its body throughout. Held by a test of its own
  (`tests/exchange.rs`, `an_owner_reading_only_as_it_is_told_reads_the_whole_body`, a test-only
  row in `vendor/UPSTREAM.md`): an owner that reads a 4 MiB body only at its request and at each
  `BodyReady`, each time until a read takes none, reads it whole and verified, where with the
  clear of `ready_sent` taken out (run once, not kept) it read 13,110 of its 4,194,304 bytes and
  was told no more; before it, the suites' servers read every exchange at every turn, and no test
  held the contract.
- **The job's step reads until a read takes none** (`Node::read_fields`, unchanged): it ends with
  its fields whole, its asker gone, or a read that took nothing, the transport's `EAGAIN`. A job
  still reading its fields after its step rests where its door holds a call, as every native job
  resting on its door does (`Job::rests`).
- **The node's task wakes it as more arrives** (`Node::on_event`, `Host::arrived`): the
  exchange's `BodyReady`, taken before the host's step, wakes its job where it reads its fields,
  found by its exchange in one probe (`Host::job_of`), as it marks the exchange's feeding and
  reply. The step that follows reads what arrived; an arrival after that read is said again. An
  exchange that ends ends its job (`Node::job_lost`), resting or not.

Tested (`node::host`, the rule before measured by the same test run with no calls, on which no
native job rests on its door): 64 native GETs whose fields arrive in two halves 32 turns apart
are stepped at their first turn, reading the first half (64), not at all for 32 turns, then as
the rest arrives and each is woken (64), and as each get asks its version, gives its head and
gives its end (64 each): 320 steps over 38 turns, five a job, where the rule before took 2,368,
37 a job, 33 of them reading its fields; each answered `Done` with no bytes, its exchange ended.

Open: a copy whose get has given its last bytes, held because its put takes none of them now, is
stepped at every turn until its put's next answer (`Job::rests`' `got` waits for the copy's end
given, `Copying::ended`).

*As built (2026-11-18), the stall fixes in step 6's shape* (upload-stall merged, `crates/node/src/node/host.rs`).
The node's task reads every native body, so it reads them as upload-stall's job did, never into a
piece of the endpoint's budget: a job's fields straight into their buffer, made at their length as
it was admitted, and nothing past them, so a put's body comes whole through its feeding and its
work carries none (`work` takes no carry); each of its pieces read into the one piece the node's
task reads with (`Node::piece`, made once, at most a piece) and copied once into the feeding's
storage, the piece the put's admission charged it for, as 6c copied it out of the budget's piece;
and what is left of a body after its reply drained into that piece where its job was admitted, a
body already whole reading nothing, and through the endpoint's one piece for its owner's reads
where it was answered at once, charged nothing (`Node::drop_body`, `Opening::admitted`). The node's
task writes every native reply (6c), so a reply the budget has no room for waits there
(`Replies::wait_for_room`), the job having ended as it gave a reply with no bytes past its fields:
its head, or the piece a body answered at once drains through, refused `Budget`, it is held,
offered again at each turn and its exchange vouched for, the node's turn due at each beat
(`Replies::room_due_ns`, `host_due`) within its client's patience as its job's bound stood when it
gave the reply (`Opening::bound_ns`), and past it its exchange is refused `Budget`; a get's head
waits the same way, its job taking no piece meanwhile, as none is written; a reply no message
holds refuses its exchange `FrameBound` (`Told::Failed`, which carries its refusal). A put whose
chunk waits for room is offered it again at each beat on either door by the host's own step
(`Host::step`, `Host::room_offered_ns`), so a driving shard's host offers its own. Tested
(`node::host`): a reply whose head the budget refuses waits, vouched for at each turn and due at
its beat, then goes whole, or past its patience is refused `Budget`, a get's head the same, and a
reply no message holds is refused `FrameBound`; an S3 put waiting for room is due at its beat;
tests/put_body.rs, red then green on upload-stall, passes over the merged code.

*As built (2026-11-18), step 6's fifth part, its first piece: a host's own wait, and the node's
piece in its floor* (6e, `crates/node/src/node/host.rs`, `crates/node/src/derive.rs`). The host's
wait is its own jobs' alone (`Host::due`): at once where whoever serves it holds the answer of
one of its jobs' asks, untaken (`Serve::answered`, the node's `has_outcome`), as an answer that
came after the host's step in a turn waits on no event; else its puts' renewals, and the beat or
bound of each job whose work is held. `Node::host_due` keeps beside it what the node's task waits
on itself: an ended job's answered ask, its outcome to drop (`abandoned`), a lookup answered, a
body fed at the last turn, a reply whose rounds ran out, and a reply waiting for room in the
endpoint's budget (`Replies::room_due_ns`). So a driving shard's task can wait on its host and
nothing of the node's. The piece the node's task reads every native body into (`Node::piece`,
`node::PIECE`, one a node, made once, at most a piece, outside the transport's budget) is counted
in the floor's transport class (`derive::Class::Transport`, §1.4). Tested (`node::host`): a host
running no job waits on nothing; a job whose ask is out is due at its client's patience; and once
the ask's answer is held, at once. The host, node and derive unit tests and tests/put_body.rs
pass.

What 6e needed still, as worked out: the lanes, in `node::driving` beside `node::shard`, and the
body's channel kept with its door. Built since (below): a native door's exchange optional, a job's
end carrying its door's half, the body's channel kept with its door, and the lanes.

- *The answer slots, pooled.* `hyper_rt::sync::oneshot` boxes the block its ends share and makes
  its ring at each one made (`ring::Shared::new`, `Ring::new`), so a slot made as it is asked
  allocates per ask. A lane of answers from the node's task to the driving
  shard, made once and holding `command_asks + read_asks + chunk_asks` values, is a pool of those
  slots: the shard's host holds each kind to the node's bound for it, each ask holding its place
  from its send until its answer is taken (an abandoned ask's until its answer comes and is
  dropped), so the node's task never finds the lane full and an answer cannot be refused, and the
  steady state allocates nothing past what the ask's own value holds.
- *An ask the node has no room for is held by the node's task, not answered `Asks`.* Answered
  `Asks` in its slot, the shard would ask it again with nothing to wait on but the next answer, a
  loop between the two tasks for as long as the room stays taken; and the job cannot take it back,
  its driver's request having become the ask. The node's task holds it, in order, at most the
  asks the shard's host holds (its count), and asks it at each turn while it has room
  (`Node::command_room`, the read and chunk bounds checked as `Node::read` and
  `Node::put_chunk` check them, before the ask's value is given up). An ask `Node::ask` refuses
  otherwise is answered its kind's refusal (`CommandOutcome::Refused`, `ReadOutcome::Refused`,
  `ChunkOutcome::Lost`), which the job ends `Failed` on as it does now.
- *The node's side of the lanes is generic over `Serve`*: each ask taken from the lane served by
  `Serve::ask`, each held ask's outcome sent once `Serve::outcome` gives it, and each job's end and
  put sample told through `Serve::job_ended`, `committed` and `filled`; so a unit test serves a
  two-shard runtime's driving task from the test's thread with the stub (`Stub`), as no unit test
  builds a `Node`, and the node serves it in 6f by lending its host as it does now.
- *A job's end carries its door's half back.* A driving shard's host keeps no doors: its job's
  half (`Native`) would wait in its `Doors` for a `Replies` half the node's task keeps, and that
  wait has no bound there. So `Serve::job_ended` takes the job's half and gives back what the host
  keeps: the node's own, the half, kept in its doors as now; the lanes', none, the half riding the
  job's end to the node's task, which gives it to its doors.
- *A request handed carries its operation, not its work* (its door's job half, its principal, its
  `Op`, its Open's block, its deadline, its admission and its shard), and the shard makes the work
  (`work`), as a job's drivers need not cross (step 6's check). Its door's exchange is the node's
  task's alone: `ExchangeId` is made only inside hyper-transport, so a test cannot make a request
  that names one, and the shard needs none (`Door::Native`'s exchange becomes optional).
- *The rest of `Serve` over the lanes*: the volume table the node last read handed in, a settled
  copy (its scheme chosen on the node's task), the latest superseding one the lane had no room for;
  the ranges' groups, their count, the ask bounds and the node's wall clock (`Wall`, `Copy`) handed
  at the task's start; `read_fields` refused, typed; `took` nothing; `freed` a flag the node's
  task tells a shard a chunk of whose jobs it answered for want of room since it last told it,
  held while the lane is full; a chunk's move (`Host::chunk_moved`) told by the number
  the shard gave its ask, once a serve of the lanes however many pieces moved and none for a chunk
  answered at that serve, dropped where the lane is full, its next piece telling it again.
- *Production starts none, and compiles every piece*: with no `#[allow(dead_code)]`, the task's
  start is reached through the node's turn and a list of driving lanes production leaves empty
  until 6f fills it.
- *The put's body channel kept with its door.* The receiver stays in the job's half (`Native`)
  and the sender in the node's task's (`Replies`), the body's feeding (`Feeding`) holding no
  sender of its own: what the ends' drops said becomes two words in `Handed`, the job let go of
  its body (the feeding reads and drops the rest, where its send found the channel closed) and the
  node's task let go of it before its end (the job's body is `IncompleteBody`, where its take found
  the channel closed, read before the channel so a piece given before it is taken first); and
  `Doors::keep` empties the body's channel as it empties the reply's. Shown by the door's
  allocation count over 64 puts, as the reply's is (0 allocations).

*As built (2026-11-18), step 6's fifth part, its second piece: a handed request needs no exchange*
(6e, `crates/node/src/node/host.rs`). `Door::Native` holds an `Option<ExchangeId>`: the node's task
names the exchange of each request it reads the fields of, and a request handed with its fields
read names none, as only hyper-transport makes an `ExchangeId` and the job needs none, its body and
reply going through its door. The node's lookups by exchange (`hosts`, `job_lost`, the reply's job
in `write_replies`) find no handed job, and a handed job reads no fields, as a local one reads none.
Tested (`node::host`): a native put and get handed with no exchange, driven only through `Serve`
by the stub, take the put's body's end from its door and give each reply to the node's task's half
of the door, which writes it whole (`Done`, the version written; `Done`, no bytes); each job's half
comes back as it ends, and each door is kept once its reply's half is given back.

*As built (2026-11-18), its third piece: a job's end carries its door's half* (6e,
`crates/node/src/node/host.rs`). `Serve::job_ended` takes the ended job's native half (`Native`),
told first that its job let go (`Handed::ended`), so the node's task writes nothing past what it
gave wherever the half is kept, and gives back what the host keeps: the node gives the half back,
kept in its host's doors as before; a driving shard's lanes will take it to the node's task, which
holds the door's other half, so a driving shard's host keeps no doors and no half waits there for
one it never sees. Tested (`node::host`): given back, the host keeps the half and the door once its
reply's half is back; taken, as the lanes will take it, the host keeps nothing, and the node's
task's doors keep the door whole once both halves are back.

*As built (2026-11-18), its fourth piece: a put's body channel kept with its door* (6e,
`crates/node/src/node/host.rs`). The body's channel is made with the door (`native_door`) and kept
with it: its receiver in the job's half (`Native::body`), its sender in the node's task's
(`Replies::body`), lent to the put's feeding while the node's task reads the body (`read_fields`,
`feed`) and given back as the feeding is let go (`Feeding::let_go`): at its end, its exchange's
failure or its job's drop (`feed_bodies`), its exchange's end (`job_lost`), or its reply's end
(`write_replies`), always before its door's half goes back, so no feeding outlives the door it
reads for. What the channel's ends' drops said is two words in `Handed`: the job let go of its
body (`dropped`, set by `drain_job`, or of its door, `ended`), so the feeding reads and drops the
rest, and does so at its next turn rather than waiting for a piece the job never takes to free its
storage; and the node's task let go of it (`cut`, set as the feeding is let go), which the job
reads before the channel (`take_body`), so a piece given before it is taken first and the body is
then `IncompleteBody`; a job that took the body's end reads neither. `Doors::keep` empties the
body's channel as it empties the reply's, clears both words, and keeps no door whose body sender is
not back. A put whose door has lent its sender already, which none has, is answered `SlowDown`, as
one whose channel could not be made was. Bound: one piece in the channel (`door::Depth::FIRST`), as
before, and a door's two channels at most one a door kept (`Doors`). Copies: none changed.
Allocations: a door made asks the allocator for its body's channel beside its reply's, once; past
it a put asks nothing for its channels. Tested (`node::host`): 64 puts past the first take the one
door kept and ask the allocator for nothing (0 allocations, 0 reallocations); with a body of four
pieces each, only the feeding's storage, made and shared once a put as before (`BytesMut::split`
promotes the storage to one its views share: 2 allocations a put, 0 reallocations); a job that let
go has the rest dropped; a feeding let go tells its job its body did not come whole once it took
the piece given before; a kept door's words are cleared. The host and node unit tests and
tests/put_body.rs pass.

*As built (2026-11-19), its fifth piece: a driving shard's task and its lanes* (6e,
`crates/node/src/node/driving.rs`, `host.rs`, `node.rs`). A driving shard's task holds a `Host`
that keeps no doors (`Host::new(0, ..)`), started through that shard's control queue by a launcher
that makes the task there, as its jobs' drivers need not cross, and spawns it Normal, as the S3
connections beside it run (`driving::Lanes::start`). Its lanes, each a hyper-rt channel made once:

| Lane | From, to | Bound | At the bound |
|---|---|---|---|
| Requests, tables, room freed, chunks moved (`In`) | the node's task, to the shard | the exchanges the transport admits at once (`parked_most`) | a request given back to run on the first shard (`Lanes::hand`); the volume table held, the latest superseding it; room freed, told only to a shard a chunk of whose jobs was answered for want of room since it was last told, held as a flag; a chunk's move told once a serve, none for a chunk answered then, dropped where full, its next piece telling it again |
| Asks, job ends, put samples (`Out`) | the shard, to the node's task | `parked_most`, the most the node's task takes in a turn | the shard waits for room, as a range's shard does |
| Answers | the node's task, to the shard | `command_asks + read_asks + chunk_asks` | never full: the shard's host holds each kind to its bound (`Asks` past it, asked again at the job's next turn), each ask holding its place from its ask until its answer is taken, an abandoned one's until its answer comes and is dropped |

The answers lane is the pool of answer slots: an ask is numbered by the shard (`Out::Ask`) and its
answer comes back under its number, so an ask allocates nothing past its own value, where a
`oneshot` an ask would make its block and ring each time. The node's side is generic over `Serve`
(`Lanes::serve`): each ask asked where the node has room (`Serve::room`, checked before the ask's
value is given up, as `Node::command`, `read` and `put_chunk` check theirs), held in order where it
has none rather than answered `Asks`, which the shard would ask again with nothing to wait on, and
answered its kind's refusal where the node refuses it otherwise; each answer the node holds sent;
each job's end told (`Serve::job_ended`, no ask let go, the node holding the shard's asks until
their answers come), its door's half given to the node's host's doors; and the put window's
samples told. The shard's `Serve` (`driving::Served`): asks numbered and held to the node's bounds,
outcomes taken from the answers come, the latest volume table handed in (`Table::settled`, a copy
with the scheme chosen now and no choice running), the ranges' groups, their count, the ask bounds
and the wall clock handed at its start (`driving::Start`), `read_fields` refused (a request is handed
with its fields read), `took` nothing, `moved` none of its own, and `job_ended` the job's half
riding its end. The task waits for the first of a request, an answer and its host's own due
(`Host::due`); the node's task wakes on what a driving shard gave or its end (`Lanes::arrived`), and
one whose task ended stops the node (`NodeError::ShardEnded`).

A handed request carries its operation, not its work (`host::Handover`: its door's job half, its
principal, its `Op`, its Open's block, its deadline, its admission's time, charge and window, and
its shard), and the shard makes its work (`Host::take_handed`). The node hands one where a lane is
held for the shard its request was placed on: its fields read, its operation is held as handing
(`Work::Handing`), its put's feeding set up from the operation's length and trailer (`body_of`),
and after the host's step it is handed (`Node::hand_off`), its exchange's feeding and reply kept by
the node's task; one its lane refuses is made into work and runs on the first shard. Production
starts none and compiles every piece: `Node::spread` starts driving tasks from a list it leaves
empty until 6f (`Node::start_driving`), and with no lane held the node's turn skips the hand-off and
the lanes' turn. Tested: a host on the second shard of a two-shard runtime, driven over its lanes by
the stub node on the test's thread, takes a put and a get handed with their operations to their
ends (the put's bucket read, its commit asked and answered `Done` with the version written; the get
answered `Done`, no bytes), each job's end told with its charge and its shard and its half riding
it back, the test's doors keeping both doors; a request past the lane's room is given back
(`node::host`); each lane's bound holds as stated, an abandoned ask's place held until its answer
comes, an ask the node has no room for held and asked once it has, the table held and superseded,
room freed held, a chunk's move dropped, a shard whose task ended refused `ShardEnded`; and an ask
and its answer cross the lanes asking the allocator for nothing, on either side (64 asks: 0
allocations, 0 reallocations) (`node::driving`).

What 6f needs first, beside placing requests by the rule: a driving shard's host is woken by its
doors. Its task waits on its lanes and its host's due alone, so a put's body piece the node's task
gives, or a get's piece it writes whole, wakes nothing there: a put awaiting its body or a get
awaiting its last piece's write has no due and would wait until something else woke the shard (the
test's put took its body's end at its first step, and its get had no bytes). A waker its door's
halves share (`Handed`), woken by the node's task as it gives a piece or writes one whole, closes
it. And the host's count of bytes moved (`Host::moved`) reaching the node's sum (`Node::moved`);
and a refused request's place in the rule's count moved to the first shard.

*As built (2026-11-19), step 6's sixth part, its first piece: a driving shard woken by its doors*
(6f, `crates/node/src/node/driving.rs`, `host.rs`). A handed job's body and reply go through its
door's channels, which neither task waits on, so each wakes the other as it moves the door. The
node's task tells the shard's word (`driving::Lanes::doors`, hyper-rt's `sync::Notify`, made once
with the lanes), which the shard's task waits on beside its lanes and its host's due, as it gives a
piece, the end or a refusal of a put's body (`Feeding::send`), lets go of the body
(`Feeding::let_go`), takes an answer from the job's reply channel or writes one whole
(`Replies::write`), or lets go of the reply (`Doors::give_replies`, `Replies`' drop); the door's
half the node's task keeps (`Replies::driving`, lent to the feeding) carries the word while its job
runs on that shard, set as its fields are read for the hand-off (`Node::read_fields`) and taken off
where the lane refuses it. The job wakes the node's task's waker, which its own half carries while
it runs there (`Native::node`), as it gives its door an answer (`Native::flush`), takes a piece of
its body, the next then readable (`feed_put`), says its work is held where the node's task took its
last word (`Native::held`: it vouches once a word, and a held job says so at each of its shard's
steps), or lets go of its body (`Native::drop_body`), as an S3 connection on another shard wakes it
after each piece it sends (`door::Submitter::wake`). Why a `Notify` and not a lane message: hyper-rt's `Notify` is an atomic
waker with a permit (one waiter registered in its cell, woken by `notify_one`, a notification with
no waiter kept for the next wait), so no wake is lost and none takes a place in a lane; a message
the lane had no room for would be dropped, and a full lane's older messages order nothing after
what the door moved. One word a shard, not one a door, as one task steps every job of its shard at
each of its turns; the job's half wakes the node's waker itself, a word its clone copies (hyper-rt's
wakers count nothing), where the node's task's turn is its own. Neither allocates per wake: the
word is made once a shard and each door's half holds a count of it (`Arc`), set and cleared with
the door. Bound: a notification is one bit, kept until the next wait. Tested (`node::host`): a put
of a body of 2.5 blocks, given a piece at a time, and a get of its bytes, each handed to the second
shard of a two-shard runtime and served over its lanes by a cell in memory on the test's thread
(Block and File ranges over models, volumes checking each chunk's CRC-32C, the Name range keeping
the version the put commits), reach `Done`, the put's size and the get's bytes exact, given in
three pieces after its fields; before the wake they hung (no job ended in 20 s).

*As built (2026-11-19), its second piece: a driving shard's bytes in the node's count* (6f,
`crates/node/src/node/driving.rs`, `Node::moved`). After each step in which its jobs moved bytes
(their bodies' pieces taken, their replies' given: `Host::moved`), a driving shard's task says the
total since it started on its out lane (`Out::Moved`), as it says the put window's samples, one
message a step at most; the node's side keeps the latest (`Lanes::moved`) and `Node::moved` sums
each shard's with its own task's and its host's. Tested: the lane's count reaches the node's side,
the latest superseding (`node::driving`), and the put and get handed above, once ended, are
counted there whole, the put's body taken and the get's bytes given, twice the object (`node::host`).

*As built (2026-11-19), its third piece: a refused request counted where it runs* (6f,
`Drivers::moved`, `Node::hand_off`). A request whose lane gives it back (its lane full, or its
shard's task gone) runs on the first shard, so its place in the rule's count moves there and its
job's shard says the first (`Handover::driven`), its end giving that place back; the shard it was
placed on counts one fewer, so the rule places the next there. A move past the drivers' shards, or
a count past a `usize`'s, moves nothing, and the place stays where it was. Tested
(`node::drivers`): a request moved from the second shard to the first is counted on the first, the
next placed on the second, the end given back to the first, and a move past the shards refused.

*As built (2026-11-19), its fourth piece: native requests placed by the rule* (6f,
`Node::spread_native`, `Drivers::place`, `driving::Lanes::takes`, `process::NativeShards`,
`derive::shard_shape`, `node::driving_lanes_bytes`). Where its operator states `native_shards =
spread`, `process::serve` starts a driving shard's task on each runtime shard past the first that
takes tasks (`others`, numbered as `Node::spread` numbers the ranges' shards), before the node's
first turn, and the rule's count is made over the first and those shards; each native request the
host admits is placed on the one running the fewest of those whose shard the watchdog does not see
held past its limit (`Lane::takes`' test, `hyper_rt::watch::stalls`), the first of those tied, and
on the first where none takes one; a refused hand-off runs on the first, counted there (above). The
node's task reads the volume table again at each turn it serves the driving shards, where its root
range applied past it (`Node::table`), and tells them the newer, as its own host may place none of
the puts that would have read it. Why a setting of its own, `native_shards = first | spread`
(`first` where unstated, said as a basis by `mantle serve`, taken by both `mantle serve`'s file and
`mantle node`'s), and not `range_shards = spread`: ladder r40 measured spreading the ranges slower
than keeping them on the first shard (above), so tied to it native placement could be measured only
with the ranges spread, its effect not told from theirs; apart, a ladder measures each on and off,
`mantle bench doors --native-shards first|spread` stating it in each founder's file beside
`--range-shards` (`transfer::Cell::make`), its `doors run` line saying both. Sizing:
`derive::shard_shape` counts on each shard past the first, where native requests are spread, the
driving task and the launcher that spawns it, holding its own slot while it spawns, and the task's
timer, beside the ranges' where they are spread too; the floor counts each driving shard's lanes and
the vectors beside them, derived from their types' sizes (`node::driving_lanes_bytes`: two lanes of
the exchanges the transport admits at once, the answers' lane and both sides' asks at the node's
asks, and the shard's word), for each shard past the first where the node hosts objects. Tested:
the rule passes over a shard that takes none now and fills it first once it takes again, and with
none taking places on the first (`node::drivers`); a driving shard takes requests while its shard
runs and none once it is gone (`node::driving`); the key reads `first` and `spread` apart from
`range_shards` and refuses another word (`process::tests`), and reads back from the bases
(`derive::tests`); the shape holds a driving shard's task, launcher and timer beside the ranges'
(`derive::tests`); the floor holds the runtime at every shape with native requests spread or not
(`tests/shard_memory.rs`); and `mantle bench doors --native-shards` parses apart from
`--range-shards` (`mantle`'s CLI tests). Open: each driving shard holds a settled copy of the volume
table, which grows with the cell's volumes and is not yet in the floor; and the native suite under
`native_shards = spread` is the end-to-end test, and the ladder over `native_shards` the measure.

*As merged with perf-10k (2026-11-19): the learned windows, the room classes and the caches' loans in
step 6's shape* (`host::Serve`, `driving::Served`, `host::Replies`). The node keeps the windows it
learned of its puts and its gets (`crate::window::Windows`, §2.8) as it kept the put window: the
host tells it each sample through `Serve` (`committed`, `filled`, and `read`, a get's block from its
row asked to its plaintext opened), and a native get's client pace is timed by the node's task, which
writes the get's pieces (`Replies`' `Drain`, from when its client's room for a piece was full until
the piece is written whole), as the S3 door's connection times its socket. A get's window is widened
and a copy's windows fitted once their sizes are read, through `Serve`: `windows`, `holds` (whether
the operations' bytes hold the bytes past the job's charge beside the others') and `recharge` (the
bytes past its charge taken, the caches giving back first what of them they hold, `host::lend`, and
those its span does not need given back). Admission is the node's task's alone (§2.5), which a
driving shard cannot ask within its step, so a job there takes no bytes past what it was admitted at
(`Served::holds`): its get holds the floor's window, and its copy the windows its admitted charge
holds, fitted to the windows the shard learned of its own jobs; what its span does not need, and
its samples, go to the node over the shard's lane (`Out::Recharged`, `Out::Read`). Each chunk
outcome is classed as perf-10k classes it (`host::taken`): a put's or a get's chunk refused for room
is asked again of the same volume, offered room at each beat by whichever host runs it, a driving
shard's told as room frees (`In::Freed`), and its refusal moves none of its job's work. Room freed
is read once a turn (`Node::step_host`: a volume answered a put or a get, or a chunk was answered
otherwise than for room), offered to the node's host's jobs at its step and told to a driving shard
only where the node answered a chunk of its jobs for want of room since it last told it
(`Lanes::tell_freed`), the one thing room freed moves there: told to every driving shard at each
turn that freed room, as first built, each chunk the node answered, the S3 door's among them, woke
every driving shard to step its host for nothing and park again. Measured on a three-shard runtime,
two driving shards served by the test's thread, room freed at each of 64 turns and through 16 gets
handed to the first (`host::tests::a_driving_shard_wakes_for_its_own_work_alone`, debug build on
an Apple M5 Max): the shard handed nothing was polled 55 to 59 times, woken 53 to 57 times from the
node's thread and parked 42 to 49 times, three runs, where now it is polled twice (its launcher and
its task's first poll), woken never and parked twice; the shard handed the gets was polled 39 to 59
times and now 40 to 46, about 2.7 wakes a get.

A native request's crossings, counted the same way
(`a_native_put_and_get_cross_the_lanes_as_counted`, rounds of a 1-byte put then a get of it): a put
gives 7 asks (each answered once), its end and a sample; a put and a get together 12 asks, and the
shard was polled about 12 times and parked 6 to 10 times a round. Two crossings a piece were not needed
and are cut. The bytes a shard's jobs moved were a message after each step that moved some
(`Out::Moved`), which at a get's piece rode beside its door's own wake of the node's task: they now
ride what else the step gave, each job's end among it, so a put and a get of 16.7 MB
(`a_driving_shards_jobs_are_woken_by_their_doors`) give 8 such messages where they gave 258. And a
chunk's move (`In::ChunkMoved`), told the shard at each piece of a chunk going to or coming from a
peer, is told once a serve of its lanes, and not at all where the chunk's answer goes at that serve
(`a_chunks_pieces_moved_are_told_once_a_serve`: eight pieces, one word). What remains is the asks:
each crosses to the node's task and its answer back, until a driving shard asks a range's shard
itself (step 6g). A reply's
head never waits for room (§3.3): what waits is the piece a refusal's body drains through
(`Replies::wait_for_room`). Open: a get or a copy on a driving shard widens no window past its
admission until the shard can take bytes of the node's admission, which `native_shards = spread`'s
ladder measures against.

## 3. Transport

Nodes, and clients through mantle's client library, talk over two layers, as the hecate
specification lays them out (07 §4.7): QUIC for everything stateful, and a separate plane of
sealed UDP datagrams for consensus control and membership.

*As built (2026-10-20).* Both layers come from hyper-raft's shared crates (vendor/UPSTREAM.md):
hyper-transport, the application protocol below, over hyper-quic (quinn-proto 0.11.18) and
hyper-tls (rustls 0.23.45 on AWS-LC), driven on a hyper-rt shard by hyper-rt-transport's driver;
and hyper-datagram, the plane of §3.4, on a socket of its own (hyper-rt-transport's
`PlaneSocket`). A node's owner (`crates/node`) runs a shard's ranges and both layers in one task:
a range's Raft messages travel on one lane to each peer, a snapshot on an exchange of the bulk
class, and control that carries no entries and fits a datagram on the plane. Of each pair of
nodes the lower id dials, so a pair keeps one connection and each end knows its side of the
plane's key schedule. Both ends key the plane from the connection's TLS exporter and number its
epoch from a second export under a label of mantle's own, `EXPERIMENTAL mantle plane epoch`
(RFC 5705 §4 keeps labels beginning "EXPERIMENTAL" for private use), so each new connection
replaces the last one's keys with nothing sent between the nodes. The transport's bounds are
derived from what the node states and from QUIC's initial estimates (RFC 9002 §6.2,
RFC 9000 §10.1; `crates/node/src/bounds.rs`) until its budgets are measured (§2.6). A client
(`crates/node/src/client.rs`) asks a range's leader its commands and its reads on the request
class, the request's head naming the range's group: a command's body is its entry, answered once
applied; a read's is its query, answered by ReadIndex (§5.1). A node that does not lead replies
with the leader it knows and, when it knows none, how long until it acts on the leader's silence
(its patience: the stream's freshness, the range's expected election and one round), by which the
range has elected. Retries and redials back off as below: from the measured probe timeout,
doubled, at most the idle timeout, with equal jitter (`bounds::backed_off`, `bounds::pause`), the
client's waits no longer than the patience its nodes stated. A node redials only on the
transport's word that a connection or a dial ended (`Closed`, `Unreachable`), so every dial must
end in an event its owner is told: hyper-transport before hyper-raft `26b5af8` told nothing of a
dial that failed as the last one had, and a peer cut off for two failed redials (18 s on the
Windows runners) was never dialed again (`crates/node/tests/cluster.rs` holds a cut that long).
A node states how long a link to a peer it dials may go without moving should that peer fall
silent, which it cannot tell from slow (`Status::reconnect`): its connection's idle timeout and
the peer's backoff. It states as well how long a command or a read it asks of a range itself may
wait paused (`Status::asking`, its backoff), and a pause it took because no node named a leader ends
as soon as its own member of the range names one. A node with no leader named and no peer has no
node to ask (the one it would ask is node 0), and pauses so rather than open an exchange to no one:
a lone node's enlistment, asked as its root range elects, waits for that range's leader and no
longer. A peer learned at a new address has its old connection closed at once, since
the transport dials no peer it holds a connection to. A node that answers a request without its
body (with the leader it knows, or a status) first reads the body to its end, keeping none of it:
ending an exchange whose request half is unread resets that half, and the asker's transport
refuses the whole exchange on the reset, the answer it carried with it. RFC 9114 §4.1 lets a
server answer early only because its client must keep a complete response, which
hyper-transport's asker does not yet do. On the Windows runners, where a request's head often
arrives in a datagram of its own, that turned 1,330 and 1,634 redirects into lost exchanges in
three rounds of two process tests (one run per Windows target), and 4 and 7 once the body was
read first. A request or reply the transport tells after it ended the exchange, its connection
lost or its stream stopped before the owner took the event, holds no head: an exchange's events
hold its slot whether or not the exchange is still there, and the refusal queued behind the event
ends it. A node and its client take such an exchange as that refusal ends it, a command asked
again, never its missing head as an empty one, which answers the command `Malformed`. Read so, a
node that asked its statement of its history (§2.4 item 7) of a leader whose stop then closed its
connections, the reply and the close reaching it in one turn of its driver, refused its own
statement and ended (`crates/node/tests/transport_ended.rs`;
measurements/2026-10-25-a-reply-past-its-connection.md). A client cuts an exchange that has not
been answered within its
period, and each cut doubles the next period with that node until that node replies (RFC 6298
§5.5, collapsed only by a new measurement of the same path, §5). A registration, which would
register again if asked again (the Raft dissertation §6.3), is cut by no period. On the Windows
runners, registrations cut and asked again filled a range's 64 sessions (`SessionsFull`).
`crates/node/tests/sim.rs` holds both laws. The node's client (`mantle_node::client`) measures
each kind of work apart, its slowest reply and each node's cuts kept by kind: a command or a read,
the range leader's quorum round; an object's request, the node's job; a chunk's put or get, its
volume's durable write or verified read. A node holding a request whole is given a period to
answer it (hyper-transport's progress), so the period must outlast that work, and one kind's reply
measures nothing of another's: measured as one, the requests' replies alone set the measure, so a
client asking only objects or chunks never had one and gave an exchange asked with no deadline all
of it (the repair suite's puts and chunk reads, asked with none), and one that had asked a request
gave a chunk put a few round trips, cut while its node said each beat that it worked on it. A
transfer's size is no part of it: a period that moved a datagram is not judged, however long the
transfer runs. An object's or a chunk's exchange is given at least the node's serve period, the
least any asker of a node gives an exchange, within which the node's notes of its jobs and held
chunks come three times (`Endpoint::working`, research/77 D1), and at most the stock S3 clients'
patience, the most a node waits on such work asked with no deadline (§5.4, research/77 D2), so
one asked with no deadline of a node that says nothing is given up before any reply is measured
(`crates/node/tests/client_patience.rs`).
Still to build: the TCP fallback below, an ALPN of mantle's own in place of the transport's, chunk
transfers (§5.2) and SWIM (§3.5).

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

*As built (2026-10-21).* The transport's bytes are one budget that QUIC's buffering and the
messages share: a connection's receive window and the receive chunks datagrams are cut from are
both charged to it, as are every message's head, every lane frame, and the piece a peer's body
is read into (an object operation's body is read into the piece its admission charged it for,
§2.8). Reading what QUIC buffered is what frees it, so the buffering is sized within the budget
less what the messages hold at once at most (`bounds::limits`): every exchange's head both ways,
one body piece, and for a node that keeps appends out a lane's window of frames out to each node
its ranges replicate to and one frame in from each peer; a peer's two connections' windows (each with the class reserve its first window carries)
and its full window of unread data packed into whole chunks share the rest, and a budget that
leaves no window is refused at start. Before, the windows and the chunks were each sized from the
whole budget: on macos-aarch64 a gateway's three connections grew their windows to the budget's
whole and the piece a 1 MiB reply is read into was refused (CI run 39694528095).

The driver's receive ring is held apart in the same way (`bounds::ring_growth`, 2026-11-18). On
Linux a receive call takes up to a batch of messages (`recvmmsg(2)`), each a datagram train the
kernel coalesced into one buffer of up to 64 KiB (`UDP_GRO`, udp(7)), and the ring grows to a
buffer for each message its drains took, up to the batch: one train from each peer and client the
node serves, within `UIO_MAXIOV` (`bounds::io`); elsewhere a call takes one datagram and the ring
is one buffer that never grows. The driver charges what the ring grows by to the budget
(hyper-rt-transport's `Driver::size_ring`), and the budget's sizing did not count it, so on Linux
a grown ring took room the heads and the owner's piece were sized for: the likely reason uploads
given up `Lost(Stalled)`, a finished job's reply head refused, were Linux's alone (CI's
ubuntu-24.04 runners, 2026-11-18). The budget's sizing now
holds the ring's most, `(batch − 1) × 64 KiB` on Linux and none elsewhere, beside the messages,
so the floor counts it.

So are the connections' state (`bounds::connections_state`, 2026-11-18). The endpoint charges each
connection's state to the budget as the connection is made, a dial or an accepted handshake:
hyper-transport's `state_charge`, the connection's own structures and its exchange tables at the
streams a connection takes, on macOS arm64 9,632 bytes and 24 a stream (9,896 at eleven streams),
all it charges before its owner observes what connections hold, which no node or client does. The budget's sizing did not
count it, so the connections took room the heads and frames were sized for: about 1.3 MB at 64
clients' 132 connections. The sizing now holds it at the most connections the endpoint holds at
once, those its admission holds and the handshakes it lets wait beside them, three for each peer and
client (1.96 MB at 64 clients and two peers), so the floor counts it. What a connection's QUIC and
TLS state allocates is counted beside the budget (`bounds::beside`, measured whole), so the floor
counts the structures the state charge stands for twice, about 10 KB a connection, rather than
leave the budget's classes short of them.

A reply is never refused for room (2026-11-18). The endpoint took a reply's prefix and head from
the budget as the reply was given (hyper-transport's `Core::message`), and a finished job whose
head it refused `Budget` had ended there, its exchange left open with nothing more to come, so its
client gave it up a period later (`Lost(Stalled)`, `tests/put_body.rs`); so did the node's other
replies, which dropped such a refusal (`node::exchange_ended`). A peer's exchange now takes the
room its reply's prefix and head take at their longest, `frame_grant(max_head)`, with its request's
head, both or neither, a request the budget cannot fund refused `Budget` as it arrives, which its
asker hears at once; the reply fills that room, and the exchange's end gives it back however it
ends (hyper-transport's `Core::admit_prefix`, `Core::reply`, vendor/UPSTREAM.md). The messages'
bytes already count every exchange's head both ways (`bounds::messages`), so the room needs no
memory past what the sizing states. What still waits for room is the piece a refusal's body
drains through before its reply, the endpoint's one piece for its owner's reads: the reply is held
by the node's task, which writes every native reply (§2.9 step 6c, `host::Replies`), as a job
whose ask is out is held: vouched for and asking again at each turn, the node's turn due at each
beat, within its client's patience as its job's bound stood when it gave the reply, and past that
patience its exchange is refused `Budget`, which needs no room and its asker hears at once
(`Replies::wait_for_room`, research/77 D1, D2); a reply no message holds, a field past its bound,
refuses its exchange `FrameBound`. A job never ends with its exchange open.

An open the exchange table refuses gives its message's room back (2026-11-19). hyper-transport's
`Core::open` reserved the request's prefix and head and put them in the exchange it offered the
table; refused `Exchanges` at the table's bound, the exchange went and its reservation with it,
never given back, as a dropped `Reservation` gives nothing back. A node whose reads waited on their
leader asked again at a full table and filled its budget so: on the node simulation's new voter,
whose reads the founder held for a quorum its held device could not give, 9,025 refused opens kept
324,900 bytes, after which every open, every reply's head and every lane frame was refused
`Budget`, and the node answered none of its own reads again while its member trusted the leader
(CI gates run 44510117734's "2 nodes" SlowDown, the same reads ended past the budget where the
detectors had not judged). The room is now held apart until the table takes the exchange, and
released when it refuses it (vendor/UPSTREAM.md).

Past its floor the transport is given its want where memory allows (2026-11-19). At the floor
each share is QUIC's initial window, 12,000 bytes (RFC 9002 §7.2), below a receive chunk's 65,527,
so the endpoint's pool of chunks is empty and every datagram is copied into a buffer of its own
(hyper-transport's `receive`), and one stream moves at most a window a round trip. The want is
each share at a chunk (`bounds::chunked_transport`): a peer's two connections' window ceilings
and its pool's chunk. A window does not take its ceiling at once: it starts at the initial
window and doubles whenever its reader consumed a whole window within two round trips, each
growth reserved from the budget first, so it is `min(BDP, ceiling)` (hyper-transport's `credit`,
after Chromium QUIC's `QuicFlowController::MaybeIncreaseMaxWindowSize`; RFC 9000 §4.1–4.2). It is
a performance want in rule D's order (§1.4): after the capacity wants and the operations the
node serves, whole for the connections those operations open while what remains holds it; then
each operation more costs its client's windows at a chunk, so every operation's windows hold a
chunk and its datagrams land without a copy, at fewer operations for it (10,414 where 13,764 at
the initial window on a 16 GiB host at 36ad012b, macOS; §1.4 gives today's and Linux's).
Windows past a chunk at a path's bandwidth-delay product wait on the transport: its ceiling is
fixed when the endpoint is made (`Limits::window_ceiling`), before any round trip is measured, so
a ceiling past a chunk would be a guess at the path, and a share past the one the budget funds
needs hyper-transport to fund a window's growth from the budget's unreserved room and set it back
when another's first window or the messages need the room (its `Window::set_back` does so for
idle connections already).

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
chunk requests; a client asks for objects alone, never a range's command or read, which write and
read metadata rows directly and are a gateway's to send once it has judged the client's operation. A Raft message's sender must be the node its certificate names, as focal's
`step_authenticated` binds `from` to the certificate (07 §2.2), and must be a member of the
range's configuration or a learner it is adding. Node identity authenticates the peer; the
range, its descriptor generation and the operation's identity in each frame decide whether a
message may act (audit §11.8), and a range refuses a frame from a term or generation it has
left (architecture §5).

*Pinned nodes (2026-11-18).* A node pinned by certificate, as `mantle init` pins each founder in
the others' configurations, is judged by its member's row once the root range holds rows, as a
node known by its name under the authority is (membership.md §2 rule 1): a row `Gone`, or
`Removed` with the root range keeping it as a learner no more, or none with its grant spent,
refuses it at its handshakes from the entry that says so, and the node closes its open connections
`Identity` as it applies that entry (`crates/node/src/directory.rs` `Peers::set_members`,
`node/members.rs`). A pin admits only a node the rows have not written off.

**Decided (2026-10-26): a client is known by its name under the authority, pinned by no node**
(research/73, closing §11's open item for clients). A client's certificate is the authority's
leaf for a connection's client end alone (`extKeyUsage` clientAuth): RFC 5280 §4.2.1.12 lets a
certificate listing purposes serve those alone, and webpki refuses at a server's verification one
whose list omits serverAuth, so it never serves as a node's. Its subject names it: its common name
`client-ID`, the transport's identity, and its organization the account it acts for, as Kubernetes
names a user and its groups. A node's directory pins nodes and gateways by certificate as before,
and takes a certificate it does not pin, verified for clientAuth under the authority at the
handshake, as a client's when its extensions are exactly the client profile's and its names read
(`mantle_cert::client_names`), unless its id is a pinned peer's or one held under another account.
A client's object operations act for that account, the principal the cell's access keys act for on
the S3 listener, so a bucket either door makes is the account's (S3 owns buckets by account); the
directory keeps each admitted client's account while the transport holds a connection of it,
pruned whenever the node's connections move, within the identities and handshakes the transport
admits. A node that hosts objects admits `gateway.clients` clients at once (at the floor, as many
as its operations, §1.4), each an identity of its transport beside its pinned peers, so clients
cannot take a node's identity from its cell; `Directory::identify` can refuse only as a certificate
refused, which a client cannot tell from one another authority issued, so the node closes a client
past its bound once its connection is up, refused `Identities`, which a client asks again after
(`crates/node/src/directory.rs`, `node.rs`). Tested by a cell of three (`crates/mantle/tests/
native.rs`): a client certificate another authority issued is refused at its handshake, and so is
the cell's authority's certificate for a node presented as a client's.

**Decided (2026-11-12): certificate compression's decoder is brotli-decompressor 6.1.1, RFC 7932
only, bounded to the certificate and behind an unwind boundary** (research/88; hyper-raft branch
doors-step8-2, hyper-tls `VENDORED.md` §11). A node's server reads a client's certificate before
the client has authenticated, and its CertificateRequest offers Brotli and zlib (RFC 8879), so the
decoder takes a stranger's bytes. On CVE-2026-111497, an out-of-bounds write in Google's C decoder
fixed on 2026-10-21, the owner kept compression and moved the decoder ("Upgrade the decoder") from
the Rust port's 5.0.3 to 6.1.1, released that day; the encoder stays at brotli 8.0.4, since no
brotli release builds on 6.1.1. The port is safe Rust, so a bug of the C kind there is a panic:
every decoder runs inside an unwind boundary, and a panic is the handshake's refusal (alert
bad_certificate), the node serving on. The decoder takes upstream rustls 0.23.46's fix for
RUSTSEC-2026-0348 (no large window, so five bytes no longer allocate 1 GiB; the certificate's
buffer filled exactly), and refuses any allocation past what a stream decoding to the
certificate's length needs: a four-byte header declaring a 16 MiB metablock took 16,780,366 bytes
before the bound and 2,584 after, a whole 1 KiB certificate 38,835. Every stream a stock encoder
makes still decodes (window 2^22 and the certificate's own, 1 to 65,535 bytes). A decoder
written apart from the encoder decodes what a node's handshakes send, at the certificate's window:
Google's C decoder 1.2.1 through `brotlic`, a dev-dependency the owner allowed on 2026-11-18,
takes every node certificate `mantle init` issues, from the shortest to the longest, and the
gateway's, as a real handshake compresses them (mantle-node `tests/certificate_compression.rs`,
research/88 [9]).

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
   caller's budget, or while its server says its work for it moves or is held (hyper-transport's
   notes, `Endpoint::working`, at most one a beat, a third of the serve period; research/77 D1,
   D2). Each request carries how long its client will still wait: the native request head's time
   left, and for one that says nothing, every S3 request among them, the stock S3 clients' 60 s
   without its work moving (`bounds::STOCK_PATIENCE`, botocore's and the AWS CLI's default read
   timeouts). The native client holds itself to the same: an exchange it asks with no deadline has
   that patience as its progress period at the most. A caller that supplies a deadline also
   bounds each source and sink wait and checks expiry before transport events; no working note
   extends that deadline on the caller's own clock. So a node that says nothing of it, one just
   started again among them, is given up at the end of the first period that moved nothing, never
   sooner than the node gives up the work, and not waited on without end. A job's work moves at each of its own asks answered, each piece of a chunk it reads
   from or sends a peer, and each piece of its own body read or reply written. A job whose own work
   is held, an ask of its own out (a peer's transfer its transport keeps, or a write at its own
   device) or a chunk of it waiting for room, is ended `503 SlowDown` past its client's patience and
   lets go of what it asked, and within it the node says each beat that its work is held; a get
   whose head has gone out, its reply begun, is told nothing more past that patience and is ended
   unfinished, as a get is, once its transport gives up a chunk read its peer says nothing of, and a
   job waiting on its own client, for its body or for room to write its reply, is the transport's to
   judge. The node says so of a peer's chunk each
   beat while a volume holds it, put and not yet durable, within the time the asking job carried
   in the chunk's head and never past the stock patience, the most a job carries; a write that fails
   is answered at once, its volume fenced, and is held no more. A device that stalls is waited on as
   long as the client would wait, and one that never answers is given up then (the owner's choice of
   2026-11-01, research/77 D2, as Quiver reviewed it). A chunk
   refused for want of room, a volume's `Busy` or a bound of the transport's reached for now (as
   its error.rs defines them: `Budget`, `Exchanges`, `LaneFull`, `Lanes`, `Events`,
   `ReceiveBuffer`, the admission's `Pending`, `Identities`, `Connections` and `Busy`, and
   `Unsent`), or `Stalled`, whose chunk the volume answers as done when the same key and bytes are
   put again (§5.2), is asked again of the same volume once a chunk is answered or a volume answers
   a put or a get, not sent on and the block left `Unplaced` (`host::taken` classes each outcome; a
   refusal for room is no movement of the job's work, so its patience still ends it); a chunk read
   refused so is asked of the same copy again, a read changing nothing, never taken for a copy
   lost, and a get whose reply began and whose read still waits at its client's patience ends
   unfinished there, as no transport ends that wait (`Get::out_of_patience`); and at each
   beat it waits, at most once a beat (`host::Host::step`), on either door:
   a peer's volume whose queue other writes or its cleaner hold, as a filling volume's is, tells
   the asker nothing when it frees room, and with no other chunk of the node out nothing asked it
   again before the client's patience. The S3 door's put, due only at that patience, then sat
   quiet with nothing to turn the node (the serve suite's fill, CI run 44454089741). So a get paused between blocks while a peer reads its chunk is not cut, nor a run
   whose copies take their peers longer to receive than its client's period (CI's busy runner,
   2026-10-31: sending a run's 8.3 MB copy took up to 7.5 s, and vouched for as its bytes went the
   cut runs went from 100 to 1 in 24 rounds, measurements/2026-10-29-a-node-cut-while-it-works.md),
   nor a put whose receivers' shared device stalls for seconds (the box, measurements/2026-10-28).
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
  HTTP/2 and HTTP/3 are not offered. The listener is mantle's own HTTP/1.1 codec (`crates/http`)
  on hyper-rt's TCP, with TLS through hyper-tls: the owner's decision, no hyper and no tokio
  (research/83). On a laptop it may serve plain HTTP on the loopback address; SigV4 still
  authenticates every request there, and a request carrying SSE-C headers is refused over plain
  HTTP, as S3 "rejects any requests made over HTTP when using SSE-C" (20 §3.3). Elsewhere it serves
  TLS with the endpoint's ECDSA P-256 chain (§1.5), and TLS 1.2 beside 1.3, with the ECDHE-ECDSA
  AES-GCM suites: Apple's Secure Transport, under the AWS CLI's transfers on macOS, cannot use
  TLS 1.3, nor can Windows 10's Schannel (research/84).

The codec does no I/O: the listener reads a connection's octets into buffers of its own and hands
them over, and the codec parses a request's head where it lies, decodes its body as it streams,
giving its octets back as spans of that buffer, and writes a response's head and chunked framing
into the listener's buffer. Its rules are research/83's:

- A request's head, its request line and field lines through the empty line, is held to S3's
  8 KB (05 §10.2): past it `400 RequestHeaderSectionTooLarge`, or `414` for a request line not
  ended within it (RFC 9112 §3). It is the one buffer a connection holds for a head, and the
  codec allocates nothing for a request: a head keeps where its field section lies and how many
  lines it holds, each judged when the head was parsed, and its fields are read from there as
  they are asked for (research/71, step 6).
- The grammar is strict where leniency smuggles: one SP between the request line's parts, no
  whitespace before a field's colon, no obs-fold, no control octet but HTAB in a value, each
  refused `400`; a line ends at CRLF or a lone LF; an HTTP/1.1 request has exactly one valid Host.
- Framing that cannot be trusted is refused and the connection closed: Transfer-Encoding with
  Content-Length, a coding list not ending in chunked or with chunked twice, Transfer-Encoding on
  HTTP/1.0, a Content-Length that is not digits, overflows or lists differing values (RFC 9112
  §6.1, §6.3). A coding other than chunked before it is `501`; neither field is no body.
- A chunked body is decoded as it streams: each size checked for overflow, extensions passed
  unread and the trailer section kept apart, each held to the head's bound per request (RFC 9112
  §7.1).
- `Expect: 100-continue` is said by the head before any body octet is read, so the listener sends
  `100 Continue` or its final status first (RFC 9110 §10.1.1; §4.2 step 6); another expectation is
  `417`.
- A connection persists by RFC 9112 §9.3's rules; requests on it are taken one at a time, each
  where the last one's body ended, so responses go in order, and any refusal closes it.
- No header timer: a slow client holds only its connection's reservation; when the node needs the
  room, it closes the connection that has made the least progress for longest.

*As built (2026-10-25).* The codec, `crates/http`: each rule above by a test of its own
(`tests/rules.rs`), generated requests read back whole and in any pieces, mutated and arbitrary
octets read or refused with a typed error and never a panic (`tests/properties.rs`), and its
allocations counted exactly (`tests/allocations.rs`: every head shape of the rule tests, cut at
every length and mutated, parsed with none, since 2026-11-06). `mantle bench http` measures it on one core
(measurements/2026-10-25-http-codec.md).

*As built (2026-10-25), the listener* (`crates/node/src/listener`). `mantle serve` binds it where
its S3 keys say (§1.5) and accepts on the node's first shard beside the node's own task, each
connection a task of its own, so the runtime wakes the one whose socket or channel moved; the
runtime holds a task for each connection the budget admits (§4.5).

*As built (2026-11-18), connections placed across the shards* (`crates/node/src/listener/shards.rs`;
audit 2026-11-12 §6). The node runs a shard for each CPU the process runs at once: its cores,
capped by the whole CPUs its CPU budget buys, affinity and cgroup quota included (hyper-rt
`Facts::cpus_at_once`), and one where the budget buys less than a whole CPU. Its ranges, its log,
its transport and its QUIC door stay on the first shard, which keeps one durable owner, one log
writer and one UDP endpoint; the S3 connections go to the others. **The placement rule:** the
listener accepts on the first shard and hands each connection it admits, served or answered
refused, to the other shard with the fewest of its connections open, the first of those tied,
passing over a shard hyper-rt's watchdog sees held in one poll past its limit (`watch::stalled`,
where a connection would wait for the poll to end before its first byte is read) and one gone;
with every other shard passed over, the first serves it, as it serves one a channel refuses. It
goes through a channel of that shard's own bounded at the connections the listener holds at once
(`listener::tasks`), so a channel never refuses one for want of room. A task on that shard takes
each, adopts its socket and starts its connection there, so the connection's future is made on
the shard that runs it, as hyper-rt's `tcp::serve` hands a connection off. A shard's count is a
budget of its own, a slot of which each handed connection's stream carries and gives back as it
drops, wherever the connection ends. A channel that refuses a connection anyway, its taker gone
with its shard, hands it back whole, and the first shard serves it: a spawn through a shard's
control queue would drop it with the request, a queue whose depth can be one. A node of one shard
serves every connection on it. The TLS sessions every connection's handshake keeps and resumes
are timed on the host's clock (`sessions::now`), which every shard reads alike, where a shard's
own clock counts from that shard's start. hyper-rt sizes every shard alike, so each holds the
first's tasks, timers and sockets (`process::runtime`), and the floor counts each shard so and
each lane at the connections it may hold (§1.4, `derive::runtime_bytes`). On a loopback address it serves
plain HTTP; elsewhere TLS 1.3 and 1.2 with the endpoint's P-256 certificate, hyper-tls's default
suites, of which a P-256 key takes the ECDHE-ECDSA AES-GCM ones for TLS 1.2 (research/84). A
hyper-tls connection holds no configuration (vendor/hyper-tls/VENDORED.md §2), so each connection
builds its own around the one certified key the listener loads at its start: it answers HTTP/1.1
when a client offers ALPN, resumes sessions from the listener's one store (below), and compresses
no certificate (RFC 8879), the chain being one leaf. A connection's
requests are taken one at a time, each at the octet its last one's body ended. Tested in
`crates/node/tests/listener.rs`: a listener on a real socket reached by hyper-tls's client over
TLS 1.3 and over TLS 1.2 alone, its certificate verified under the endpoint authority for the name
dialed, HTTP/1.1 chosen over an offered HTTP/2, an ECDHE-ECDSA AES-GCM suite on TLS 1.2, and a
request answered over each.

*As built (2026-11-17), a connection's turn by time* (`crates/node/src/listener/io.rs`; hyper-raft
docs/runtime.md §3.14). Each try of a connection's socket charges its turn on its shard: a plain
read goes through hyper-rt's `TcpStream::read` and a plain write through `write_all` or
`write_vectored`, and under TLS each `read_tls` and `write_tls` is charged first
(`hyper_rt::coop::consume_budget`). Once the connection has held its shard past the shard's own
quantum, the one its watchdog and `wanted` use, with anything else wanting the shard, the charge
answers no and the connection yields at that await, after Tokio's cooperative budget and Seastar's
task quota (as Kiln's runtime copy built it): a client whose octets are always there no longer holds
its shard for the length of its body. A wait for readiness is charged only once it fires, and each
poll's first charge goes on, so a client that waits pays nothing for its waits and a connection is
never refused at every poll. Tested in hyper-rt's `tests/coop_budget.rs` and `tests/coop_waits.rs`.

*As built (2026-11-17), resumption without early data* (`crates/node/src/listener/sessions.rs`;
research/94). A client resumes its TLS session with the endpoint, which skips the certificate and
its signature (RFC 8446 §2.2), and sends no early data. The sessions stay on the node, in one store
the listener reserves whole when it binds: `listener::SESSION_BYTES` for each connection it serves,
two sessions' slots and their share of the store's index, counted into what an operation costs the
node. A TLS 1.3 handshake, full or resumed, gives its client two tickets (RFC 8446 Appendix C.4),
each the 32-octet key of a session kept there and used once (§8.1); a TLS 1.2 handshake gives a
session ID, resumed as often as the store keeps it (RFC 5246 §7.4.1.3). No ticket carries a session
sealed under a key of the node's, so there is no ticket key to rotate or lose, and no TLS 1.2
client is offered an RFC 5077 ticket. A session is resumed for 24 hours from the full handshake
that authenticated the endpoint (RFC 5246 Appendix F.1.4, and the lifetime hyper-tls states on the
ticket), a resumed handshake's tickets keeping the time of the session it resumed (RFC 8446
§4.6.1). The store holds the last sessions made, a newer one taking the slot of the one made
longest ago; a session taken, found past its lifetime or displaced is zeroed. The configuration
allows no early data, so no ticket offers it, and early data a client sends anyway is skipped
unread (RFC 8446 §4.2.10). Tested in `sessions::tests`: a stock client's TLS (hyper-tls's)
reconnecting to the listener's own configuration, its second and third connections resumed over
TLS 1.3 by tickets and over TLS 1.2 by its session ID, each side saying so, the client asking for
early data and having none to send and the server reading none, and the store keeping two tickets a
TLS 1.3 handshake less the one a resumption spent; the lifetime, single use, eviction and refusals;
and the store's bytes counted by the allocator, at most 1,142 a connection against the 1,143
stated, nothing allocated as it churns.

*As built (2026-10-26), the native protocol's messages* (`crates/protocol`, `mantle-protocol`): the
kinds of message and their classes, the transport's bounds and an object's request and reply, which
the node and its clients share, so a client depends on none of the engine, the ranges or the chunk
store. An object request's head carries, beside its kind, fields' length, identity and a put's
length, the algorithm of the checksum a put's trailer brings after its bytes (0 for none), so the
body's exact length is known before any field is read and a client that streams a source it reads
once states the checksum at its end, as aws-chunked's trailer does; the node feeds its driver the
object's bytes alone and checks the trailer's value before anything commits (`BadDigest`
otherwise). The listener's aws-chunked trailer travels on the same field.

*As built (2026-10-26), native uploads* (gateway.md §2.1). Four kinds carry an upload whose runs the
node cuts: Open (the object's length where known, the client's buffer where its source is read
once, the full-object checksum's algorithm, a CRC), answered with the upload's ID and its progress;
Append, a run, its bytes and its CRC trailer after the fields as a put's, at the offset it begins,
answered with its progress frame, or `Offset`, the progress naming the upload's offset, where it
begins anywhere else or follows the last run; Offset, the upload's progress, which a resuming client
appends from; and Finish, the object's length and full-object checksum, answered as a completion,
or `BadDigest` with nothing committed. A reply's progress is its durable offset, its runs, the
length of every run but the last, whether the last committed, and a run's frame's CRC. The node
reads the upload's row before a run's body and refuses a run at another offset, of another length
than the plan's or past the last, so a stale run never streams; the Name range checks the same at
the commit, where two runs of one offset race.

### 4.2 A request's path

1. **Admit the connection** against the connection budget.
2. **Route** with `mantle_s3::route`: operation, bucket and key, by virtual-hosted or path
   style. A header or query parameter that appears twice is resolved once, the same way for
   routing, signing, policy and the body (audit §9).
3. **Authenticate** with `mantle_s3::sigv4`: the `Authorization` header, a presigned URL, or
   anonymous, against the cell's access keys as `mantle serve` read them (§1.5). Owner identity,
   SSE-C authority and bypass privileges come from what authentication established, never from
   fields a caller supplies (audit §9).
4. **Authorize**: the bucket's row from the Bucket range, through a cache whose staleness is
   bounded (metadata.md §1, §6), and its policy judged by `mantle_s3::policy`.
5. **Admit the request**: the tenant's share (§4.5) and the request's working set from memory,
   the devices and the peers it will use. A PUT reserves its first block's window and, when its
   scheme is coded, a coder's work space; a GET its first block.
6. **Answer or continue.** Refusals so far need no body. The codec says a request expects
   `100 Continue` before any of its body is read (§4.1), and the listener sends it only once the
   steps above have passed; RFC 9110 lets a server answer "with a final status code" in its place
   (25 §2), so a request refused here is refused before a client using `Expect: 100-continue`
   sends its body, which is what S3 tells PUT clients to rely on: "If the message is rejected
   based on the headers, the body of the message is not sent" (25 §3).
7. **Run the drivers**: a `Put`, `Get` or `Completion` from `mantle-gateway`, and the S3 layer's
   own documents for everything else. Each driver's requests are served as §5 describes.
8. **Respond** with `mantle_s3::response`'s documents, streaming a GET's bytes as the `Get`
   driver gives them out.

*As built (2026-10-25), the native door.* Steps 1 and 3 are the transport's: a connection
admitted by the node's admission (hyper-transport) and authenticated by the cell's certificates
(§1.5). Step 2 is the request's kind (`crate::object`). Of step 4, only a bucket's owner is
judged: a bucket's head, delete and versioning change are refused to any other principal, S3's
answer where no policy grants them, and a principal's objects are not yet judged beyond the
cell's certificates; the bucket's policy comes with the listener. Step 5 is the host's bound on
the operations it runs at once (`gateway.objects`): one past it is answered `SlowDown`, its body
read and dropped so the exchange ends whole. Steps 7 and 8 are §5.1's: the drivers, the ranges'
commands and reads, and a typed reply the native protocol encodes.

*As built (2026-10-25), the S3 listener* (`crates/node/src/listener`, `crates/node/src/door.rs`).
1. A connection past the budget, ten (research/72), is answered `503 SlowDown` with `Connection:
close` from a reserve of ten more, and one past both closed as it comes (§4.5, 2026-11-02). Every
`503 SlowDown` the S3 door answers keeps S3's `<Code>` and says its path in its `<Message>`, S3's
words and then its cause (`door::Slowed`): past the connections served at once, the door full or
the node gone, the key lookups full, a stopping node, the access key's root-range read or an
operation's range read or command without a leader that serves it within its budget, a block's
chunks unreadable or unplaced, or work held past a client's patience. The native door's `SlowDown`
carries its cause beside the status in the reply's head (`mantle_protocol::object::Slowed`, one
byte, zero for none, beside `SlowDown` alone), the same causes and three of the native door's own:
a request the host had no room for while as many waited as it parks, one parked past its client's
deadline or patience, and one whose operation, learned from its fields, holds more bytes than the
host has room for; the CLI prints its words after `SlowDown: `. The node counts each cause it
answers on either door, the listener as it answers and the host as a native reply goes, and says
each count as it changes, `slowed CAUSE COUNT`, at most once a cause each half serve period, as
`moved` is said, so a refusal storm says at most twelve lines a half period. 2. `mantle_s3::route` names the bucket by the path at the endpoint's stated names, its
addresses and `localhost`, and by the host under one of its names; any other host is
`InvalidURI`. 3. `mantle_s3::sigv4` checks the signature, its header's or a presigned URL's,
against the cell's access keys; who asks is the account the key acts for, and a mutation's identity
the SHA-256 of that account and the request's `amz-sdk-invocation-id`, which every attempt of an
SDK's operation carries, so its retries are one operation (gateway.md §2.1) and no account names
another's; a request without one is an operation of its own, its identity drawn. 4. A bucket's
owner is judged by the host as for the native door; bucket policies come with PutBucketPolicy,
which this path does not serve yet. 5. The operation goes through the door (`crate::door`): a
bounded channel the node takes into the same jobs the native door's requests become, with no
loopback QUIC hop. The door holds a request for each connection the listener admits, so it refuses
none for room, and the node takes them in the order they came while the host has room for another
operation (`gateway.objects`), the rest waiting there with their bodies unread; a stopping node
answers `503 SlowDown` before any of a body is read. 6. A put's body is read only once the host says its driver wants it, after every check that
could refuse it, and `100 Continue` goes then; a refusal goes in its place (§4.5). 7. The host runs
the drivers as for the native door. 8. `respond.rs` renders the host's typed reply as S3's response:
§4.4's codes, the listings' and multipart's documents from `mantle_s3::response`, and a get's bytes
written as the host gives them, its response left unfinished if the host fails past its head. A
job's answers go into the door's channel as it has room: a get whose end finds the channel full,
the listener behind on its connection, holds the end and goes on answering until it went, and a job
a refusal moves to draining replies at once (`moved_on`, 2026-10-27; before, a step that found its
job no longer its own kind ended it, and the listener closed the connection under a response whose
every byte it had sent, in 4 of 12 runs of `tests/s3.rs` with each process on one CPU).
Served: CreateBucket, HeadBucket, GetBucketLocation, DeleteBucket, ListBuckets, Put and Get
BucketVersioning, PutObject, GetObject whole and by range, HeadObject, DeleteObject, ListObjectsV2,
CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts and
ListMultipartUploads; since 2026-11-14 an object's content headers and `x-amz-meta-*` metadata,
kept and returned, GetObject, HeadObject and DeleteObject of a version by its ID,
ListObjectVersions, CopyObject, UploadPartCopy, DeleteObjects, and GetObjectTagging, a version's
empty tag set read as a head reads the version (s3-protocol.md §14, research/89); every other
operation `mantle_s3::route` names, and a write naming tags, is `501 NotImplemented` on this
path. DeleteObjects' document is read whole, as CompleteMultipartUpload's is, before its
integrity header is checked. Tested in
`crates/mantle/tests/s3.rs` on a cell of three made by `mantle init`, every request signed with the
operator's key through each node's listener: a bucket made, listed, headed and refused again;
objects put through each listener read back whole, by range and by head through the others; every
key listed through every listener; a key made only where none is (`If-None-Match: *`) and replaced
only at its ETag (`If-Match`); a delete through one seen by all; a multipart upload whose parts go
through two listeners, listed, completed through a third with S3's ETag, and read back whole and
by a suffix range through each; objects put through one listener alone held by the other two nodes
exactly as they received them over QUIC's transfer class, neither putting any itself; and on every
node, the bytes its volumes hold the bytes its peers put on it over QUIC and those it put itself,
read by a `chunks` line on standard input, answered `chunks RECEIVED WRITTEN HELD UNKEPT FRAGMENTS
BUDGET FREE`, `UNKEPT` the bytes of the first two no volume kept: a volume's refusal, or a body that
ended before it was whole; so a node's received and written bytes are its held and unkept ones
exactly. The last three are the room its volumes have, the two things that refuse a put `Full`
(chunk-store.md §5, §8): the fragments their indexes hold, the most they may, and the bytes of their
free segments. Stock clients
pass against a cell of three in `crates/mantle/tests/stock_clients.rs`, run on every CI target: the
runner image's AWS CLI v2 (`aws s3 mb`, `cp`, `ls` and `rm`; `aws s3api`'s object, listing and
multipart commands; `aws s3 cp` past its 8 MiB multipart threshold, from standard input and to
standard output, its multipart upload committed with S3's ETag form) and boto3 (its object calls,
a listing and its managed transfers, with the SDK's default checksums), every object read back
byte-identical; CI installs boto3 pinned with every hash, and the CLI from AWS's installer where an
image lacks it, test tools the owner approved (`scripts/stock-clients.sh`).

*As built (2026-11-14), principals* ([principals.md](principals.md)). Steps 3 and 4 judge a key the
configuration does not hold as a principal's: the claimed key's row is read from the root range by
ReadIndex as the request arrives (`door::KeyAsk`, `Query::Signer`), its secret unwrapped under the
root key, the signature checked against it, and the request judged by the principal's identity
policy, `AccessDenied` past it; so a key revoked by a committed command is refused at every node
from then on. The operator makes, lists, rotates and revokes principals and keys with `mantle
principal` and `mantle key` over the native protocol, kinds 25 to 31, as the account's root.

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

*As built (2026-10-25), the S3 listener's bodies.* The listener reads a put's body in the native
door's piece, the transport's receive chunk of 65,527 octets, so a driver takes the same pieces by
either door, and holds two in flight to the host, one taken while the next is read: a body comes no
faster than the driver takes it, which TCP's window carries to the client. A body framed
`aws-chunked` is decoded as it streams by `mantle_s3::chunked`, each chunk's signature checked as it
comes; a body whose SHA-256 the signature covers is hashed as it streams and checked at its end; and
the body's end goes to the host only once those held, with the checksum its trailer carried, which
the put's driver checks the object against before anything commits. The trailer's algorithm, which
`x-amz-trailer` names, goes to the host with the operation, so the driver hashes the body by it
from its first byte; a body that states a checksum in a header and in a trailer is refused. A body that ends short of its
length is `IncompleteBody`. A small document's body (CompleteMultipartUpload, PutBucketVersioning,
CreateBucket's configuration) is read whole, within its `mantle_s3::body` bound, before its
operation is made, into one buffer at the length its request stated within that bound. The slow
body's reclaim and the socket options above are open.

*Since 2026-11-06 (research/71, step 6).* aws-chunked is decoded in place: `Decoder::step` gives
each run of payload as a span of the connection's buffer, its framing line held in an array of its
own, and each run goes to the host as a piece of exactly its bytes, one copy, as a plain body's
do. The listener still copies each body octet once into its piece: its buffer is read into again
as soon as a piece leaves, and a piece handed on as a slice of it would need a buffer made, and
zeroed for the read, for every piece instead. SigV4's canonical request is hashed as it is formed,
and its string to sign, and a chunked body's for every chunk, is one buffer at its length. A
response's field lines are written once, checked as written, into a buffer the connection keeps
across its requests, and its head and its document, or a GET's head and its first bytes, go in one
vectored write.

*Since 2026-11-18 (the Lean doors plan's step 6 bound).* A request holds at most
`listener::REQUEST_BYTES` above its connection at rest, whose read buffer, head buffer and
response head buffers it keeps across its requests: one head (`mantle_http::MAX_HEAD`, 8 KB) for
what the door makes of a head and answers it with, one body segment (`PIECE`) and one TLS record
at its largest (`TLS_RECORD`, 2^14 octets and TLS 1.2's 2,048 of expansion, RFC 8446 §5.1–5.2
and RFC 5246 §6.2.3), 92,156 octets. A TLS connection encrypts a response a record at a time,
each sent before the next is made, where hyper-tls's default would encrypt 64 KiB ahead of the
socket. A put whose body comes in more than one piece, a read or a TLS record each, holds
`listener::streamed_bytes`: one head and one TLS record, and its body's pieces in the host's
queue, at most the queue's depth of them. The connection waits for the queue's room before it
copies a piece (hyper-rt's `Sender::reserve`), so it holds none the queue has no room for; at a
depth of one a put holds `REQUEST_BYTES`. `tests/listener_memory.rs` counts each request from its
connection at rest to its answer, at the first depth and at the most pieces a body's queue holds:
there a TLS put of one segment, four records, holds 96,348 octets, past `REQUEST_BYTES` and within
`streamed_bytes`.

*Since 2026-11-18 (the owner's rule: no fixed depth, learned from what the node measures).* The
queue's depth is learned from the pieces themselves (`door::Depth`), where it was 2 by fiat. By
Little's law (Little 1961) the pieces in the queue at once for a body to come at its client's pace
are the rate it comes at times the time each waits there: the host's time to take a piece, from
its send to the host's turn that takes it, over the connection's time to deliver one, from its
last piece's send to the next one's bytes ready, none of it waiting for the queue's room; rounded
up, and at least one. Each piece carries both times' ends (`door::Paced`), and the host sums them
over a phase of as many pieces as admission's phase measures (`admit::PHASE`, 267, §2.8): the
ratio of two such sums tells a quarter's change from chance as admission's ratio of two phases'
powers does. A phase's depth replaces the last's, so the sample is at most a phase's pieces and
what a phase measured is forgotten the phase after; a body's first piece, whose time is its
client's answer to `100 Continue` as much as its pace, is no sample. Until a phase is measured the
depth is one, the least that moves a body: the host takes a piece while the connection reads the
next into its own buffer, as the learned window starts at one block (§1.4). A connection reads
the depth as it makes a body's channel, whose capacity hyper-rt fixes then
(`hyper_rt::sync::channel`), and caps it at what the node's memory division gives a connection
(`listener::most_pieces`): the bytes `derive` charges each connection (`CONNECTION_BYTES`) past
its read buffer, a head and a TLS record, in whole pieces, four, the four pieces of body the charge
was counted holding. A put at the cap holds 288,737 octets above its connection at rest.

*Since 2026-11-18 (the owner's rule, the reply's side).* A reply's channel holds the answers the
node learns a reply needs in flight from its host (`door::Replying`), where it held 2 by fiat
(`REPLY_PIECES`). Its argument is the body's turned round, the host giving and the connection
taking, the connection writing one piece to its socket while the host gives the next. By Little's
law the answers in the channel at once for a reply to go at its client's pace are the rate the
connection writes them times the time each waits to be given: the host's mean time from one turn's
answers to the next turn's, over the connection's mean time to write a piece, from taking it to its
last octet written, none of it waiting for the host; rounded up, and at least one. The host's time
is a turn's, not a piece's: a turn sends every piece the channel has room for at once
(`Node::write_get`), so within a turn the time between two pieces is none, and a ratio taken over
pieces would read a host that keeps pace as one that never waits. Each piece carries the instant of
the turn that sent it, one for every piece a turn sends, a piece held for room
(`host::Local::pending`) taking the turn that sends it; the connection counts a turn at each piece
whose turn is not the last one's, the time between the two its sample, none for a reply's first
turn, whose time is its driver's first read. The ratio of the means is the ratio of the sums times
the pieces a turn gives, as measured: `⌈(Σ given × pieces) / (Σ written × turns)⌉`. The
connection writes a turn's pieces before the next turn comes, or waits for it, so the time between
turns is at least their writing, and the depth holds a turn's group: a host that keeps ahead gives
a turn the room the connection made since its last, its turns that many writes apart, and one that
falls behind gives its turns at its own pace, the depth the pieces the connection writes between
them. A phase is as many turns as admission's phase measures (`admit::PHASE`, 267), turns the
fewer samples of the two, and its depth replaces the last's. Until a phase is measured the depth
is one, the least that moves a reply: its head, or the piece the host gives while the connection
writes the last.

The samples are taken where they happen, on the connection's shard, and learned where the body's
depth is, on the node's: each connection adds its turns' and pieces' counts and times to atomics
the door shares with the host (`Submitter::replied`), each time before its count, and the host alone
takes them each turn before the door's requests (`Doorway::replies`), its count before its time,
sums them into its phase and publishes the phase's depth, which a connection reads as it makes a
reply's channel (`Submitter::reply_depth`). No lock is taken and the phase has one writer, as the
body's does; a time added between the host's taking of a count and of its time waits a turn for
its count, so a phase may hold a turn's time more or less for each connection writing as it ends.
The depth is the node's, not a connection's: a connection's own phase would grow every connection
past the bytes its charge was counted at (`CONNECTION_BYTES`), and the host's turns, the ratio's
numerator, are the node's. Over clients of different paces the mean write is the faster clients',
who write the most pieces, so the depth is theirs; a slower client's channel holds more than it
needs, within the cap.

A connection caps the depth as it makes a reply's channel at what the node's memory division gives
it (`listener::most_answers`): the bytes `derive` charges each connection past its read buffer, a
head, a TLS record and the piece it writes, taken from the channel, in whole pieces, three. A get
at the cap holds four of the host's pieces, three in its channel and one it writes, the four pieces
of body the charge was counted holding, and the host may hold one more for room
(`host::Local::pending`), its own; the pieces are the bytes the get's window let go as the host
took them (`Get::take`), so they are the connection's to bound. `tests/listener_memory.rs` makes
each request at the first depths and at the caps of both channels: a get at the caps holds its
two channels' larger rings beside what it holds at the first, 408 octets more over plain HTTP,
within `REQUEST_BYTES` as at the first.

*Since 2026-11-19, a job answered before its body ended lets go of the body.* A reply's head and
its end are two answers, so in a channel of one the end waits for the connection to read the head,
and a connection sending a body reads its reply only once the body is sent or the body's channel is
closed (`listener::conn`'s `stream`). A job answered before its body ended, a put refused or slowed
with its body unread, lets go of the body as it answers (`Host::drain_job`), its channel closed, so
the connection stops sending, reads the reply and closes (§4.5), as the native door's job lets go of
its body before its reply (§2.9 step 6c). Held until the job ended, its end gone, the body kept the
connection from the reply and the reply kept the job from its end: from the commit that learned the
reply's depth from one (9a312b2b, where two had held a head and its end), `tests/serve.rs`'s S3 put
of the first object its filling cell held for room, then refused `InsufficientStorage`, went
unanswered until its client gave it up, about four runs in five on Linux x86_64 and aarch64 and on
macOS (CI runs 44575349545, 44578467575 and 44581897076 bisected it). The host test
`an_s3_put_answered_while_its_body_waits_lets_go_of_the_body_so_its_answer_is_read` runs that put
against the connection's order of waits at the first depths.

What the door makes of a head does not grow with its fields' count: they are read where the head
holds them (`request::Parts`, `mantle_s3::sigv4::Fields`), no table of them made, which at 32
octets a field held 60,160 for a head padded to 8 KB with 1,880 empty fields; and the metadata a
version keeps copies only the fields it keeps, each name judged in place. A head so padded, its
lines ended by CRLF or by a lone LF (the most fields a head carries), holds what the same request
unpadded does. A warmed request reallocates none of the door's buffers, nor hyper-tls's: its
deframer keeps a record at its largest across records and idles, where it let its buffer go at
every drain and grew it four times for the next record (vendor/hyper-tls/VENDORED.md §13;
`a_warmed_request_reallocates_none_of_the_doors_buffers`).

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
| A read's preconditions (`mantle_s3::conditional::read`, RFC 9110 §13.2.2) | 304 `NotModified` carrying the ETag, or 412 `PreconditionFailed` (05 §2.4) |
| A key whose current version is a delete marker, read | 404 `NoSuchKey`, `x-amz-delete-marker: true` (05 §7.4) |
| A range past the object's end, or any range of an empty object (`mantle_s3::range::span`) | 416 `InvalidRange` (05 §12) |
| DeleteBucket of a bucket holding a version or a delete marker | 409 `BucketNotEmpty` (05 §10.5) |
| Another principal's bucket headed, deleted or its versioning changed, where no policy grants it | 403 `AccessDenied` |
| Versioning suspended on a bucket Object Lock holds | 409 `InvalidBucketState` (18 §2.1) |
| `Unplaced`, admission refused, a range splitting or frozen for a move | 503 `SlowDown` |
| `PutError::Full`: every volume a block's chunks were offered refused them as full | 507 `InsufficientStorage` (RFC 4918 §11.5), which stock clients do not retry (research/75) |
| An internal step whose outcome is unknown after the request's retries (§5.4) | 500 `InternalError`, which clients retry |

A body with both `Transfer-Encoding` and `Content-Length` is refused 400 and the connection
closed, as RFC 9112 requires for a framing error in a request (25 §2).

*As built (2026-10-25), the S3 listener* (`listener/respond.rs`) answers each host status with the
code and status above, `Malformed` as `400 InvalidRequest`, and refuses on its own: the codec's
refusals with their statuses (research/83), the connection closed; the router's `InvalidURI`,
`KeyTooLongError`, `MethodNotAllowed` and `NotImplemented`; SigV4's `AccessDenied`,
`InvalidAccessKeyId`, `SignatureDoesNotMatch`, `RequestTimeTooSkewed` and its malformed headers; a
body whose length is not stated before it, `411 MissingContentLength`; one past S3's largest
object, `400 EntityTooLarge`, before any of it is read; a document past its bound,
`MaxMessageLengthExceeded`; a `Content-MD5` that is not 16 octets of base64, `InvalidDigest`; and a
body whose SHA-256 differs from its signed one, `XAmzContentSHA256Mismatch`, the code S3 is widely
seen to send and neither of AWS's lists holds (research/05, research/72). Each error is S3's
`Error` document naming the resource, its request's id and the node as its host; a HEAD's carries
none.

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
it is, and its remedy is operational: a PUT, a part or a native run whose block every volume
refused as full is answered `507 Insufficient Storage`, natively `Status::InsufficientStorage`, a
status no stock client retries, where `SlowDown` would turn a full cell into retries; while a
volume's cleaner may still free a segment the put is `SlowDown`, and asked again it meets the typed
answer once the cleaner's pass has said it cannot (research/75; `tests/serve.rs`, a cell whose
stated volumes fill, through both doors). Whether to send `x-amz-retry-after` is open, since whether
S3 sends it is unverified (25 §3).

*As built (2026-10-25), the S3 listener.* The door holds a request for each connection the listener
admits, and the node takes them in the order they came while the host has room for another
operation, so a burst past the host's operations waits at the door, its bodies unread, rather than
being refused: the AWS CLI's transfers send ten parts at once, against what was then a host floor of two operations (now as many as the operations' bytes hold, §2.8), and its
multipart uploads failed `SlowDown` after its two retries until requests waited (AWS CLI 2.34.18
against a cell of one, 2026-10-25). A stopping node's refusal is `503 SlowDown` before any of a
body is read, so before `100 Continue`. A response sent before its request's body was read says
`Connection: close`, whether or not its client waited for `100 Continue`, since it may send the body
anyway (RFC 9110 §10.1.1); the listener then half-closes and reads what comes until the client
closes, at most the largest body a request may carry, so the response is not lost to a reset
(RFC 9112 §9.6; research/72). The connection budget is as many connections as the node runs
operations at most (`gateway.objects`, §2.8), each carrying one request at a time, so one past them
would only wait at the door; each connection's bytes, measured with a request in flight and its
whole body taken (`listener::CONNECTION_BYTES`, 382,052 at most, over TLS 1.3 at one connection:
382,028 counted on Windows x86_64 and the 24 a connection's way into the session store holds; a
connection over TLS 381,988 and over plain HTTP 346,132 from one connection to four on macOS arm64;
`tests/listener_memory.rs`), enter what an operation costs the node and so the count itself. It
was ten, what botocore and the AWS CLI's classic transfers open at their defaults (research/72),
thirty requests in a cell's S3 door at once. The
kernel queues as many connections as the budget. A connection past it is answered
`503 SlowDown` with `Connection: close` to its first request, from a reserve of ten more, never
closed with no answer: the AWS CLI drops a ranged GET whose every attempt met a connection closed
unanswered and ends its copy exit 0 with the object short, where a `503` it asks again and then
fails loudly (research/72 [7]). Past both a connection is closed as it comes. A connection waits on
its client's next octet at most a stock client's patience, 60 s (`bounds::STOCK_PATIENCE`):
between requests it then closes, owed no response, so a client's idle pooled connections give
their slots back; and the read after a `Connection: close` ends there too. Each connection task
holds a timer for that wait, which the node's runtime counts beside its task. Open: the process's
descriptor limit, which the budget does not yet read (a connection past it waits for one of the
node's own to close, `Listener::serve`); the reclaim of the connection that has made the least progress for longest when
the node needs the room (§4.1, §4.3); and the shared admission authority above, whose sojourn test
would refuse a request that waits at the door past its target, where now it waits as long as the
host's operations ahead of it take.

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

*As built (2026-10-25).* A node given a root key and the gateway's settings (`root_key`,
`gateway.*`, `durability.*`) hosts the drivers itself (`crates/node/src/node/host.rs`). A client
or gateway asks it on an exchange of the transfer class (`crate::object`) for S3's operations:
CreateBucket, HeadBucket, ListBuckets, DeleteBucket, PutBucketVersioning and GetBucketVersioning;
PutObject with its bytes on the exchange, GetObject with them on the reply, HeadObject,
DeleteObject and ListObjectsV2; CreateMultipartUpload, UploadPart with its bytes on the exchange,
CompleteMultipartUpload, AbortMultipartUpload, ListParts and ListMultipartUploads; and since
2026-11-14 CopyObject, UploadPartCopy, DeleteObjects and ListObjectVersions, a version named on
GetObject, HeadObject and DeleteObject, and a version's metadata on PutObject,
CreateMultipartUpload and a native upload's Open, returned on every reply that states a version
(s3-protocol.md §14). Each new operation is a kind of its own and the older operations' fields
were extended only at their end, so every earlier golden vector keeps its bytes but for the
appended field and its fields' length (`tests/golden/object.txt`). The protocol names no version
of its own yet, and its decoders take a message only when its fields end where its length says,
so a client and a node a release apart refuse each other's extended messages as malformed rather
than misread them; the versions a connection's first exchange names (§1.7) are what will let them
differ. A request's head is a few fixed fields, held for the exchange's
life: its kind, its fields' length, the operation's identity, which every mutation carries
(gateway.md §2.1; the Name range records none yet, STATUS item 9), and a put's length; its fields
ride in the body ahead of a put's bytes, each bounded by S3's own bound on what it names. A
reply's head is its status, the object's size and its fields' length, the fields (the version's
ID and ETag, Last-Modified in whole seconds, the checksum, whether the version is a delete marker,
the range served, a bucket's versioning, a page) opening its body ahead of a get's bytes. The
operation is typed on both sides of that encoding, so another door hands the host the same
operation. Each exchange is a job, at most `gateway.objects` of them, one past answered
`SlowDown`.

The operations' preconditions and ranges are S3's header texts, judged as the HTTP/1.1 listener
judges its own, by `mantle_s3::conditional` and `mantle_s3::range`. A put's `If-Match` and
`If-None-Match` become the Name range's preconditions, judged as the version commits (05 §2.2);
its checksum and `Content-MD5` are what the `Put` driver checks at the body's end, before anything
commits (05 §3.2). A get or head reads its key's current version, linearizably (§5.1's reads),
judges its preconditions against it in RFC 9110's order with the version's time taken in whole
seconds once, resolves its range against its size, then replies with the version's facts or makes
the `Get` driver over the range. A delete reads its bucket's row for the incarnation and
versioning its Name command names, so a versioned bucket's delete makes a marker (05 §7.3). A page
of ListObjectsV2 is one read of the Name range: `mantle_s3::list::page` runs on the leader's
engine over the Name layer's scan of current versions, and ends the page early, truncated, at the
first item its reply has no room for, the next page resuming exactly where it said (s3-protocol.md
§3). ListBuckets reads a page of the asker's buckets from the Bucket range's owner rows. A create
or a delete runs the bucket's coordinator (`mantle_meta::coordinator`): a create the Bucket
range's `Create`, the Name range's gate read and opened, the bucket activated; a delete
`BeginDelete`, the gate closed, the Name range probed for a version or a delete marker, and then
either the gate reopened (`BucketNotEmpty`) or the bucket deleted, its rows swept and its name
forgotten, the reply coming once the coordinator is done. A bucket's head, delete and versioning
change are refused to another principal than its owner, S3's answer where no policy grants them;
the policy itself is the listener's step (§4.2). A put reads its bucket's row and makes the `Put`
driver over it, a data key drawn for the file and wrapped under the root key's current generation
(encryption.md §2) and the scheme the durability model chooses for the cell's domains
(`mantle_ec::durability::choose`), chosen once per change of the volume table's volumes or their
domains and carried across the entries the root range applies otherwise (it ran at every entry,
2,448.5 allocations each time, research/71 step 5). The choice runs on hyper-rt's compute pool,
beside the shards and below them (its runtime.md §3.16; `host::Scheming`), not on the node's
shard, which it held while it ran, a tail for every request there: the shard asks it with the
model's inputs owned (the candidates, the count of the domains that take chunks now, the gateway's
rates and target), tagged with the applied index of the reading whose domains changed, and takes
its answer at a later look at the table. Meanwhile new blocks take the scheme chosen before
wherever it still fits the domains, no wider than they are: the model prices a scheme by the scheme
and the rates alone, the domains only ruling the wider candidates out (durability.md §4), so a
block placed under it, a chunk to each domain (`place::offer`), is exactly as durable as the
blocks before the change (within the target, or as near it as the cell came), at most at a cost in
bytes that a cheaper scheme the new domains allow saves once the answer lands. Where it no longer fits, or none was chosen yet, a block placed
under it would put two of its chunks in one domain, which the model never priced, so placements
wait for the choice, made at once on the shard as before the pool; so is one the pool refuses (none
running, at its bound, or stopping), so no table waits on a choice nobody runs. A node runs one
choice at a time: an answer for domains the table has left since is dropped and the choice asked
again for the domains it has, as a per-node task is discarded if its node changed meanwhile
(CLAUDE.md §10); a choice whose job unwound refuses the scheme, a put failing as it does where no
candidate fits, until the domains change. A put's chunks go to
their volumes as one buffer the driver sealed, shared by every copy, the node's own volume
writing from it as a peer's exchange sends from it.

A multipart upload's create and abort read the bucket's row and ask the Name range's
`CreateUpload` or `Abort`, the upload's ID its initiation time in the version-ID alphabet, so a
key's uploads list in the order they began (05 §4.7); a create naming a checksum type its algorithm
does not have, a full-object hash or a composite CRC-64/NVME, is refused before anything is
written (05 §3.3). A part streams through the `Put` driver as a file of its own, under its own
data key, its checksum and `Content-MD5` checked at its end, and is committed by the Name range's
`PutPart`, which keeps the latest part of a number (05 §4.3). A completion runs the `Completion`
driver (`mantle_gateway::complete`): it reads the upload's row and its parts a page at a time by
ReadIndex, derives the object's size, its ETag in S3's multipart form and its full-object or
composite checksum from the parts' rows, never from the request, writes the object's file of parts
in the File range, and commits the Name range's `Complete`, which checks the parts against its own
rows; asked again with the same parts, the Name range answers it from the version it made (05
§4.4). A read or command the completion's driver asks that is refused fails it `500`, which clients
retry, never the `404` an upload not found would be. ListParts reads a page of an upload's parts,
at most S3's 1,000 and as many as the reply has room for, and ListMultipartUploads a page of the
bucket's uploads through the S3 layer's paging over the Name layer's scan of uploads, ended early at
the first upload its reply has no room for.

*As built (2026-11-14), versions, copies and batch deletes* (s3-protocol.md §14; research/89). A
get, head or delete naming a version reads that version (`Query::Version`, read kind 13) where it
would read the current one: text that is neither `null` nor 13 characters of mantle's alphabet is
refused before anything is read, and a delete marker named is `MethodNotAllowed` with the marker's
facts. ListObjectVersions is one read of the Name range (`Query::Versions`, kind 14) over the Name
layer's scan of versions, ended early at the first version its reply has no room for. A copy is a
job that reads its destination's bucket row, a part copy its upload's row, and then its source's
version, current or named; judges the source's conditions and S3's refusals against that version;
draws the new file's data key; and then drives a `Get` over the source version's file and a `Put`
of the new file together, each piece the get yields fed to the put as the put takes body, so a copy
holds at most the get's window of blocks and one piece, and a slow put holds the get back. The put commits a version or
a part as a PUT's does; a copy whose new version is the null version reads the mark its write left
(`Query::Mark`, kind 15) for the version's time. A copy takes two of the host's
`gateway.objects` operations, one for each driver: the local door holds a copy while fewer are
free, the requests behind it waiting so the door keeps its order, and the native door answers it
`SlowDown`. DeleteObjects reads its bucket's row once and then asks each object's delete as a Name
command of its own, as many at once as its share of the node's command asks,
`(command asks - 1) / gateway.objects`, the one held back being its enlistment's; each delete's
outcome is that object's result, in the document's order. Tested at both doors against a cell of
three (`crates/mantle/tests/s3.rs`, `native.rs`, `stock_clients.rs`) and in
`node::host`'s `what_a_version_metadata_or_a_copy_names_is_refused_before_anything_is_read`.

The host serves each driver's requests as the table above routes them: `Place` from the node's
replica of the volume table (§6.6), read from the root range's rows once for each entry that
replica applies, the only thing that changes it; a chunk to its volume's node, in-process for the
node's own; a command or read to its range's leader through the node's own sessions (§6.6). A
put's body is read only as fast as its driver takes it, a piece at a time, and a get's bytes are
taken from its driver only as fast as the exchange takes them. The node's asks out are bounded
(commands, reads, and chunk asks out to its peers): a request that finds them full waits in its
place and is asked once an answer gives room back. That bound is the node's own, not a volume's
refusal, which sends the chunk to the next volume offered (§5.2). Each asker takes only its own
asks' outcomes (`Node::take_command`, `take_read`, `take_chunk`), and the asks of a job that ended
before they were answered, its exchange lost or its driver failed, are let go, their outcomes
dropped as they come. The host's asks carry no deadline of their own (§5.4 is to be built): a job
ends with its exchange. Tested with `mantle node` processes (`crates/mantle/tests/node.rs`): a cell
of three hosting every layer creates a bucket and takes objects of 0 and 1 bytes, either side of a
sealed segment's 64 KiB, 3 MiB and 20 MiB (three blocks) through each node in turn; every object
reads back whole and its middle third through another node; each block's chunks are on distinct
nodes; and with each node in turn killed by `SIGKILL`, every object reads back whole and in part
through the other two, and through the node once started again. It passes with a busy loop on
every core. And with `mantle serve` processes (`crates/mantle/tests/serve.rs`,
`s3s_operations_answer_as_s3_does_through_every_node`): through each node of a cell of three,
conditional writes (`If-None-Match: *` twice, `If-Match` right, wrong and on no object),
conditional reads in each order AWS documents, a checksum and a `Content-MD5` refused with nothing
committed, ranges, heads, deletes conditional and not, a versioned bucket's stacked delete
markers, missing keys and buckets, another principal refused, a bucket deleted only once empty and
made again, and 1,050 keys listed in pages of 1,000 and of 97 through the three nodes in turn, every
key once and in order. And (`a_multipart_upload_completes_through_another_node_with_one_killed_between_parts`):
an upload whose parts go through different nodes, one node killed by `SIGKILL` and started again
between parts, a part sent again, completes through another node into an object whose bytes,
ETag (MD5 of the parts' MD5s, `-3`) and full-object CRC-64/NVME are S3's, asked again with the same
answer, read back whole and across a part's edge through every node, no block of it with two
chunks on one node; parts list a page at a time and the upload among the bucket's; every refusal is
S3's code (a part number of 0 or 10,001, an upload not there, a stale ETag, parts out of order, a
part not the last under 5 MiB, a checksum type its algorithm lacks); and an upload aborted is
neither listed nor readable, its second abort finding none.

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
returns them; a place that cannot give them (`Corrupt`, `NotFound`, `Range`, its volume failed,
fenced, closing or not there, its peer gone) is `Unreadable`, and the driver reads another copy or
decodes the block (gateway.md §3). One refused for want of room, the volume's reads full (`Busy`),
a bound of the transport's reached for now or a period that moved nothing (`Stalled`), waits and
is asked of the same place again, as a put's chunk is (§3.8, `host::taken`), within its client's
patience; one no volume serves as it was asked ends the GET `500`. A volume that returned
`Corrupt` also reports the chunk for repair (chunk-store.md §7).

*As built (2026-10-21).* A node's storage role (`crates/node/src/storage.rs`) opens each volume
its configuration names before its runtime starts, since an issuer's start and a volume's
recovery wait on the device: it takes the device directory's `LOCK` (§1.6), starts one issuer a
device the OS names, lays the volume out under another name at its first start and renames it
into place, and opens it after. Chunks travel on a class of their own, `Transfer`, node.md §3.1's
lower request level, below the requests someone waits on and above bulk, sent by nodes and
gateways and never by clients (`crates/node/src/chunk.rs`): a put's head names the volume, the
key and the sender's CRC-32C, its body the bytes; a get's head names the range; a reply's status
is the volume's own answer, typed (`Stored`, `Read`, `Busy`, `Full`, `Fenced`, `NoVolume`,
`Checksum`, `TooLarge`, `NotFound`, `Range`, `Corrupt`, `Exists`, ...). A put is written from the
buffer it arrived in (`Volume::put_checked_waking`) and answered when the volume's writer wakes
the node; a get is a batch at the device's issuer, taken as its answer comes (`Volume::reader`),
so the shard never waits on a chunk's I/O. A put a peer sends takes room from what the volumes'
writer queues hold before its body is read, and one the node has no room for is held on its
exchange, its body unread, first come first served, and read in the turn a volume's answered
write gives room back (`node::hold`, 2026-11-19): refused unread (`Refusal::Budget`), as every
such put was before, its asker heard nothing when the room came back, room freed by other nodes'
writes or the volume's cleaner answering nothing of its own, and asked again only at its put's
next beat, a probe timeout, about a second (`Host::step`). A held put's asker is told at every
half beat that it is held, as a chunk its volume holds is (research/77 D2), the vouch resting on
the writes in flight that hold the room (a put is held only once its room was refused, which
`take_room` does only while some is taken; STATUS.md's conditions on a vouch), so its transport
keeps the exchange out (`Endpoint::working`; `tests/put_body.rs`: a 4 KiB put held unread three
periods is answered, and given up `Stalled` within two when not vouched for); it is refused
`Budget` as before once its request's patience passes, and let go if its exchange ends. The held
puts are bounded by the chunk exchanges the volumes take at once and their bodies by the room
(one larger alone, as it is taken alone), past either refused `Budget` as it came. A full volume
still answers each put `Full` once taken, which gives its room back, so a full cell still
answers `InsufficientStorage` (chunk-store.md §8). A put the queue refuses `Busy` once read is
still answered so: holding it would keep its bytes, and `Volume::put_checked_waking` takes them
whether or not it queues them. A node asks its own volumes
in-process, the laptop's path (§5.5), and its peers' over the transfer class (`Node::put_chunk`,
`Node::get_chunk`); a gateway asks them through a client whose certificate the nodes name a
gateway's (§3.6; `Client::put_chunk`, `Client::get_chunk`), one exchange at a time, a reply's
body read from the transfer class's budget. Tested node to node over real sockets and disks
(`crates/node/tests/cluster.rs`): chunks on every node read back whole and in part from any node,
each refusal answered why, and what a peer acknowledged read back after it stops and starts again.
And with `mantle node` processes (`crates/mantle/tests/node.rs`): a gateway's puts round three
nodes, one node killed with `SIGKILL` mid-put; every put to a running node is stored, the killed
node's until the kill and lost after; with it down its peers' chunks read back whole; started
again it holds the same volume, every chunk acknowledged reads back whole, a lost put landed whole
or not at all and is stored when retried (a retry's bytes are the first's, chunk-store's audit
B05), and each refusal is typed. It passes with a busy loop on every core.

### 5.3 Sessions

Each gateway holds one session with each range it writes to, registered by a `Register` command
at first use (replica.md §1). Commands carry the session, a serial, and the lowest serial whose
answer the gateway has not yet received, so the range forgets answers only once the gateway has
them. A gateway keeps up to as many entries out a session as the range's log has frames in
flight, `ask::ENTRIES_OUT`, two (§6.6; one until 2026-11-17), each carrying every command waiting
that fits the entry's bytes; the range keeps their answers as its row holds them, and refuses the
rest `AnswersFull` before they take effect, which the gateway asks again under their serials
(§2.8). The rest wait at the gateway, bounded by the operations that ask them (§2.6).

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

**A step's budget.** A range step a request waits on, a read or a command, waits without a leader
that can serve it only as long as its range is judged able to have one again. Where the node holds
a member of the range whose detectors judge, their judgment decides: the step's clock runs exactly
while that member and the members its detectors trust are no quorum, leading or following, and the
step ends once the clock has run the range's patience (§2.4's law, `Node::patience`: the longest a
peer's evidence may go unmoved before it is suspected, the judge's wait for a completion of its own,
the range's expected election and one more round). A member left no quorum has a range that cannot
elect or commit from there: a leader leads on and hands the range to an heir that can be elected
without it (hyper-raft `Raft::left_no_quorum`), committing nothing and confirming no read meanwhile
(Raft dissertation §6.4), and a pre-candidate asks no more (`Raft::stop_asking`). A failover keeps a
trusted quorum, so a step waits out the election however long it takes: the patience is the
election's expected time, not a bound on it, and a clock counted from the leader's loss cut the
cluster test's failover in a young cell, whose detectors began to judge 3.5 s after the stop.
Where nothing here judges (the node holds no member, or its detectors are not yet calibrated, which
a peer that died before any link was measured leaves so for good), the step's own time decides: its
clock runs from its first round of asking that found no node to answer, or from its own member
refusing it (no room) or taking it, and nothing short of the step's end stops it. A redirect that
names a leader stops nothing there: a member that cannot elect follows the leader it left
(`Raft::stop_asking`), which a node cut from it never reaches. That budget is the longer of what the
members stated in their redirects, their expected election, and one round of asking each node, each
given the transport's idle timeout (RFC 9000 §10.1). While an exchange to a leader is open the
transport's progress rules judge it (§3.8), so a slow step that moves is never cut. A read
asked of a leader its member trusts that answers nothing, its opens refused or its exchanges ended
unanswered, has a second clock (2026-11-19): from its first ask since a node last answered it, any
reply resetting it, over the budget where nothing judges; its member's trust stops only the first
clock, and on the node simulation's new voter (seed 1) a read so asked again for good was never
ended, a gateway's GET on it waiting out its client. A round of asking that found no node to answer
pauses the step by its backoff, and the pause ends early only where no leader was named when it
began: one taken while the member named a leader that answered nothing is waited out, never ended
at once to ask that leader again at every turn. A step the
node's own member took, leading with a trusted quorum, is the range at work: a leader whose disk or
connections stop it is suspected by its followers as a crashed one is (hyper-liveness's flush
proof; the plane is keyed by the connection), so the range elects another and the step follows it.
Two waits stay bounded only by the deadline a request carries (research/77 D2): a device every node
of a cell shares, a laptop's cell, that stalls, leaving each judge with its own proof outstanding
and suspecting no one (hyper-liveness, "A judge whose own proof stalls"); and a node cut from a
leader that its trusted quorum still follows, in a partition that is not transitive, which neither
elects nor reaches it. The node's own statements to its ranges, its enlistment and its history
(§2.4), are no request's: each is asked until its range answers, the range's laws deciding when,
and a founder's enlistment so waits for the founders its cell needs, which may start tens of
seconds apart (an enlistment bounded so ended the native suite's founders, exiting, once their
cells took longer than 6 s to form). Past its budget a request's step ends and the request is
answered `503 SlowDown`, a cause that passes and that clients retry with backoff (§4.5;
research/75); a command whose outcome is uncertain keeps its identity, so its retry is answered
from what it made. Every step was asked with no deadline before this: a read whose range named no
leader paused and asked again without end, and a node whose own member led with no quorum held its
own reads and commands without end (research/77 D2; CLAUDE.md rule 2).

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
| Range commands | a CRC-32C per command, computed where the command is first encoded, at the gateway, carried by the leader in the gateway's bytes, and checked at apply on every replica: a mismatch is answered `Corrupt`, writes nothing, is sent again once and counted against the gateway's node (replica.md §2, as built) |
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

*As built (2026-10-19).* The port gave way, by the owner's decision of 2026-10-06, to an engine of
its own structure and format (engine-structure.md): the production engine is
`mantle_engine::shard_db::ShardDb`, one instance per range replica in its own store file
(`range-GROUP.store`, §1.6) on hyper-block's device file, with no write-ahead log of its own,
held as the metadata layer's engine by `mantle_range::Stored` (metadata.md §4). Its page,
extents and memtable are derived per node (§1.4). The trait has not yet gained the checkpoint of
files, export or ingest: snapshots still carry every row through `image` and `install`.

*As built (2026-11-19), maintenance in a turn's idle time.* A range's engine paid its packings,
compactions and consolidations only as its puts and checkpoints did, so after a burst of PUTs a
range taking only GETs read the busy trunk until its next write: about 18 sources a get, against
2.6 once its maintenance was done (40,000 Name-sized rows under the derived engine; read-amp's
counting test at 49e6ac8f). Each turn of a shard (the node's own and each range shard's) now ends,
after its reads, writes and messages are out, in its ranges' maintenance (`Engine::upkeep`,
`ShardDb::idle_step_now`, `node::upkeep`): in SILK's order (the packing, then the trunk's work and
what its workers finished, then the consolidations seeks paid for, then the cache's; research/34
§5), waiting for nothing, a job still out on a worker left owed rather than waited for. A turn's
budget is as long again as the turn's own work took, in units at the rate the shard's turns have
done them, both measured, as the engine's runtime slices are sized (`mantle_engine::ranges`): a
request that arrives during it waits no longer than that, and a shard with no request gives
maintenance about half its time. Each owed range is first in turn. While any engine owes work it
can do now the shard's wait is due at once, after one yield to its other tasks; with none owed,
or none it can do now, the wait is what it was. A step that finds nothing to do but a job out on
a worker (nothing back, nothing handed out, nothing merged on the shard) answers none done and
work owed, and leaves the task it runs on registered on the pool's channel back, as the pool's
`receive` does for the runtime's ranges, so that the worker's next message wakes it once
(`ShardDb::idle_step_now`, `Pool::wake_on_message`): the wake ends the shard's wait, and the turn
it begins takes the result. Such a look used to answer a unit done, so the shard turned again at
once to look until the job came back. The node's own wait asks such an engine again before it
sleeps (`Node::wait`): the process awaits between the node's turns (saying a line), and a wake
that came then ended that await rather than the wait, which would sleep with the result in hand;
asked, the engine takes the result or leaves the task waiting on its workers again, with no await
between that and the wait. A range shard's loop awaits nothing between its maintenance and its
wait. Maintenance writes no checkpoint and flushes nothing, so
what is durable and when a checkpoint answers are unchanged. Counted (`node::upkeep::tests`,
40,000 Name-sized rows under the derived engine, checkpointed, then a get a turn, each turn
ending in its maintenance and followed, as a node's wait follows it, by a yield while maintenance
is owed and otherwise by a wait for any wake of the task, at most the idle timeout; a debug
build on macOS aarch64, 2026-11-19): inline, a one-row seek listed 5.9 sources right after the
burst and 1.0 after 4,222 to 4,616 turns (about 40,100 units; the turns follow the measured
budget); with the node's one worker, 12.8 and 1.0 after 129 turns, 120 of them looks the shard
turned again at once after, and since the wake 10.6 and 1.0 after 15 turns, none polled: six
found only the job out and waited, each wait ended by the worker's message and none by its
bound, and the turn each wake began found the result. A get read 1.005 and 1.018 pages, then
1.000; no turn did more than its budget, and maintenance made no checkpoint and no flush. The
test holds the turns within the burst's rows. Through a range shard's own loop
(`shard::tests`), an engine whose job ended while the shard waited had its result taken at the
next turn.

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

*What flushes while a node serves only reads (2026-11-19).* A node's `counts` answer says each
cause's flushes since it opened (`process::flush_counts`), and `mantle bench doors` says each
one's growth over every phase: the log's frames, flushes, confirmations written on their own and
slots filled ahead (hyper-log's `LogStats`); the liveness stream's writes; the ranges' writes by
what they carried, entries, a hard state alone, the commit alone, a new start; the engines'
checkpoints and their stores' flushes; and the puts the chunk volumes took (`node::FlushCauses`).
As built, a range that only serves reads writes nothing: a read is confirmed by a round of
heartbeats, the range writes its commit alone after no quiet period (`mantle_range`'s `QUIET`),
and its engine checkpoints only when a compaction asks, which the node asks only when the log
refuses a write for room (above). A checkpoint flushes the store twice, its map and then the
superblock naming it, and nothing else does: a compaction's output is written unflushed and made
durable by the checkpoint that first names it, and a checkpoint with nothing applied since
flushes nothing (`a_group_serving_only_reads_writes_nothing_to_its_logs`,
`its_store_flushes_only_at_a_checkpoint_and_twice_there`). What remains of a read-only node's log
flushes is the liveness stream's proof: an empty update each heartbeat interval in which nothing
else became durable (hyper-liveness's flush-proof mode, `Liveness::set_disk_apart` off), a lone
frame that costs its flush and its confirmation's (raft-log.md §6). Fewer would take the owner's
decision on that mode or on confirming a frame that holds only empty updates (durability.md §7.5).

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

*As built (2026-10-20).* A node replaces a member lost for good (`Node::replace`; `mantle node`'s
`replace FAILED JOINING`, in every range the node hosts): while it leads a range it proposes what
`membership::Replacement` names next at each turn, a change only while none is pending, and the
node that leads when the replacement ends reports it once (`replaced`, with the range's group). Every node may be told it, since the replacement
is stateless. The joining member is a spare identity every directory holds from the start, its
node started as the founders were, with their configuration and rows, of which it is no member
until the learner change adds it; moves chosen from size and load are still to build.

*As built (2026-11-14): members that come and go* (docs/design/membership.md §6). A node's
directory takes an identity it did not hold: a granted node's, by the root range's rows. Each
range's leader steps its configuration toward the cell's members (`membership::Toward`): a member
serving a voter, added as a learner and promoted once caught up; one leaving a learner of the root
range alone; one gone or removed nowhere; an absent voter a learner where dropping every absent
voter lets the range survive more failures; one server at a time, adds before removals.

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

### 6.6 The cell's ranges and the volume table

A node hosts the ranges its configuration names, each `range = GROUP LAYER`: the cell's root
range and its Bucket, Name, File and Block ranges, each a Raft group of its own on the node's one
log (raft-log.md §1), its rows in an engine store of its own. A cell of three nodes hosts every
range on every node, voters one to a node (§1.5's cell of one grown to the three domains `f = 1`
needs, §6.5); ranges placed on some nodes of a larger cell are placement's (§7).

**The volume table** is the root range's (§1.5; architecture §4), `mantle_meta::root`: a row per
node, its failure domain, and a row per volume, the node that holds it and its bytes, with a
census of both. A node enlists itself and its volumes by one `Enlist` command. The range records
what it does not hold and answers `Conflict`, changing nothing, for a node whose domain or a
volume whose node or bytes it holds otherwise, since a device moved between nodes is a move to
make and never a silent overwrite; and `Full` past the most nodes and volumes the cell states, as
every map keyed by a value a peer chooses is bounded (CLAUDE.md §2). A domain is at most 255
bytes, DNS's bound on a name (RFC 1035 §2.3.4). The table is read a page at a time
(`read::Query::Volumes`), each page as many volumes as a reply carries counted at the longest
domain, and a read is answered only by a range of the layer whose rows it reads (`WrongLayer`
otherwise).

**Placement** (`Place`, §5.1) reads the node's own replica of the root range, which every node of
a three-node cell holds: a stale table costs a refused chunk, never a wrong placement, since the
Block range refuses two chunks on one volume (metadata.md §1) and a volume refuses a write it
cannot take (architecture §5's invariant). It offers every volume, one to a domain first, in an
order drawn from the block's id, which is uniform random (metadata.md §1), so blocks spread
evenly and the same block is offered the same order again; then the volumes of domains already
used, which a block takes only when the cell has fewer domains than the block is wide, as §1.5
lets a cell short of domains place on the domains there are. Choosing by two random choices on
credits (§7; research/27 D11a) waits for the credits nodes advertise.

**A node's own commands.** A node enlisting, and the gateway host after it (§5.1), send commands
to ranges as a gateway does: each within a session the node registers with the range at first
use, its serials in order (replica.md §1). One whose range this node leads is proposed
in-process; one whose range another node leads goes to that node on the request class, following
the leader a node names, and is asked again with the same session and serials when its exchange
is lost, which the range's sessions answer without applying twice. The asks out at once are a
stated bound. The root range's leader takes an enlistment only from the node it names, over that
node's own connection.

*As built (2026-10-21).* The root layer, its rows and commands (`mantle_meta::root`), and its
paged read (`crate::read`), with the layer check on reads; a cell of three `mantle node`
processes hosting the root range and a range of each other layer reads its volume table through
a client (`crates/mantle/tests/node.rs`). A node's own commands (`Node::command`,
`crate::ask`): a session registered at first use, up to `ask::ENTRIES_OUT` (two) entries out a
range at once, as many as the range's log has frames in flight (research/71 step 9; research/87
§1), each carrying the commands waiting in order (at most a session's unacknowledged answers,
within the range's entry bytes), so a command asked while one entry is out goes in the next
rather than waiting out the first's flush, quorum and answer. Each entry out is asked on its
own: proposed in-process where the node leads, its proposal waiting under its own place
(`Out::id`), else sent to the leader the node's member or the last reply names, or to each node
in turn, pausing by RFC 6298's doubling backoff once every node was asked; its reply found by
its exchange, its answers given to its own commands, asked again with the same serials when its
exchange is lost, given up at its commands' deadlines or its own clock without a leader, and a
command whose session expired asked again under a new one, in its place. Entries out together
keep no order between them, as the range may apply either first: what needs an order waits for
the answer before it (a PUT's block, file and name commands; the node's statements, one out at
a time). A range holds uncommitted `ENTRIES_OUT` entries an identity and one of its leader's
(`derive::in_flight`). Tested node to node over real sockets (`crates/node/tests/cluster.rs`):
the gate asked in-process by the leader, puts asked by a follower over QUIC, and puts out when
the leader is stopped, each answered once with the version the client then reads as its key's
current; and two puts the leader proposes beside each other while cut off, both out at once,
each answered its own version once the others' new leader is asked. Every entry a node proposes is stamped with its wall
clock (`Wall`) as the entry's time, the leader's time every command in it takes
(`mantle_meta::wire::Entry::at_ns`), where before a client's entry kept whatever time its client
wrote. A node with volumes states its failure domain and the cell's bounds on the table, hosts
the root range, and enlists itself and its volumes there once it listens (`mantle node`'s
`enlisted` line), the root range's leader taking an enlistment only from the node it names over
that node's own connection (`command::Status::Forbidden` otherwise); a refusal stops the node with
the range's answer named. Placement's order is `crate::place::order` over the table's volumes
(docs/research/63): a gateway that reads the table pages places from them. A cell of three
`mantle node` processes hosting every layer enlists each node's volume, the table a client reads
names each volume's node and domain, placement over it offers a volume of each node first for
each of a thousand blocks, a node started again enlists again with the table unchanged, and one
started in another domain is refused and ends saying why.

## 7. Placement, repair and cells

| Piece | Exists | To build |
|---|---|---|
| A block's scheme from the domains available and the durability target | `mantle-ec` durability model and choice (durability.md) | choosing per block when sealed, from the cell's live domain count |
| Placement of a block's chunks | the Block range refuses two chunks on one volume (metadata.md §1) | the cell's volume table in the root range, published to every node in full on a loop (architecture §4); gateways choosing each chunk's volume from placement's candidates by two random choices weighted by advertised credit, never a global least-loaded order, which every gateway would read alike in a herd (research/27 §6.2, D11a: two choices give "exponential improvements" over one); among feasible devices, the one whose projected budget exhaustion is latest (chunk-store.md §9.3); copysets per media pool that satisfy the domain rule (04 R2; storage-classes.md §3); the Block range refusing chunks that share a domain |
| Repair | *Built (2026-11-14; repair.md):* the cell's facts in the root range (each node seen or away as the root's leader judges by its detectors and connections, its measured availability; each volume active, leaving or gone, gone only by its node's own enlistment or leave); a service beside the Block range's leader walking the reverse rows of the volumes that need it in passes by margin, rebuilding each chunk from verified sources and naming it by the Block range's compare-and-swap, an away node's chunks waiting the durability model's repair time less the measured rebuild time, the places they left kept (Carbonite's reintegration); concurrency from the measured pass time | budgets per disk, NIC and rack beyond the operation slots it shares with clients; reconciling chunks no block names with a fence against a rebuild naming them (repair.md §8); a fence between repair and reclamation (audit §8.4) |
| Reconciling chunks no block names | the sweeps settle files and blocks (metadata.md §2) | each volume's chunks checked against the reverse rows, taking those past their deadline (metadata.md §6) |
| Rebalancing and draining | *Built (2026-11-14; repair.md §4):* the domain rule restored as domains return or join (a chunk sharing its domain with another moved to one holding none of the block's); a node's leave drains its volumes through the repair path, retires them and the table forgets them; placement offers only volumes that take chunks, weighted by bytes times measured availability | moves for disk space once a volume is at risk, under a byte budget ranked by imbalance removed per byte; an operation gate that keeps every range's quorum and every block's margin before a drain or restart (architecture §7) |
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
(11 §14). The node's operating envelope is each derived limit, the value it took, and the input
that bound it (audit §12.6), with the accounting audit §15.1 asks for evaluated on measured values,
among them the node's memory as the sum of its owners and the heartbeat rate its led ranges cost.
As built, the limits, their values and their inputs are said once, as the node starts:
`mantle serve` says each with its basis before it runs, and `mantle derive` says the same without
serving (§1.4). A command that says a running node's envelope, with the accounting on measured
values, is not built; `mantle status` is the supervisor's readiness check alone (§1.6;
kubernetes.md §5).

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

*As built (2026-10-20).* `crates/node/tests/sim.rs` runs a cell of three or five nodes and its
clients on one hyper-rt simulation runtime (`SimRuntime`): every shard on one thread on a
virtual clock, every socket on its fabric. The nodes are the production owner (`Node::open_on`),
elected by suspicion and replicating over QUIC and the sealed plane. Each node's log is
hyper-log on hyper-block's simulated device (`NodeLog`), and its range runs on the model engine.
The clients are the production client. A run is its seed: keys, certificate serials, every
endpoint's draws and a client's TLS draws derive from it (`KeyPair::from_seed`,
`Serial::from_octets`, `Endpoint::new_seeded`, the client's `Seeded`: its hellos' randoms and
the order their extensions are encoded in), so the handshakes carry the same bytes in the same
order and a seed run twice gives the
same histories, operation by operation. The paths are focal's profiles: one switch, one
continent, or across the planet. Half way through the clients' operations, the leader or a
follower is cut off, loses power (its device keeping what it flushed, as the seed draws), or has
its device fail a write, which stops it as its process would. On some seeds every path also
loses 2% of its datagrams meanwhile. The WGL checker judges each key's history, and every node
must apply as far as the others. Two further tests hold laws the Windows runners exposed (§3):
a registration slower than every exchange its client measured is applied once, and a request
answered without its body is still answered when its body arrives after its head. Three more
deafen the leader's plane, so that it hears no peer's plane while they hear its, as a host that
loses what it is sent and not what it sends: once it has judged its links, for a while and for
good, and at its election, before it has judged any. Its followers trust it, for they hear it;
it suspects them and steps down, and the others elect one of themselves, which it follows by its
lanes. A Windows runner showed the same end with no fault at all: a leader on a starved host
suspected both its followers at once. Two hold a batched entry's answer to its flushes
(durability.md): every node's power cut at once at a moment the seed draws around the answer,
after which the cell applies through any entry it answered; and the flushes held, each held
node's store keeping its writes' answers from its ranges as a device whose flush has not ended
(`Sim::hold_writes`), so that an entry is unanswered while the nodes left to flush it are one
short of a quorum, and while its leader's own flush is held after its followers' quorum applied
it, and answered whole once they end. Still to build: many ranges per node, the clients in their
millions, and the faults and loads listed above.

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

*As built (2026-10-25).* The AWS CLI v2's and boto3's suite runs on every CI target and the
constrained jobs against a cell of three `mantle serve` processes (`crates/mantle/tests/
stock_clients.rs`, §4.2); the AWS SDK for Rust and ceph's s3-tests are still to run.

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

- **Each class's share above its floor** (§1.4): the knee where a class's throughput stops growing
  as its share grows, measured at the scale step, decides what a budget above the floor buys.
- **A gateway known by its name under the authority** rather than by its pinned certificate (§1.5,
  §3.6). Clients are so known (§3.6, 2026-10-26), and nodes of a cell made by grants by their
  rows in the root range (membership.md §2, 2026-11-14).
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
