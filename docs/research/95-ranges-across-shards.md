# 95. A node's ranges, its log and its doors across its shards: the sources

The sources for node.md §2.9: how shard-per-core systems place Raft groups on cores, how they
feed a shared log, and what the operating systems and QUIC allow one UDP port to do. Each quote
was read at its source on 2026-11-18 (UTC), the URL given; a claim that could not be read at its
source is marked UNVERIFIED and no decision rests on it.

## 1. Seastar: a shard per core, message passing between them

- Seastar's tutorial (`doc/tutorial.md`, master, https://github.com/scylladb/seastar/blob/master/doc/tutorial.md):
  "Memory, data structures, and CPU time are not shared; instead, inter-core communication uses
  explicit message passing", and "communication between cores happens via explicit message
  passing".
- research/26 §4.1 holds the rest, read at seastar.io's "Shared-nothing Design": Seastar "runs one
  application thread per core, and depends on explicit message passing, not shared memory between
  threads. This design avoids slow, unscalable lock primitives and cache bounces".
- Not read at the source this time: the tutorial's text on `smp::submit_to` and `sharded<T>` was
  past what the fetch returned (UNVERIFIED). The design uses only the message passing above.

## 2. Redpanda: every partition, a Raft group, assigned to a core

- "How Redpanda Works" (https://docs.redpanda.com/current/get-started/architecture/), "Raft
  consensus algorithm": "Every topic partition forms a Raft group consisting of a single elected
  leader and zero or more followers (as specified by the topic's replication factor)."
- Same page, "Thread-per-core model": "Redpanda implements a thread-per-core programming model
  through its use of the Seastar library." "This allows Redpanda to pin each of its application
  threads to a CPU core to avoid context switching and blocking." "It combines this with message
  passing to asynchronously communicate between the pinned threads."
- "Cluster Balancing" (https://docs.redpanda.com/current/manage/cluster-maintenance/cluster-balancing/),
  "Intra-broker partition balancing": "In Redpanda, every partition replica is assigned to a CPU
  core on a broker." "Redpanda prioritizes an even distribution of a topic's partition replicas
  across all cores in a broker." `core_balancing_on_core_count_change`: "Set to `true` to rebalance
  partition assignments across cores after broker startup, if core count increases or decreases"
  (default true). `core_balancing_continuous`: "Set to `true` to rebalance partition assignments
  across cores in runtime" (an enterprise licence's).

So a Raft group lives on one core, placed for an even count across the broker's cores, and is
placed again when the core count changes between starts; moving groups while running is a separate
balancer.

## 3. ScyllaDB: tablets balanced across a node's shards

- "Data Distribution with Tablets" (https://docs.scylladb.com/manual/master/architecture/tablets.html):
  "The tablet load balancer decides where to migrate the tablets, either within the same node to
  balance the shards or across the nodes to balance the global load in the cluster"; "tablets are
  balanced across shards in each node"; `tablets_per_shard_goal` "is a soft limit and not a hard
  constraint".
- The page does not say that each tablet is a Raft group of its own, nor where ScyllaDB's
  topology Raft group runs; the design claims neither (UNVERIFIED).

## 4. TiKV: many Raft groups on a pool of pollers, their log writes handed to writers

- The `batch_system` crate (https://tikv.github.io/doc/batch_system/index.html): `BatchSystem`,
  "A system that can poll FSMs concurrently and in batch"; `Router`, "Router route messages to its
  target mailbox"; two kinds of FSM, a normal one that "usually represents a peer" and a control
  one that "usually represents something that controls how the former is created or metrics are
  collected". The page does not state whether an FSM is pinned to one poller thread.
- TiKV's configuration file (https://docs.pingcap.com/tidb/stable/tikv-configuration-file/):
  `raftstore.store-pool-size`, "The allowable number of threads in the pool that processes Raft,
  which is the size of the Raftstore thread pool" (default 2); `raftstore.apply-pool-size`, "The
  allowable number of threads in the pool that flushes data to the disk, which is the size of the
  Apply thread pool" (default 2); `raftstore.store-io-pool-size`, "The allowable number of threads
  that process Raft I/O tasks, which is the size of the StoreWriter thread pool" (default 1; 0
  before v8.0.0).

So TiKV routes each Raft message to its group's mailbox, drives groups on a pool of threads, and
hands their log writes to a separate writer pool, one thread by default: many drivers, one writer.

## 5. One UDP port, several sockets

- Linux, socket(7) (https://man7.org/linux/man-pages/man7/socket.7.html), `SO_REUSEPORT` "(since
  Linux 3.9)": "For UDP sockets, the use of this option can provide better distribution of incoming
  datagrams to multiple processes (or threads) as compared to the traditional technique of having
  multiple processes compete to receive datagrams on the same socket." "To prevent port hijacking,
  all of the processes binding to the same address must have the same effective UID."
- macOS, setsockopt(2) (the development machine's own manual page, Darwin 25.4): "SO_REUSEPORT
  allows completely duplicate bindings by multiple processes if they all set SO_REUSEPORT before
  binding the port. This option permits multiple instances of a program to each receive UDP/IP
  multicast or broadcast datagrams destined for the bound port." It states no spreading of unicast
  datagrams.
- Windows, "Using SO_REUSEADDR and SO_EXCLUSIVEADDRUSE"
  (https://learn.microsoft.com/en-us/windows/win32/winsock/using-so-reuseaddr-and-so-exclusiveaddruse):
  "Once the second socket has successfully bound, the behavior for all sockets bound to that port
  is indeterminate", the exception being multicast, where "data will be delivered to both sockets".

So spreading one port's unicast datagrams over sockets is stated on Linux alone.

## 6. QUIC's connection IDs

RFC 9000 (https://www.rfc-editor.org/rfc/rfc9000.html):

- §5.1: "Each endpoint selects connection IDs using an implementation-specific (and perhaps
  deployment-specific) method", one that lets packets carrying such an ID be routed back to the
  endpoint and identified by it on receipt; the connection ID's primary function is to ensure that
  "changes in addressing at lower protocol layers (UDP, IP)" do not deliver a connection's packets
  to the wrong endpoint; and "Connection IDs MUST NOT contain any information that can be used by
  an external observer" to correlate them with the other connection IDs of the same connection
  (the quotes are the sentences' openings as read; the rest is paraphrased).
- §5.2: a packet whose Destination Connection ID is non-zero-length and matches an existing
  connection is processed for that connection; with a zero-length one, the endpoint matches by
  address.

So a node may put the shard that holds a connection into the connection IDs it issues and route
each datagram by its Destination Connection ID, whichever socket received it, provided an observer
cannot read the shard out of the ID: the shard is written under a key, never in the clear.

## 7. Mantle's own ground truth

- research/71, "Step 9, after": a 4 KiB write and `fdatasync(2)` on the box's btrfs took 5.6 ms at
  the median alone, 11.0 and 11.4 ms with 6 and 16 writers at once, "124, 466 and 478 flushes a
  second in all: btrfs joins concurrent flushes, so concurrency, not one writer, reaches the
  device's rate"; and group commit's rule that one log write and flush commits the group (Helland
  et al., Tandem TR 88.1, §2; DeWitt et al., SIGMOD 1984, §5.2), with Raft's batching and
  pipelining (Ongaro 2014, §10.2).
- hyper-log (`vendor/hyper-log/src/lib.rs`, crate documentation): "one log per device, shared by
  every group on it, so that one flush commits every group's writes"; "the writer gathers every
  update that arrived while the last flush ran into one frame"; the log is "a handle to its owner,
  which any number of threads may share by reference"; callers reach the owner by message and hear
  back through tickets; "A log runs those two threads, whatever its number of groups or callers."
  Its refusal `LogError::Busy` is its queue holding all it takes (`node::refused_for_room`).
- Run r20 (the coordinator's, a three-node cell on one Mac Studio, M3 Ultra, 32 cores; a release
  build of lean-doors `aef9efae`; a closed ladder of 1-byte PUTs through the native door): 1,290
  PUTs a second at 512 clients, 0.69 ms of server CPU and 7.0 M instructions a PUT, the three
  nodes summed; 2,284 at 1,024 clients, 0.74 ms and 7.6 M; 913 at 2,048, a collapse answered
  SlowDown.
- audit 2026-11-12 §6, as S1b's commit `a93c172a` quotes it: one core a node caps a cell well
  under 10,000 PUTs a second.
