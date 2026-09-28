# Architecture: regions, cells and ranges

Status: design, 2026-09-28. Sources: docs/research/09 (cells, routing, moving data; cited
as "09 §x"), 01 (Tectonic and Meta's storage), 04 (erasure coding and placement), 05 (S3
semantics), 06 (consensus and range-partitioned metadata), 07 (focal's consensus stack).
Parameters named here are sized by models or measurements in docs/research/11; none is
chosen by hand.

Mantle takes two things from two published designs. From AWS it takes cells: a deployment
is divided into complete, independent stacks, so a failure, a bad upgrade or a surge of
requests stays inside one, and growth means adding cells. From Tectonic it takes the
inside of each cell: storage nodes that own their disks, metadata split into layers that
are partitioned and replicated, and clients that write chunks directly (01 §1).

## 1. Three levels

| Level | What it is | Holds |
|---|---|---|
| Region | The S3 endpoint, a set of cells, and the cell map | the cell map (§4), the router (§5) |
| Cell | A complete mantle stack that owns a set of key ranges: gateways, metadata ranges, chunk stores, and the cell's control plane (placement, repair, rebalancing, garbage collection) | Name, File and Block metadata; chunks |
| Range | One Raft group over a contiguous span of one metadata layer | the rows of that span |

"Cell" is used only for the second level. The sources use the word for both a whole stack
(AWS's cells, Windows Azure Storage's stamps) and a single consensus group (Physalia);
mantle's consensus group is a *range* (09 §9.1). Raft groups and erasure-coded stripes
never span cells (09 §9.3).

A laptop is one region with one cell and one node, and a cell map with one entry covering
every key. The same code runs; the router and the cross-cell mover have nothing to do
(09 §9.3).

## 2. Why cells, and why large ones

Tectonic replaced many clusters per datacenter with one, because federated clusters bring
"the operational complexity of bin-packing datasets" and "resource-heavy data copying"
whenever data moves between them [TEC §7, p. 228] (09 §9.2). AWS's cell guidance argues the
other way, for isolating failures and bad deployments, but reports no data (09 §5.3,
non-peer-reviewed); Windows Azure Storage runs stamps in production and pays for them with
account-size caps and migrations at 70% utilization (09 §7.4.1).

The two address different failures, so mantle takes from both:

1. **Cells are large,** closer to a Tectonic cluster than to a 2011 Azure stamp, so moving
   data between cells is a rare event rather than routine balancing.
2. **The cell map and the move between cells exist from the first release,** because a
   cell that fills or fails must be relievable, and AWS's guidance is to build the
   migration mechanism first (09 §9.2 item 2).
3. **Most isolation comes from mechanisms inside a cell that cost no capacity** (§8):
   independent ranges on overlap-limited replica sets, admission control per tenant,
   shuffle-sharded request resources, deterministic commands, and deployment waves.
4. **A cell's size is bounded in the units its control plane pays for** (§3), and a new
   cell is added when a cell reaches its bound or when a tenant must be separated.

## 3. What bounds a cell

A cell's bound is stated in the quantities its control plane holds and refuses beyond
(CLAUDE.md rule 2), as Azure caps a stamp at what its stream manager holds in memory and
Shard Manager caps a partition at what one solver can place (09 §9.3):

- ranges per cell, and Block-layer placement entries (Tectonic records every chunk's
  location explicitly, 01 §1.10);
- the placement driver's solve time at that size, measured;
- the repair budget: a cell must rebuild its largest failure domain within the durability
  target (04 §A5–§A6), so its bound follows from measured repair bandwidth and the
  failure-domain size;
- the largest size at which the whole cell can still be load-tested.

The values come from those measurements; the design records only which quantities bound
a cell.

## 4. The cell map

- **A range map over the ordered Name-layer keyspace** `(bucket, key)`. The usual entry
  covers a whole bucket, so almost every request routes with one lookup; a bucket is split
  across cells only when it outgrows one (09 §9.4).
- **An explicit table with an override table,** not hashing: explicit maps give control of
  hot cells and migrations, and Slicer replaced load-aware consistent hashing after 18
  months (09 §7.1.7, §9.4).
- **Versioned and compare-and-swapped.** Entries are `(range, cell, epoch)`; only the
  control plane changes the map, by compare-and-swap on its version.
- **Stored in a small root range of mantle's own Raft,** never in objects served through
  mantle's S3 path, which would make routing depend on itself (09 §9.4).
- **Distributed with constant work:** routers and gateways receive the full map on a fixed
  loop rather than on change, so control-plane load does not depend on cache state
  (DynamoDB's partition-map cache, 09 §3.5; 09 §5.4). The last copy is persisted locally
  and keeps routing while the control plane is down, including across a restart (09 §9.4).

## 5. Routing

| Hop | How | On a stale route |
|---|---|---|
| Client → cell | Virtual-hosted-style requests resolve the bucket's DNS name to the owning cell's gateways; path-style requests and buckets split across cells go through a thin router that reads the bucket (and key, when needed) against its in-memory map, with no signature checks and no metadata reads. | The cell answers with a redirect naming the owner and the map epoch; the router refreshes and retries once. |
| Gateway → range | Cached range descriptors with a generation and a membership epoch; a floor lookup on the key. | The replica answers with its newer descriptor or a typed stale-route error, never with data. |
| Range → chunks | Explicit Block-layer locations, with an epoch for blocks still being written. | The chunk store refuses appends at a stale epoch. |

**Invariant: stale routing costs liveness, never correctness.** Every owner can refuse:
a replica checks the epoch, a source cell keeps a tombstone for a span it handed off, and a
chunk store checks the block's epoch. Physalia model-checked this property for its routing
cache; DynamoDB's storage nodes enforce it (09 §0 item 5, §9.5).

## 6. How data moves

| What | Protocol | Fencing |
|---|---|---|
| Range leadership | Raft leadership transfer; the old leader forwards or redirects in-flight requests during the handoff | lease sequence |
| Range replica | Add a learner, catch it up, joint consensus, remove the old replica; one at a time | membership configuration in the log |
| Range split | A command in the parent's log; both children stay on the parent's replicas until the directory is updated, and only then may one move. Split at an observed key for size or sustained heat, never for single-key or sequential heat | descriptor generation |
| Range merge | Align replica sets, freeze the right-hand range, require every right-hand replica to acknowledge; always abandonable | descriptor generation |
| Replicas of a block still being written | Overlapping quorum sets under a membership epoch, as Aurora replaces segments without consensus when there is one writer | block epoch |
| Sealed chunk | Copy, verify the checksum, compare-and-swap the Block-layer entry, delete after the metadata grace period | Block-entry compare-and-swap |
| Key range between cells | §6.1 | cell-map epoch |

Sources for each row are in 09 §9.6.

### 6.1 Moving a key range between cells

No source publishes the fencing for this move, so mantle's protocol combines AWS's clone,
flip, redirect and forget phases, Spanner's background copy with an atomic cutover of the
last delta, Azure's clean failover, and Akkio's fenced, resumable mover (09 §9.6):

1. The control plane records the migration: range, source, target, epoch `e`, and a mover
   sequence number that a restarted mover bumps to take over.
2. The target creates non-authoritative ranges for the span.
3. The target copies metadata and chunks into its own placement while the source serves,
   repeating deltas until the remainder is small.
4. The source commits `frozen(e+1)`: it refuses writes to the span with a retryable
   `503 SlowDown` and keeps serving reads.
5. The target copies the last delta.
6. The source commits `handed_off(e+1, target)`: from then on it redirects reads and writes
   for the span, and keeps that tombstone.
7. The control plane compare-and-swaps the map entry to the target at `e+1`; the target
   serves only once it has seen the new entry.
8. The source deletes the span after the grace period.

No write is lost, because the source accepts none after step 4 and the target none before
step 7; no read is stale, because the source serves none after step 6 and between steps 4
and 6 holds the same state as the target. Writes to the span are unavailable from step 4
to step 7, reads from step 6 to step 7; both windows are measured. Every mover has a rate
limit per range, a global rate limit and a stop switch (09 §9.6).

**This protocol is modeled in TLA+ before it is built** (§10), as are splits, merges,
leadership transfer and replica moves.

## 7. Balancing, growing and shrinking

- **Balance with explicit assignments computed from the current one,** under a budget of
  bytes moved per round, ranking moves by imbalance removed per byte moved (Slicer, Shard
  Manager; 09 §9.7). An emergency mode restores lost redundancy under hard constraints; a
  periodic mode improves balance and never worsens it.
- **Move leadership before replicas;** balance heat as well as bytes, using disk time
  (Tectonic's accounting, 01 §1.11); do not chase request locality by default.
- **Rebalance only when some node is at risk,** judged against thresholds mantle measures
  for its own nodes rather than a borrowed percentage (09 §9.7).
- **Grow** by adding nodes to a cell, filled under the churn budget, or by adding a cell
  that new buckets are assigned to.
- **Shrink** through an operation gate that approves a restart, drain or decommission only
  if every Raft group keeps a quorum and every block keeps its tolerated-loss margin,
  counting replicas that have already failed (Shard Manager's TaskController, 09 §9.7). A
  node drains by handing off leadership, refusing new allocations, evacuating through the
  repair path and verifying redundancy. A cell retires by taking no new allocations,
  moving every range out, and draining.
- **Headroom** is sized from the measured capacity of the largest failure domain, so a
  cell survives losing it without scaling up during the failure (09 §9.7).

## 8. Isolation inside a cell

- **Admission control per tenant at the gateway,** with time-limited tokens vended to
  gateways, as DynamoDB moved to after per-partition limits diluted throughput on splits
  (09 §0 item 9); per-range and per-node limits stay as ceilings. Over budget, or while a
  range splits, the answer is `503 SlowDown`, which S3 clients retry with backoff.
- **Shuffle sharding of request resources** (gateways, admission queues): a tenant's shard
  size is its client's retry count plus one, and isolation needs a pool of at least
  16–64 (09 §8.3, derived). Metadata ranges and chunk placement are not shuffle-sharded.
- **Overlap-limited replica sets for metadata ranges:** with `q`-of-`k` quorums, sets that
  share at most `k − q` nodes keep every other set above quorum when one set fails (09
  §8.4.1).
- **Deterministic, versioned commands:** a replica refuses a command version it does not
  understand (Physalia; 09 §9.8).
- **Deployment waves:** a canary cell first, then cell by cell; within a cell, one failure
  domain at a time.

## 9. Control plane and data plane

The data plane is the gateways, range replicas and chunk stores; the control plane is the
cell-map service, each cell's placement driver, repair and rebalancing, and bucket
configuration (09 §9.9).

1. The data plane never waits on the control plane, including at startup.
2. The control plane fails closed; the data plane serves from its last map, and owners
   fence stale requests.
3. Configuration moves by constant work: full snapshots on a loop.
4. Every control-plane action has a rate limit and a stop switch.
5. The control plane keeps its state in mantle's own Raft ranges, in the root range.

## 10. Verified before built

TLA+ (or P) models come first for: range split and merge with descriptor generations and
cached routes; leadership transfer; joint-consensus replica moves (focal-raft already
models its fast track, 07 §1.6); replica changes for blocks being written; the move between
cells, including a range that leaves a cell and returns; and termination of moves and
splits (09 §9.10). The implementations then run under deterministic simulation that can
trigger every split, move and crash on demand, and the end-to-end test moves ranges
between cells under a mixed workload with a linearizability checker (06 §A6.8).

## 11. Decisions

1. **Name-layer partitioning: ordered `(bucket, key)` ranges, split at an observed key for
   size or sustained heat.** S3's `ListObjectsV2` returns keys in UTF-8 binary order with any
   delimiter and `StartAfter` (05 §6.1); an ordered index serves it with one range scan per
   page, where Tectonic-style hashed directories (01 §6.2 option A) need a merge across
   shards for every listing without a `/` delimiter. S3's own general-purpose index is an
   ordered keymap split for heat (09 §6.2, non-peer-reviewed), and Azure's blob index is
   range-partitioned in peer-reviewed production (09 §7.4.10). Tectonic's objection, that
   sequential keys concentrate load (01 §6.2; 06 §A4.5), is answered as S3 answers it:
   `503 SlowDown` while a range splits, and no splits for sequential patterns (09 §0 items 7
   and 9). This supersedes note 01's provisional choice of option A.
2. **Replicas per metadata range: `2f + 1`,** where `f` is the number of simultaneous
   failures the deployment is configured to survive, placed one per failure domain at the
   level the configuration names (disk, node, rack or zone). A laptop has `f = 0`.
3. **Cell-map granularity: range entries from the start,** each usually a whole bucket
   (§4).
4. **Client → cell routing: per-bucket DNS where the client uses virtual-hosted style, a
   thin router otherwise,** and redirects on stale routes (§5).
5. **Cells span zones when the deployment has several,** so a zone's loss is survived
   inside each cell, as AWS's guidance prefers unless a service is zonal (09 §9.11 item 5).

Open until modeled or measured: the cell bound's values (§3); log-only replicas for faster
quorum restoration (DynamoDB, 09 §3.3); the seal length for blocks written with 2-of-3
acknowledgement, where Azure's shortest-replica rule is unsafe (09 §0 item 11); the
shuffle-sharding pool and shard size per deployment (§8); and the family of replica sets
for metadata ranges, and whether it is shared with chunk copysets (09 §8.4.1).
