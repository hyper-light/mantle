# 90 — Replica maintenance under churn: repair, reintegration and rebalancing

Status: research note, 2026-11-14, for docs/design/repair.md. It gathers what the primary sources
say about four questions a cell of uneven, churning machines raises: when a chunk whose machine is
unreachable should be rebuilt; what happens to the copy that machine still holds when it returns;
how repair is ordered and bounded; and where new copies and moved ones go. Each quotation was read
in the source's own text (the PDFs named below), its section given. Analysis is marked as such.

## 1. Sources

- **[CAR06]** B.-G. Chun, F. Dabek, A. Haeberlen, E. Sit, H. Weatherspoon, M. F. Kaashoek, J.
  Kubiatowicz, R. Morris, "Efficient Replica Maintenance for Distributed Storage Systems", NSDI
  2006, pp. 45–58 (usenix.org/legacy/event/nsdi06/tech/full_papers/chun/chun.pdf). Carbonite.
- **[GFS03]** S. Ghemawat, H. Gobioff, S.-T. Leung, "The Google File System", SOSP 2003
  (research.google.com/archive/gfs-sosp2003.pdf).
- **[DYN07]** G. DeCandia et al., "Dynamo: Amazon's Highly Available Key-value Store", SOSP 2007,
  pp. 205–220 (allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf).
- **[FORD10]** D. Ford et al., "Availability in Globally Distributed Storage Systems", OSDI 2010;
  its findings are research/04 §A5's, quoted there.
- research/04 (R2, R3), research/15 and docs/design/durability.md (the model and its repair rate),
  research/63 (highest random weight placement).

## 2. Transient failures cannot be told from permanent ones remotely

[CAR06 §1]: "a system cannot always distinguish between transient failures and permanent disk
failures: it may waste network bandwidth by creating new copies during transient failures. Third,
after recovery from transient failures, some replicas may be on nodes that the replica lookup
algorithm does not query and are thus effectively lost."

[CAR06 §5]: "we do not want to make new copies in response to transient failures, but it is
impossible to distinguish between disk failures and transient failures using only remote network
measurements."

[CAR06 Table 1], PlanetLab, March 2005 to February 2006, 632 hosts: 21,255 transient failures
against 219 disk failures, a hundred to one; transient downtime median 1,208 s, mean 104,647 s,
90th percentile 14,242 s.

[FORD10 §1], research/04 §A5: "less than 10% of events last longer than 15 minutes ... GFS
typically waits 15 minutes before commencing recovery of data on unavailable nodes", and among its
recommendations (§10) "a dynamic delay before initiating recoveries, based on failure
classification and recent history of failures in the cell".

[DYN07 §4.8.1]: "In Amazon's environment node outages (due to failures and maintenance tasks) are
often transient but may last for extended intervals. A node outage rarely signifies a permanent
departure and therefore should not result in rebalancing of the partition assignment or repair of
the unreachable replicas. ... For these reasons, it was deemed appropriate to use an explicit
mechanism to initiate the addition and removal of nodes from a Dynamo ring."

**Finding 1.** No source tells away from gone by watching a machine: Carbonite says it cannot be
done remotely, GFS and Dynamo answer with a delay or with an explicit statement of departure. A
machine's departure for good is a fact only its operator, or the machine itself, can state.

## 3. Reintegration: keep the copies an absence left, and the absence costs nothing more

[CAR06 §5]: "The key technique needed to achieve this is to ensure that the system reintegrates
object replicas stored on nodes after transient failures; this means the system must be able to
track more than r_L replicas of each object. The number of replicas that the system must remember
turns out to be dependent on a, the average fraction of time that a node is available. However, we
show that the correct number of extra replicas can be determined without estimating a by tracking
the location of all replicas, including those that are offline."

[CAR06 §5.1]: "Because it is not possible to distinguish between transient and disk failures
remotely, Carbonite simply responds to any detected failure by creating a new replica. ... If fewer
than r_L replicas are detected as available, the algorithm creates enough new replicas to return
the replication level to r_L. However, Carbonite remembers which replicas were stored on nodes that
have failed so that they can be reused if they return. ... Over time, it is increasingly unlikely
that the system will need to make any more replicas."

[CAR06 §5.3]: the probability a repair is needed with r copies extant is
`Pr[R < r_L | r extant copies] = Σ_{i=0}^{r_L−1} C(r,i) a^i (1−a)^{r−i}`, and "after the system has
created 2r_L/a replicas, the probability of a new object creation is exponentially small."

[CAR06 §5.4]: "Carbonite is simplified by the fact that it does not need to measure or estimate a
to create the 'correct' number of replicas."

[CAR06 abstract]: "Carbonite is able to keep all data durable and uses 44% more network traffic than
a hypothetical system that only responds to permanent failures. In comparison, Total Recall and
DHash require almost a factor of two more network traffic than this hypothetical system."

**Finding 2.** A rebuilt chunk's old place, kept and counted again when its machine returns, makes
the bandwidth spent on an absence a one-time cost: the copies converge to what masks the cell's
churn. The cost is storage, up to `2r_L/a` copies a block where availability is `a`.

## 4. Timeouts, where reintegration already masks the absences

[CAR06 §5.5]: "A common approach to reduce transient costs is to use long timeouts ... Longer
timeouts reduce the number of misclassified transient failures and thus the number of repairs. On
the other hand, a longer timeout also increases the latency between failure and repair in the event
of a true disk failure; if additional permanent failures occur during this larger 'window of
vulnerability,' data may be lost." And: "Timeouts are most effective when a significant percentage
of the transient failures can be ignored, which is dependent on the downtime distribution. However,
for durability to remain high, the expected node lifetime needs to be significantly greater than
the timeout." Figure 10: "as the length of the timeout increases past the average downtime, we
observe a reduction in the number of bytes sent without a decrease in durability. However, as the
timeout grows longer, durability begins to fall: the long timeout delays the point at which the
system can begin repair, reducing the effective repair rate."

**Finding 3 [Analysis].** The wait before a repair is not free to choose: it is part of the repair
time the durability model already takes. durability.md §5 counts detection in the repair rate
("one over the mean time to detect a loss plus the time to rebuild one chunk"), and a block's scheme
is chosen to meet its target at that rate. So the longest a chunk can wait for its machine, without
the block falling short of its target, is that repair time less the time the rebuild itself takes.
A wait so derived is the model's own number, not a timeout picked against a downtime distribution;
Carbonite's warning, durability falling once the wait passes the point where the effective repair
rate drops, is exactly the bound it keeps. Laptops asleep for a night and machines rebooted for an
upgrade, which [FORD10] finds dominate unavailability, come back within it and cost no repair at
all; a machine gone for good is rebuilt within the time the model allowed. Where repair waits past
that time for some absence, reintegration (Finding 2) keeps the second absence of the same machine
free.

## 5. Repair order and bounds

[GFS03 §4.3]: "The master re-replicates a chunk as soon as the number of available replicas falls
below a user-specified goal. ... Each chunk that needs to be re-replicated is prioritized based on
several factors. One is how far it is from its replication goal. For example, we give higher
priority to a chunk that has lost two replicas than to a chunk that has lost only one. ... Finally,
to minimize the impact of failures on running applications, we boost the priority of any chunk that
is blocking client progress."

[GFS03 §4.3]: "To keep cloning traffic from overwhelming client traffic, the master limits the
numbers of active clone operations both for the cluster and for each chunkserver. Additionally,
each chunkserver limits the amount of bandwidth it spends on each clone operation by throttling its
read requests to the source chunkserver."

[FORD10 §5.1], research/04 §A5: "These queues prioritize reconstruction of stripes which have lost
the most chunks", and "there is an explicit design tradeoff in the use of bandwidth for recovery
operations versus serving client read/write requests."

**Finding 4.** Repair goes by remaining margin, fewest chunks to spare first, and runs within a
bound on concurrent rebuilds that leaves client work its share. [Analysis] With repair's time bound
by the durability model (Finding 3), the concurrency a cell needs is the least that rebuilds what an
absence leaves within that time, from the time a rebuild is measured to take (Little's law,
research/11 §4); more spends client bandwidth on margin nobody asked for, as chunk-store.md §9.1
argues for the scrub period.

## 6. Rebalancing and departure

[GFS03 §4.3]: "Finally, the master rebalances replicas periodically: it examines the current replica
distribution and moves replicas for better disk space and load balancing. Also through this
process, the master gradually fills up a new chunkserver rather than instantly swamps it with new
chunks and the heavy write traffic that comes with them. ... the master must also choose which
existing replica to remove. In general, it prefers to remove those on chunkservers with
below-average free space so as to equalize disk space usage."

[GFS03 §4.2]: "We must also spread chunk replicas across racks. This ensures that some replicas of
a chunk will survive and remain available even if an entire rack is damaged or offline".

[DYN07 §4.6], hinted handoff: "all read and write operations are performed on the first N healthy
nodes from the preference list, which may not always be the first N nodes encountered while walking
the consistent hashing ring."

[GFS03 §4.4.1]: "In a HeartBeat message regularly exchanged with the master, each chunkserver
reports a subset of the chunks it has, and the master replies with the identity of all chunks that
are no longer present in the master's metadata. The chunkserver is free to delete its replicas of
such chunks."

**Finding 5.** New writes go to the machines reachable now (Dynamo's healthy nodes, GFS's below-
average utilization), a joining machine fills as new blocks choose it rather than by bulk moves, and
existing copies move only to restore what placement promises: the failure-domain spread, and a
departing machine's contents. A chunk a machine holds that no metadata names any more is garbage its
machine may drop once told so.

## 7. What mantle takes [Analysis]

1. **Facts, not timeouts, for presence and departure.** A machine is away while the cell's failure
   detectors and its connection say so (node.md §2.4, §3.8), and gone only by a statement: its node
   started without the volume, or it left. The record lives in the root range, stated by the node
   that leads it, so every node places and repairs by the same facts. Findings 1 and 3.
2. **Reintegration.** A chunk rebuilt for an away machine keeps that machine's place beside its new
   one, counted again on return. Finding 2.
3. **The wait the durability model gives.** An away machine's chunks fall due at the model's repair
   time less the measured rebuild time. Finding 3.
4. **Margin order, bounded concurrency.** Finding 4.
5. **Placement around the absent, weighted by measured availability.** Only volumes whose node is
   seen take new chunks; each is weighted by its bytes times its node's measured availability, so a
   machine often away is given less to serve. Findings 5 and 2 (fewer copies on machines whose `a`
   is low keeps `2r_L/a` near `r_L`).
6. **Moves only to restore placement's promises**: the domain rule, and a leaving machine drained.
   Finding 5.
7. **Left**: chunk reconciliation per volume (GFS's heartbeat garbage report), which needs a fence
   against a rebuild naming a chunk the reconciliation deletes (docs/design/repair.md §8).
