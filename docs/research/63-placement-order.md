# 63. The order placement offers a block's volumes

Status: research note, 2026-10-21. For node.md §6.6 and §7: the order of volumes a node offers the
gateway for one block (`Place`), before credits exist to choose among candidates by two random
choices (research 04 §A8, research 27 D11a). The decision this note supports is in node.md §6.6.

## The question

Research 04 settled where placement is recorded: explicitly, in the Block layer, as Tectonic does,
never recomputed from a function as CRUSH does (04 §A7; Tectonic §3.3, §3.5). It also settled the
rules a placement keeps: at most one chunk of a block to a failure domain (04 §0, Ford §5.2), and,
among feasible candidates, two random choices on load (04 §A8; Mitzenmacher 2001), never the
global least-loaded from stale statistics (Mitzenmacher 2000). What it leaves open is the order
in which a node offers the volumes of the cell before any load signal exists: every node of a
three-node cell holds the volume table (node.md §6.6), no node yet advertises credits, and the
gateway's driver writes chunk `i` to the `i`-th volume offered and takes the next one past the
block's width when a volume refuses (gateway.md §2; `mantle_gateway::put::Request::Place`).

That order must be a function of the block and the table alone: no load to draw on, and no state
a node would have to agree on with others. It must spread blocks over volumes in proportion to
what each volume holds, since volumes differ in size, and move as few blocks' choices as possible
when the table gains or loses a volume, so a node whose table is a little stale offers nearly the
same order as one whose table is current.

## Sources

### Thaler and Ravishankar, highest random weight (IEEE/ACM ToN 1998)

D. G. Thaler and C. V. Ravishankar, "Using Name-Based Mappings to Increase Hit Rates,"
*IEEE/ACM Transactions on Networking* 6(1):1–14, February 1998. Read from the authors' PDF
(`cs.ucr.edu/~ravi/Papers/Jrnl/HRW98.pdf`), pages 1–7.

- §IV-A defines highest random weight (HRW) mapping: "An object name and server address together
  are used to assign a random 'weight' to each server. The servers are then ordered by weight, and
  a request is sent to the active server with the highest weight." Its output is an ordered list:
  "the output of such a hash function must be an ordered list of servers rather than a single
  server name" (§IV), so that a server down sends the request to the next.
- §II-B, Goal 4 (Minimal Disruption), and Theorem 1: for every mapping that divides objects evenly
  among `m` servers, the fraction remapped when a server comes up or goes down is at least `1/m`.
  §V-D: HRW achieves it: when a server goes down "all objects which mapped to that server must be
  reassigned. All other objects will be unaffected," and when one is added, only those objects for
  which it has the highest weight move.
- §V-B, Theorems 4 and 5: with objects partitioned by HRW, each server's share of the requests
  balances as the number of objects grows (the coefficient of variation of a server's load goes to
  zero).
- §IV-A on unequal capacity: weights "may be scaled by a constant distributed to clients along with
  the server's address." The paper assumes equal capacity thereafter; a constant scale of a uniform
  weight does not give shares proportional to the constant, which the next two sources do.

### Efraimidis and Spirakis, weighted sampling by keys (IPL 2006)

P. S. Efraimidis and P. G. Spirakis, "Weighted random sampling with a reservoir," *Information
Processing Letters* 97(5):181–185, 2006. Its algorithm and lemma as P. S. Efraimidis restates them,
"Weighted Random Sampling over Data Streams" (arXiv:1012.0256v2, 2015), §2–§3.2, read from
arXiv's PDF:

- Definition 3, WRS-N-W: a sample without replacement in which, "in each round, the probability
  of every unselected item to be selected in that round is proportional to the relative item
  weight with respect to the weights of all unselected items."
- §3.2, algorithm A-ES: "each item `v_i` of the population V independently generates a uniform
  random number `u_i ∈ (0,1)` and calculates a key `k_i = u_i^(1/w_i)`. The items that possess the
  `m` largest keys form a weighted random sample," a WRS-N-W sample. Remark 1: for independent
  uniform `U1, U2` and `X_i = U_i^(1/w_i)`, `P[X1 ≤ X2] = w2 / (w1 + w2)`.

Ordering every item by its key, largest first, therefore gives an order in which each position is
a draw, proportional to weight, among the items not yet placed. Since `ln` is increasing, the order
by `ln(u_i) / w_i` is the same as the order by `u_i^(1/w_i)`.

### Ceph's straw2 bucket (the Linux kernel's CRUSH mapper)

`net/ceph/crush/mapper.c`, `bucket_straw2_choose`, in the Linux kernel (read from
`android.googlesource.com/kernel/common`, commit 445019bbca5d8): for each item with a nonzero
weight it hashes the input, the item and the replica rank to `u` in 16 bits, takes `ln` of it from
a table ("[0,0xffff] (corresponding to real numbers [1/0x10000, 1]" mapped to "[-11.090355,0]"),
and "divide[s] by 16.16 fixed-point weight. note that the ln value is negative, so a larger weight
means a larger (less negative) value for draw"; the item with the highest draw wins, and an item of
weight zero never does. This is HRW with Efraimidis and Spirakis's key, `ln(u)/w`, as Ceph runs it
to choose among devices in proportion to their weights.

## What follows for mantle

1. **Order by a weighted HRW key.** For block `b` and volume `v` of `bytes` bytes, draw `u` uniform
   in `(0, 1]` from a hash of `(b, v)` and key the volume `ln(u) / bytes`; offer volumes by key,
   largest first. By Remark 1 each position is a capacity-weighted draw among the volumes not yet
   offered (WRS-N-W), so volumes fill in proportion to their size; by HRW's minimal disruption, a
   volume added or removed changes the order of only the blocks whose order it enters or leaves at
   its place, and a node with a slightly stale table offers nearly the same order.
2. **Domains first.** Take, in that order, the first volume of each failure domain not yet used,
   then the rest in the same order. The first volume of each domain in key order is the domain's
   largest key, and by the same race (the largest of a domain's keys is a key of weight the
   domain's total), the order of domains is itself a capacity-weighted sample without replacement:
   a domain is offered first with probability its share of the cell's bytes. A block narrower than
   the cell's domains thus spreads its chunks over distinct domains; one wider takes the domains
   there are, then distinct volumes of domains already used, as node.md §1.5 allows a cell short of
   domains.
3. **The hash.** SplitMix64's output function (Steele, Lea and Flood, OOPSLA 2014, already the
   workspace's in `mantle_disk::measure::SplitMix64`) chained over the block's and the volume's
   64-bit halves gives `u`'s 53 bits. A uniform hash is what HRW's and A-ES's analyses assume of
   their weights; nothing here needs a cryptographic one, since the order is advisory: the Block
   layer records where each chunk went and refuses two on one volume (metadata.md §1), and a volume
   refuses a write it cannot take (chunk-store.md §4).
4. **What waits.** Choosing between candidates by two random choices on advertised credits (research
   04 §A8, research 27 D11a) replaces the first position's draw once nodes advertise credits;
   copysets (Cidon et al. 2013; research 04 §A6) constrain the candidates once a cell has enough
   domains to make them matter. Neither changes the table, the record, or the refusal rules.

## Tests this asks for

Exact, not statistical (no level decides a test): the order offers every volume once; offers one
volume of each domain before a second of any; is the same for the same block and table; puts a
volume of no bytes last (its key is taken as the lowest, never `ln(u)/0`); and, for a table that
loses a volume, ranks every other volume by key in the same relative order for every block (HRW's
minimal disruption, which holds exactly because each volume's key depends on the block and that
volume alone; the domains-first pass after it may then promote the lost volume's domain's next
volume, as it must).
