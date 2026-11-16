# Membership: a cell whose machines come and go

Status: design, 2026-11-14 (branch `fleet-cell`). Sources: docs/research/06 (consensus; its §A1.7
quotes Ongaro's dissertation on membership changes and the 2015 single-server bug), 15 (Ford et
al.'s availability study), 64 (starting and stopping a node), 75 (a full cell), 91 (this record's
own sources: the anti-replay window, revocation's bound, and each operating system's statement of
the space a process may write); and the records this one joins: node.md
(§1.4–§1.7, §2.4, §3.6, §6.4–§6.6), architecture.md (§7), metadata.md, replica.md (§6) and
kubernetes.md.

A cell is the machines that store one pool of data: each object's chunks spread across them, and
one Raft-replicated record of what is stored. kiln's machines are to be one cell: always-on Linux
boxes, Mac Studios, a Windows machine, laptops that sleep, machines added and removed over time.
Until now a cell's members were fixed by `mantle init`: each founder pinned every other founder's
certificate in its configuration, and a member lost for good was replaced only by a spare every
directory held from the start (node.md §6.4). This record says how a node joins a running cell, how
it leaves in order, how the cell tells a node away for a while from one gone for good and removes
the latter, and what each step tells the repair service, which moves the data (architecture.md §7;
the `repair` branch builds it). It closes node.md §11's "a node known by its name under the
authority rather than by its pinned certificate".

## 1. What the root range holds of each member

The cell's root range already holds each node's failure domain, its volumes and its history
(node.md §2.4 item 7, §6.6). It now holds each member's **standing**, a row of its own per node
(`mantle_meta::root`, the `Membership` record):

| Field | What it is |
|---|---|
| `state` | `Serving`, `Leaving` or `Gone` (below) |
| `grant` | the number of the grant its certificate carries (§2) |
| `fingerprint` | the SHA-256 of its certificate's DER, what a handshake's certificate is compared to |
| `quic`, `plane` | where its peers dial it, `HOST:PORT` each, as it last stated at its start |

Whether the cell sees a member, and since when, is its presence (repair.md §2.1, the `Presence`
row), the one record of that fact (§8).

and, once for the cell, the **grant window** (§2).

A member's states and what moves it between them:

| From | To | By | Meaning |
|---|---|---|---|
| (none) | `Serving` | `Join`, from the node itself, its grant in the window | a member: it votes, its volumes take chunks |
| `Serving` | `Leaving` | `Leave`, from the node itself | it asked to leave in order: it serves its chunks until repair has moved them, takes no new one, and votes in no range |
| `Serving`, `Leaving` | `Gone` | `Gone`, judged by §5's rule, or stated by an operator, a supervisor or repair | its identity is refused from now on; its chunks are rebuilt elsewhere |
| `Leaving`, `Gone` | `Removed` | `Remove`, once no volume of it is left in the table | its identity refused; its row kept until no range's configuration names it |
| `Removed` | (delisted) | `Delist`, once no range names it | its rows go; its grant stays spent, so it can never join again |

Every transition is a command of the root range, applied the same on every member (node.md §6.6);
a refused one changes nothing and says why. The table holds at most `root.max_nodes` members in any
state, as before (CLAUDE.md §2): a join past it is refused `Full`.

## 2. Identity: a grant from the cell's authority

**Decided: a node's identity is a certificate the cell's authority issues under a numbered grant,
and the root range spends each grant once, in a window as wide as the cell's bound on nodes.**

A node is named, as before, by a 64-bit id drawn from the OS, a key pair it keeps, and a certificate
from the cell's authority naming it `node-ID` (node.md §1.5). The certificate now also carries the
grant's number, `G`, in its subject (X.520's `serialNumber` attribute, a printable string of at
most 64 characters, which every RFC 5280 implementation must take in a subject: §4.1.2.4,
research/92 [2]). The authority numbers its grants in order, keeping the
last number beside its key (`OUT/authority/grants`, written whole and durable before the identity
it numbers leaves the granter), so no two grants of one authority share a number.

*Why numbered.* An identity must stop working once its node is removed, and the certificates do
not expire (node.md §1.5: their notAfter is RFC 5280's "no well-defined expiration" until rotation
is built). A revocation list may drop a certificate only once it has expired (RFC 5280 §3.3,
research/92 [2]), so revoking certificates that never expire would grow a list without bound,
which CLAUDE.md §2 forbids. A grant spent once, in order, needs no list: the root range keeps the
highest grant it has spent and which of the `W` below it are spent, IPsec's anti-replay window
over sequence numbers (RFC 4303 §3.4.3, research/92 [1]). A grant above the window's top is new;
one inside it is new if its bit is clear; one at or below the window's bottom is refused. The
window is `W = root.max_nodes` wide, the most identities the cell holds: a granter issuing grants
for every slot of the cell at once has each still joinable, in any order. State: one number and
`W` bits, bounded by the cell's own statement.

*The directory.* A node admits a connection whose certificate the authority issued (the TLS
handshake verifies the chain, node.md §3.6) as the node it names when (`crate::directory`):

1. it is a pinned peer (a gateway, or a founder of a cell made before grants), byte for byte, as
   before, unless its grant is spent and the root range holds no member of its id: removed;
2. its id is a member the root range holds `Serving` or `Leaving`, and its certificate's SHA-256 is
   the member's fingerprint; or
3. it names no member and its grant is new by the window: a node granted and not yet joined.

A `Gone` member, a removed one (its grant spent, its rows gone) and a certificate that is neither
are refused at the handshake, charged to no identity (hyper-transport `Directory::identify`). A
node applies its root range's entries to its directory at each turn that applied any, and closes
the connections of an identity it refuses from then (`Endpoint::refuse` with `Refusal::Identity`),
so a removal reaches connections already open (node.md §3.6).

*The protocol.* Granting is the authority holder's alone, offline: `mantle init grant DIR --cell
OUT --node ...` (§7) draws the id, numbers the grant, issues the node's certificate and its S3 endpoint's,
and writes the node's directory: its configuration of stated keys, the cell's ranges, the members
it may dial first (each `seed = ID QUIC PLANE`), its identity and keys. The node does the rest
itself: it dials a seed, is admitted by rule 3, and asks the root range to `Join`. Nothing is sent
to the cell at grant time, so a supervisor grants without reaching the cell, and a grant never
used costs one bit. A granter that lost its count (an old copy of its directory restored) issues
a number the window has passed; the join is refused `Stale`, and the granter grants again past the
top the refusal names. `mantle init` numbers its founders' grants 1 to F.

Which authority grants is Ada's question (§10): the operator's `OUT/authority`, or kiln's own
certificate authority. Either works under this design, provided it numbers its node grants in
order and names nodes `node-ID`.

## 3. Joining

A node joins by these steps, each the code's own (`mantle serve` on a granted directory):

1. **It starts with no member.** Each of its ranges opens with an empty configuration: a member
   that votes for no one and campaigns for nothing, and takes its configuration from the leader
   that adds it (node.md §6.4's spare; Raft dissertation §4.1: servers accept entries from a
   leader not in their configuration, research/06 §A1.7).
2. **It asks to join.** It dials each seed whatever their ids (a joiner is the one end that knows
   the other), and asks the root range `Join { node, grant, fingerprint, domain, quic, plane,
   volumes, max_nodes, max_volumes }`, which the leader takes only from the node it names over that
   node's own connection (node.md §6.6). Applied: the grant spent, the member `Serving`, its domain
   and volumes in the table as an enlistment records them (§1). The same join after a restart only
   restates where the member listens: a laptop's address moves. A join from a member `Leaving`
   answers so, and the node resumes its leave; one from a `Gone` or removed member is refused.
3. **The cell learns it.** Every node applies the join in its root range: it adds a peer for the
   new member, dials it where its id is the lower of the pair (node.md §3.2), and its directory
   admits it by rule 2.
4. **Each range adds it as a learner, then a voter.** Each range's leader steps its configuration
   toward the cell's members (§6): the new member as a learner first, which votes on nothing, so
   adding it changes no quorum (Raft dissertation §4.2.1: "new servers join first as non-voting
   members"); once it has confirmed holding everything the leader knows committed
   (`Replica::caught_up`), one change makes it a voter. One server changes at a time, each only
   after the last is applied (dissertation §4.1), and a new leader proposes none until it has
   committed an entry of its own term, the 2015 bug's fix, which hyper-raft's core enforces
   (research/06 §A1.7).
5. **Its volumes take chunks.** Placement offers every `Serving` member's volumes (node.md §6.6);
   new blocks spread over the new member at once. Moving existing blocks onto it is repair's
   rebalancing (§8), not the join's.

The node says `joined` once the root range answered its join (and takes its readiness lock then,
kubernetes.md §5), and `config GROUP VOTERS LEARNERS` each time a range's configuration changes as
its member applies it.

### 3.1 How much disk a joining node gives

**Decided: a volume stated to take what its file system spares takes the space the operating
system states this process may still write there, less what its operator or supervisor states the
machine's other work keeps, less what the node itself will write there.** No fraction is chosen
(`derive::volume_sizes`):

- *The OS's statement* (research/92 [5]–[8]): `statvfs`'s `f_bavail`, "number of free blocks
  available to non-privileged process", times `f_frsize` on Linux, and `statfs`'s same count in
  `f_bsize` units on macOS, which leave out the blocks a file system reserves for the system
  (mke2fs reserves them "to allow root-owned daemons ... to continue to function correctly after
  non-privileged processes are prevented from writing"); `GetDiskFreeSpaceExW`'s
  `FreeBytesAvailableToCaller` on Windows, which honors the caller's disk quota. `mantle_disk::
  probe` reads each (`FileSystem::available_bytes`).
- *The machine's other work*: what its operator or supervisor states, `volume = DIR spare KEEP`
  (`DIR spare` keeps none; `mantle init`'s `volume=DIR:spare:KEEP`). kiln knows what its runners'
  caches take on each machine; a laptop's owner knows what they keep free. mantle cannot know a
  machine's other users' needs, and picks no share for them.
- *The node's own*: the volumes stated on the same file system and not laid out yet; and where it is
  the node directory's file system, the log's file (`log.segment_bytes × log.max_segments`) and the
  ranges' stores, at the most of its store a range's engine took an object
  (`STORE_BYTES_AN_OBJECT`, measured) for each object the cell's volumes make with this one's
  (`cell_blocks`: an object a block, the cell's nodes' fragments over its scheme's width). The
  volume takes the most `f` blocks of `b` bytes for which `f·b + R·c·n·(F + f)/w` fits the space,
  `F` the other volumes' fragments, `R` the ranges, `c` an object's store bytes, `n` the cell's
  nodes and `w` its scheme's width, so the rows of every object the cell's volumes can hold fit
  beside it.
- Two such volumes that may share a file system are refused (`DeriveError::Spares`): dividing one
  file system's spare between them would be a share chosen. One whose file system states nothing
  is refused (`Unspared`), and one short of its keep and the node's own (`Spare`).
- It is measured once, at the volume's first start, and laid out in whole segments (chunk-store.md
  §2): a laid-out volume keeps its layout's bytes (`Geometry::end`), as its profile does. Each is
  said, `basis volume.N.size = BYTES (derived: ...)`, beside its stated `volume.N.keep`.

A `volume` line's words are read from its end, so a directory with a space in it reads; a directory
whose last word is `spare` is stated with a `/` after it.

## 4. Leaving in order

A node leaves by `mantle serve CONFIG --leave` (a node not running), or by a `leave` line on its
standard input (one running), the steps a supervisor runs without a person:

1. It asks the root range `Leave { node }`: `Leaving`. Placement offers its volumes no more.
2. It hands each range it leads to a caught-up voter and waits until that voter leads, as its
   orderly stop does (node.md §1.7; dissertation §4.2.2 prefers a transfer to a leader removing
   itself, research/06 §A1.7).
3. Each range's leader removes it from the configuration (§6). The root range keeps it as a
   learner, so it goes on applying the root range's entries and sees its volumes leave the table.
4. Repair moves every chunk on its volumes to other members and retires each volume (§8). The
   leaving node stays up through this, serving the reads repair copies from.
5. With no volume of it left in the table and no range having it vote, the root range's leader
   asks `Remove { node }`: removed. The leaving node, still a learner of the root range, applies
   it, says `left`, stops in order and exits 0. The leader then drops it from the root range's
   configuration, once the leaving node has said its storage holds the commit that removed it (a
   learner dropped sooner would never hear of its own removal), and, once no range names it, asks
   `Delist { node }`: its rows go.

A leave never completes while its volumes hold a chunk the cell needs: until repair has retired
them, the node waits in step 4, which keeps every acknowledged object as whole as it was.

## 5. Away for a while, or gone

**The facts.** A member's absence is its presence as repair keeps it (repair.md §2.1, §8 here):
the root range's leader states `Seen { node, present }` from the node-pair liveness stream
(node.md §2.4) and the transport, applied at the entry's time (the leader's clock,
`Entry::at_ns`), for every member, those holding no volume as well as each volume's node
(`Node::observe_presence`). A leader is present by leading, and a member's `Join` at each start,
which restates where it listens, says it seen, as an enlistment does. A new leader whose links
judge otherwise than the table states the same, so the record follows the cell's liveness whoever
leads. Silence alone cannot tell a laptop asleep from a
machine dead; what the cell knows is how long the member has been silent, and what its own
durability allows.

**Away** is a `Serving` member whose absence is recorded. Away has two consequences, each priced
by a model and neither by a timeout:

- *Its chunks.* Blocks with a chunk on it are short a chunk while it is away. Repair rebuilds
  them once they fall due, `1/ρ` less the rebuild's measured time after the absence began, keeping
  the away place beside the new one (repair.md §2.3, §3). Placement offers its volumes no new chunk
  (repair's `place::offer`: active volumes whose node is seen), while a new block's scheme is still
  chosen over every volume not written off, away or not (repair.md §5: an absence is no change of
  the cell's shape). A block that scheme cannot place on the members present is refused
  `SlowDown` at once rather than sent to a sleeper or written short of its scheme (CLAUDE.md §6): a
  cell whose scheme needs every member writes again when the sleeper wakes, and one with more
  domains than its scheme's width writes on.
- *Its votes.* A range keeps an absent voter while that costs no tolerance, and drops it while
  dropping every absent voter would let the range survive more failures: a range of `v` voters
  with `a` of them absent survives `(v − a) − (⌊v/2⌋ + 1)` more failures, and `(v − a) − (⌊(v −
  a)/2⌋ + 1)` with the absent ones removed. The absent voters are made learners, one change at a
  time, only where the second is the larger (§6), and stay learners so the node-pair stream keeps
  judging them and sees them come back. Three voters with one away keep it (both survive none);
  five with two away drop them (none, against one); four with one away drop it. A member back is a
  voter again once caught up, as a join's (§3 step 4).

**Gone, judged.** **Decided: an absent member is judged gone once its recorded absence passes the
time the cell's durability model takes a lost chunk to be whole again.** The scheme each block is
stored in is chosen to meet the cell's durability target under a repair rate (node.md §1.4's
`durability.*` row: `durability.repair_rate`, the model's rate of making a lost chunk whole), so the
target holds only while a chunk that cannot be read is whole again within that model's mean,
`1 / repair_rate`. An absence shorter than it costs nothing the model does not already price, and
waiting is cheaper than rebuilding, as most absences end by themselves (research/15 §8.4, Ford et
al.: "The vast majority of such unavailability events are transient and do not result in permanent
data loss"); an absence past it is one the cell's durability can no longer wait out, and the member
is gone: its chunks are rebuilt elsewhere and its identity refused. The root range's leader asks
`Gone { node }` once the leader's clock less the member's recorded start passes the mean. No time
is chosen: the mean is the durability model's own input, today research/15's day until repair is
built and measures itself (constants.md: `derive.rs` `REPAIR_HOURS`, an open row), and a measured
repair rate shortens the wait with no change here. A judgment wrong about a member that comes back
costs a grant and a join (§2, §3), and the rebuild the model would have asked anyway.

**Gone, stated.** An operator, a supervisor or repair that knows a machine is gone (kiln
uninstalling it, a disk known dead, a block's margin that cannot wait) states it: a `gone ID` line
on any member's standard input, or repair's own `Gone { node }` (§8), applied at once.

A gone member's identity is refused at once (§2), its votes are removed (§6), and repair rebuilds
its chunks from the others (§8); once its volumes are retired, the root range's leader asks
`Remove`, then, once no range names it, `Delist`. A gone member that comes back finds its identity
refused at every peer (`link PEER ... Identity`); its supervisor grants it anew, and it joins as a
new member.

## 6. Each range's configuration

Each range's leader steps its configuration toward a target read from its own node's root range
(`mantle_range::membership::Toward`, stateless as `Replacement` is: a step a leader drops, or loses
with its leadership, is asked again by whoever leads next):

- voters: the `Serving` members, less the absent ones where §5's rule drops them;
- learners: in the root range only, the `Leaving` members (§4 step 3);
- nothing else.

One change at a time, only while none is pending (hyper-raft's `has_pending_conf`): a target member
not in the configuration joins as a learner; a learner the target makes a voter is promoted once
caught up; a voter the target makes a learner is demoted (one voter moves: a simple change,
hyper-raft `Configuration::simple`); a member the target names nowhere is removed. Adds come before
removals, so the voters never fall below the smaller of the old and new sets mid-way. A leader the
target drops hands its range to a caught-up target voter first (§4 step 2), and its core hands it on
by itself if it applies a configuration that drops it (hyper-raft `Raft::hand_leadership_on`).

Every range has every member as a voter, as a cell of three has today (node.md §6.6): which nodes
a range's voters are, `2f + 1` one to a failure domain (§6.5), is placement's, not built.

## 7. Commands, and the steps a supervisor runs

| Command | What it does |
|---|---|
| `mantle init OUT --node ... --max-nodes N ...` | as before; its founders' certificates carry grants 1 to F, and its cell file the cell's ranges, bounds and each founder's dial addresses |
| `mantle init grant DIR --cell OUT --node quic=ADDR,plane=ADDR,domain=NAME,volume=DIR:SIZE (or DIR:spare:KEEP),...` | a new node's directory in DIR: its id and grant, its certificate and its S3 endpoint's, the cell's keys, its configuration with the cell's ranges and seeds; `granted ID GRANT DIR` |
| `mantle serve CONFIG` | runs a founder or a granted node; a granted node joins at its first start |
| `mantle serve CONFIG --leave` | runs the node until it has left (§4), then exits 0 |
| `leave`, `gone ID` on a running node's standard input | the same leave; an operator's statement that `ID` is gone (§5) |
| `mantle status DIR` | as before: 0 while a living process of the node holds its readiness lock, taken once it joined |

kiln's installer, on each machine it adds: `mantle init grant` on the machine holding the authority
(kiln's control plane), `mantle pack` the directory, `mantle install` it on the new machine, then
run `mantle serve CONFIG` under kiln's supervision, ready once `mantle status` says so. On each
machine it retires: `mantle serve CONFIG --leave` (after stopping the running one in order), done at
exit 0. For a machine that vanished: nothing; the cell judges it gone (§5), or kiln states it with
`gone ID` to any member.

## 8. The seam with repair

Repair's record (docs/design/repair.md, its §2 and §5) and this one meet at the root range's rows.
Each fact has one owner and one record; where the two were built side by side with a stand-in for
the same fact, their merge (2026-11-15) kept the owner's:

| Fact | Owner | Record and command |
|---|---|---|
| a node present or away, since when | repair.md §2.1 | `Presence`, stated by the root range's leader with `Seen { node, present, at_ns }` from the node-pair liveness stream and the transport, for each volume's node and each member holding none; §5 judges gone by `Presence::since_ns` |
| a member's standing and identity | this record | `Membership`; `Join`, `Leave`, `Gone`, `Remove`, `Delist` |
| a volume active, leaving or gone | repair.md §2.2 | `VolumeStanding`; `Leave` makes the member `Leaving` and each of its active volumes leaving in one entry, `Gone` every volume gone with the member, `Retire { volume }` a drained one gone, `Forget { volume }` a gone one no block names leaves the table |
| where new chunks go | repair.md §5 | `place::offer`: active volumes whose node is seen; the scheme over the volumes not written off |

What each tells the other:

- *Membership to repair.* `Leave { node }` is one command, the member `Leaving` and each of its
  active volumes leaving, so repair drains them while the node serves (§4 step 4). A member judged
  or stated `Gone` (§5) has every volume gone in the same entry: the third way a volume is gone,
  beside its node's own enlistment without it and a drained leaving volume's retirement (repair.md
  §2.2), and repair rebuilds its chunks at once and forgets kept places on it (repair.md §3).
- *Repair to membership.* Nothing but the volume table: once no volume of a `Leaving` or `Gone`
  member is left in it (repair's `Forget`), the root range's leader asks `Remove`, which ends a leave
  (§4 step 5).
- *What neither does.* Membership moves no chunk; repair changes no member's standing and no range's
  configuration. A join moves nothing: what a new member is given is placement's and repair's
  (repair.md §5's domain rule).

**One point to settle between the two records.** repair.md §2.1 never writes a volume off for the
time its node has been away (Carbonite's remote watcher cannot tell a transient failure from a
permanent one); §5 here judges a member gone once its absence passes the durability model's mean
repair time `1 / repair_rate`. The two agree on the data: repair's deadline for an away node's
chunks, `1/ρ` less the rebuild's measured time (repair.md §2.3), falls before §5's judgment, so by
the time a member is judged gone its blocks are whole without it, as kept places, and the judgment
costs no durability. What it costs is what Carbonite's reintegration saves: a member back after the
judgment is granted anew and filled again, its old chunks discarded. What it buys is the cell's
bound: without it a machine that vanished holds its rows, its learner places in every range and its
kept places until a person states it gone, and a table of `root.max_nodes` fills with machines that
will never return. Until repair's record answers, both rules are written down and the merge keeps
§5's judgment with repair's kept places beneath it.

## 9. Bounds

Every map here is keyed by an identity the authority issued and bounded by the cell's statement
`root.max_nodes`: the standings and members, the grant window's bits, each node's peers and
directory entries, its liveness pairs (`Settings::max_peers`) and the plane's keys
(`PlaneLimits::max_peers`), and the transport's identities (`bounds::Stated::peers`), each sized
for every member the cell may hold, not those it holds now. The history of a removed member is added to one row of removed members' totals and its own row goes,
so the cell's history stays the evidence of every run without a row per member ever removed
(node.md §2.4 item 7).

## 10. Embedding

The design works whichever way Ada decides kiln runs mantle. Membership is the node's own: the root
range's commands and the node's turn (`Node::turn`) carry it, and nothing in it assumes a process
of its own. A separate `mantle serve` that kiln supervises is built (the commands above), and needs
no decision. An embedded node would take the same `Node` on the host's runtime and thread budget,
its supervisor's lines (`leave`, `gone`) as calls, and its grant from the host's authority.

The owner's to decide, none of which this design waits on:

- Whether kiln runs mantle inside its own process, on threads kiln grants and kiln's runtime, or
  supervises a separate `mantle serve` (built).
- Whether kiln's certificate authority is the cell's authority, or the cell keeps its own (§2).
- Whether `x86_64-pc-windows-gnu` joins the targets mantle builds and tests for.

## 11. Testing

`crates/mantle/tests/membership.rs` runs real `mantle serve` processes on loopback under a steady
load of writes and reads through both doors, the S3 listener signed with SigV4 and the native
protocol: a cell of one grows to five by grants and joins, then shrinks to two by leaves; nodes are
held still (`SIGSTOP`) and killed (`SIGKILL`) for long stretches and come back; a node joins while
another is away; and after every step every acknowledged object reads back byte for byte through
both doors. Each wait is on a fact a process says (`joined`, `config`, `left`, `standing`).
`mantle_meta::root`'s and `mantle_range::membership`'s tests pin every transition and every
refusal; `crate::directory`'s, each admission rule.

## 12. Open

- The voters of a range a subset of the cell, `2f + 1` one to a failure domain (node.md §6.5),
  rather than every member: placement's. A cell of many sleeping laptops keeps quorum by §5's
  demotion only while its members leave one at a time.
- Certificates' rotation, which would let a grant expire and the window shrink to the grants in
  flight.
- A volume that grows and shrinks with its machine's free space, where §3.1 sizes it once.
- Gateways known by grants as nodes now are, rather than pinned.
- §8's one point to settle with repair: whether a member is judged gone at `1 / repair_rate`.
- On the node directory's file system the stores' bound is still that file system's total less
  the log's file and the volumes there (node.md §1.4); §3.1 leaves the rows of the cell's objects
  room beside a volume of what it spares, but the stores' own bound is not yet taken from it.
- APFS volumes of one container share its free space under different device names, which
  `Device::may_be` tells apart, so two volumes there each stated to take what it spares are not
  refused; the container's identity is the fact to compare.
- A member removed while leaving that stops for good before it says it holds its removal stays a
  learner of the root range, its row kept: the gone rule judges members serving or leaving only.
