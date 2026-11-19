# Membership: a cell whose machines come and go

Status: design, 2026-11-14 (branch `fleet-cell`). Sources: docs/research/06 (consensus; its §A1.7
quotes Ongaro's dissertation on membership changes and the 2015 single-server bug), 15 (Ford et
al.'s availability study), 64 (starting and stopping a node), 75 (a full cell), 92 (this record's
own sources: the anti-replay window, revocation's bound, and each operating system's statement of
the space a process may write), 98 (coming back after a write-off: re-admission online, an issuer
beneath an offline root, and wiping a returning machine, §5.1, 2026-11-18); and the records this
one joins: node.md
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

and, once for the cell, the **grant windows** (§2, §5.1), the **ledger** of written-off
identities that may come back, and the **`rejoin`** setting (§5.1).

A member's states and what moves it between them:

| From | To | By | Meaning |
|---|---|---|---|
| (none) | `Serving` | `Join`, from the node itself, its grant in the window | a member: it votes, its volumes take chunks |
| `Serving` | `Leaving` | `Leave`, from the node itself | it asked to leave in order: it serves its chunks until repair has moved them, takes no new one, and votes in no range |
| `Serving`, `Leaving` | `Gone` | `Gone`, judged by §5's rule, or stated by an operator, a supervisor or repair | its identity is refused from now on; its chunks are rebuilt elsewhere |
| `Leaving`, `Gone` | `Removed` | `Remove`, once no volume of it is left in the table | its row kept until no range's configuration names it; its identity refused once the root range keeps it as a learner no more (§4 step 5) |
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

1. it is a pinned peer (a gateway, or a founder, which `mantle init` pins in every other
   founder's configuration), byte for byte, as before, unless the root range's rows refuse it as
   they refuse a node pinned nowhere: its member `Gone`, or `Removed` and kept as a learner no
   more, or no member of its id held and its grant spent (delisted);
2. its id is a member the root range holds `Serving` or `Leaving`, or `Removed` while the root
   range's configuration, as this node applied it, keeps it as a learner (§4 step 5), and its
   certificate's SHA-256 is the member's fingerprint; or
3. it names no member and its grant is new by the window: a node granted and not yet joined; or
4. the cell's issuer signed it, its fingerprint is one the root range committed in a `Regrant`,
   and its grant is new by the issuer's window: a node re-granted and not yet joined (§5.1). A
   certificate the issuer signed is a node's alone, and is admitted by no other rule until it has
   joined, when rule 2 admits it as any member.

A `Gone` member, a removed one the root range keeps as a learner no more, a delisted one (its grant
spent, its rows gone) and a certificate that is neither are refused at the handshake, pinned or
not, charged to no identity (hyper-transport `Directory::identify`). A node applies its root
range's entries to its directory at each turn that applied any, and closes the connections of an
identity it refuses from then (`Endpoint::refuse` with `Refusal::Identity`), so a removal reaches
connections already open (node.md §3.6).

*Decided (2026-11-18, the owner): a pinned founder is refused from the entry that writes it off.*
A founder pinned in its peers' configurations was refused only once its rows went at its
delisting, so while its row said `Gone` or `Removed` its peers admitted it again whenever it
dialed. Rule 1 now judges it by its row as rules 2 and 3 judge a node pinned nowhere
(`Peers::set_members`, `crate::directory`), so no written-off identity has a window in which it
still talks to the cell, and a leaving founder learns of its drop as a granted node does (§4 step
5).

*The protocol.* Granting a new machine is the authority holder's alone, offline; re-granting a
machine the cell wrote off is the cell's, online, under an issuer the authority made (§5.1).
`mantle init grant DIR --cell
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
   it and says `left` once its own replica holds its row `Removed` (a row not read yet says
   nothing of its volumes). It stays admitted while the root range keeps it as a learner (§2
   rule 2), and serves on, so the leader hears that its storage holds the commit that removed it:
   a member's answers carry its durable commit (hyper-raft core step R-6), and a leader elected
   meanwhile, whose tracker starts empty, hears it again. The leader then drops it from the root
   range's configuration (a learner dropped sooner would never hear of its own removal), and, once
   no range names it, asks `Delist { node }`: its rows go. Each member refuses its identity once it
   applies the drop, pinned or not (§2 rule 1), and the leaving node, at the first refusal since
   its removal by a member its replica holds serving or leaving, stops in order and exits 0
   (`Departure::settle` in `crate::process`). A link its own handshake refused, to a member gone,
   removed or delisted, says nothing of its own standing and does not stop it. Refused at the
   removal itself, as the members first did, it could not answer: it either never heard its
   removal or was never heard, and the leader kept it as a learner for good.

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
is chosen: the mean is the durability model's own input, the rate the cell states
(`durability.repair_rate`, `mantle init --repair-rate`), else research/15's day until repair
measures its own rate (constants.md: `derive.rs` `REPAIR_HOURS`, an open row), and a measured
repair rate shortens the wait with no change here. A judgment wrong about a member that comes back
costs a grant and a join (§2, §3), and the rebuild the model would have asked anyway.

**Gone, stated.** An operator, a supervisor or repair that knows a machine is gone (kiln
uninstalling it, a disk known dead, a block's margin that cannot wait) states it: a `gone ID` line
on any member's standard input, or repair's own `Gone { node }` (§8), applied at once.

A gone member's identity is refused at once (§2), its votes are removed (§6), and repair rebuilds
its chunks from the others (§8); once its volumes are retired, the root range's leader asks
`Remove`, then, once no range names it, `Delist`. A gone member that comes back finds its identity
refused at every peer (`link PEER ... Identity`); it wipes its disks and comes back under a new
identity the cell issues it (§5.1), or, where the cell's setting or a ban forbids that, its
supervisor grants it anew, and it joins as a new member.

### 5.1 Coming back after a write-off

**Decided (2026-11-18), Ada: automatic.** Asked how a node written off as gone, its data already
rebuilt on other nodes, should come back as a new member, the owner chose that it does so on its
own: it wipes its disks and rejoins. That needs the cell to issue an identity online to a node
that asks. The sources are research/98.

**Decided (2026-11-18), the coordinator, under the owner's rule that a question of robustness is
decided on what is most robust, correct and efficient:**

- *A machine kiln uninstalls or reports lost is banned* (`ban ID`) by kiln's supervisor at that
  moment. Those are the moments a supervisor knows the machine's hands are no longer the cell's;
  the ban writes it off as `gone ID` does and drops its ledger entry, so the machine, should it
  come back, is never re-granted (below, *Hands are judged by a person*).
- *Only a machine the cell gave up on (`Gone`, judged or stated) rejoins on its own; one that left
  in order does not.* Its leave was its supervisor's statement that it goes, so the ledger holds
  no entry for it (below) and its request is refused `Unknown`.
- *The cell's issuer is signed by the cell's own offline authority, `mantle init`'s, not kiln's.*
  A cell then re-grants with nothing outside it, the reason an online issuer kiln runs was not
  taken (below), and kiln's authority, whichever way §10's question goes, signs no cell's issuer.

**Who issues.** **Decided: the cell itself, under an issuer the authority makes once, offline; the
authority's own key stays offline.** The authority signs one certificate, the cell's issuer: a CA
whose pathLenConstraint is zero, so it signs node certificates and no further CA (RFC 5280
§4.2.1.9, research/98 [7]), as a public root's key signs subordinate CAs and not leaves (CA/B
Forum Baseline Requirements §6.1.7, [8]) and as SPIRE's upstream authority mints its server's
signing intermediate ([4]). `mantle init` makes it beside the authority and writes its key into
each founder's directory, as it writes the root key, and `mantle init grant` into each granted
node's. The root range's leader signs with it only for a `Regrant` it proposes (step 3 below), and
what it signs is admitted only once the root range has committed it. Every member keeps the key,
since every member votes in the root range and may lead it (§6). A member's directory already keeps the cell's root key, which opens every object
(encryption.md §2), and the issuer's key is kept as that is; it is worth less to a thief: the
directory takes a certificate the issuer signed only at a fingerprint the root range committed
(rule 4, §2), so a key taken from a disk signs certificates no member admits. A cell made before
the issuer has none until its authority's holder makes one (`mantle init issuer --cell OUT`) and
its members take its key; until then a request is refused `Unissued`.

Not taken: an online issuer kiln runs, which would make coming back kiln's act and leave a cell kiln
does not supervise (a laptop's, the tests') without it; and the authority's key online, which signs
founders', gateways' and clients' certificates as well. The cell's own authority signs the issuer,
as decided above, whichever authority §10's question makes the cell's.

**What the node presents.** Its written-off certificate and, over a TLS 1.3 handshake under
mantle's enrollment protocol (an ALPN of its own, node.md §3.6), proof it holds that certificate's
key: the CertificateVerify signs the handshake's transcript, fresh to that handshake (RFC 8446
§4.4.3, research/98 [6]), so a recorded exchange cannot be replayed. With it, the public key of a
key pair it has just made. This is SPIRE's `x509pop` attestation, a node proving possession of a
provisioned certificate's key and named by that certificate's fingerprint ([4]), and Kubernetes'
renewal by the certificate a node already holds ([1]).

**Why that is safe.**

- *It gives the key no standing it lacked.* Until the write-off, whoever held that key was admitted
  as the member itself, and a member away and not yet judged gone comes back under it with nothing
  asked (§5). The write-off is the cell's judgment about time, made so its durability need not wait
  (§5), not a judgment about whose hands hold the machine. A re-grant gives the key's holder a new
  member with empty disks, less than it held before.
- *Hands are judged by a person.* An operator or supervisor that knows a machine lost, stolen or
  retired for good bans it: `ban ID` on any member's standard input writes the member off, as `gone
  ID` does, and drops its ledger entry (below), so it is never re-granted, SPIRE's ban: "A banned
  attested node is not able to re-attest" ([4]). The cell's setting `rejoin off` re-grants none
  (below). A stolen machine or disk comes back only where the cell's setting allows it and no one
  has banned it.
- *Once.* A written-off identity is re-granted at most once: its ledger entry is spent with the
  first key it certifies, so a copied disk yields one member, not two, and the second claimant is
  refused `Regranted`.
- *Only once nothing on its disks is needed.* The root range re-grants a written-off member only
  once it is `Removed` or delisted, so no volume of it is left in the table (§4 step 5): repair
  forgot each volume because no block names it (§8), so nothing on the returning disks is the last
  copy of anything. Before then the request waits (step 3).

**The ledger.** The root range keeps an entry for each identity it wrote off that may be
re-granted: the old node's id, grant and fingerprint, and once re-granted the new identity's id,
grant and certificate. `Gone`, judged or stated, adds one; a member that left in order adds none,
its leave being its supervisor's statement that it goes; `ban` drops one; the re-granted identity's
`Join` drops its predecessor's. At most `root.max_nodes` entries, the cell's own bound (§9): one
past it evicts the oldest written off, whose node then needs the authority holder's grant, today's
path (§2). The issuer's grants are numbered by the root range itself, in a window of their own
beside the authority's, `root.max_nodes` wide as that one is (§2): the count is the root range's,
committed, so it cannot be lost as a granter's file can (§2's `Stale`), and two issuers numbering
apart never collide.

**The order.** The node's configuration names one certificate and its key at every instant, the
old until step 5's rename and the new after, so no step leaves it with half an identity, and every
step resumes from the node's files after a crash at any point. The wipe follows the cell's answer
rather than preceding the request: only the root range knows that no block names the returning
node's volumes, and a node refused (a ban, the setting) keeps its disks for whoever decides.

1. **It learns it is retired.** A member its replica holds serving or leaving refuses its identity
   at a handshake either side began, from an entry the member applied (§2). But a member's directory
   refuses with the same `access_denied` a node no row names once the grant window is full, and a
   certificate not the one its dial named; and on a link this node dialed, `Identity` is also this
   node's own refusal of the peer's certificate (slice 1). So one refusal is not the cell's word.
   **Decided (2026-11-18): retired only on refusals none of those explains.** Its own replica
   holding it gone is the write-off itself, applied here: retired at once. Else the count begins at
   the first turn of its run at which its own replica holds it serving or leaving, a member, which
   no grant window judges, so no refusal of its join counts, each link's endings taken as they stand
   then (as a leave's end is judged, §4). Once its patience has passed since, two idle timeouts (a
   dial made as the count begins connects or ends within one, RFC 9000 §10.1, and a redial waits at
   most one more, `bounds::backed_off`), it is retired when a member its replica holds serving or
   leaving refused its identity since, that link's last ending a connection that was up closed
   `Identity`, and no such member admits it, none connected to it. A connection up and then closed
   `Identity` is the peer's word on this node's certificate: the transport ends a dial whose server
   this node's own directory or verifier refused before it is up (`Unreachable`), and closes one
   whose certificate the peer refused after (`Closed`, slice 1; `Link::was_up`), so a member's old
   port answered by another member, or by another cell's node, says nothing of this node. A member
   connected is the cell's word that it is not written off, so a certificate refused at one stale
   address does not retire a node another member admits; a link that ended another way, a timeout or
   a bound of the peer's admission, says nothing either way, so a member down does not hold a
   written-off node serving nothing for good; a refusal from a node its replica holds gone, removed
   or not at all says nothing of it. Not taken: a majority of the members it holds. A member dials
   only the members of higher ids (`Node::dials`), and members drop a node they write off from their
   peers (`Node::keep_peers`), so a written-off node hears only from the members above it and those
   that pin it, and a majority would leave about half of them refused and waiting for good. What is
   left: a node no member admits over its patience, refused only by a member that dialed another
   member at this node's address (its port since taken by this node), or by a member's verifier that
   could not take its certificate (one that expired as it ran, a skewed clock), is retired wrongly,
   and its data is the cell's to rebuild once its absence passes the repair time (§5), as for any
   node that stops; and a written-off node above every member it holds, and pinned by none, hears no
   refusal at all, which the rejoin closes (slice 6: a member that reached no member over its
   patience dials each one its replica holds, each once). The node writes a marker, `rejoin`,
   holding its old certificate's fingerprint and where each member it knows listens, at most
   `root.max_nodes` of them, whole and durable (hyper-block's record: a temporary name, the
   platform's full flush, a rename, its directory flushed, and a CRC-32C checked on read, as the
   node keeps its run, node.md §1.6), and says `retired`. The addresses are kept because a node may
   know where a member listens only from what its process was told as it ran (a `learn` line on a
   harness's standard input, a member's row its replica no longer holds): its next start reads
   nothing but its files and its configuration before it asks, so what it is not told again it
   would not know (found 2026-11-19: a node of the tests, told every member's address on its input,
   its root range's store as read before the node opens it, its log not replayed, naming none of
   them, asked no member and ended `rejoin unreachable`).
2. **It stops serving.** Its doors, ranges and chunk service stop; it dials no member under its old
   identity and writes no liveness record. A node that starts with the marker present starts here.
   Until the rejoin is built (slices 3 to 6), the process ends here: the marker kept, it says
   `retired` and ends with a typed reason (`retired: the cell wrote this node off`, the old
   certificate's fingerprint) and a nonzero status, its log and volumes closing as the node is
   dropped; a start that finds the marker says `retired` and ends the same way before anything
   opens, measures, dials or writes.
3. **It asks.** It makes a key pair and writes it (`identity/next.key`) whole and durable before
   sending anything, and keeps it through every retry, since the cell may commit a certificate for
   it before the answer arrives. It dials each member it knows, its seeds, the members its marker
   names and the members its replica holds, each once, at most `root.max_nodes`, each within the
   transport's handshake deadline, under the enrollment protocol, and sends `Regrant { key }`. A member's directory
   admits under that protocol only a certificate whose fingerprint is a ledger entry, and only for
   that request and its answer. The member forwards it to the root range's leader with the
   fingerprint its handshake proved, never one the request names, as `Join` is taken only from the
   node it names (§3 step 2), and the leader proposes `Regrant { old, node, grant, key,
   certificate, at_ns }`: a new id drawn from the OS, the issuer's next grant, and the
   certificate it signed for them. Refused: `Off` (the setting), `Unissued`, `Unknown` (no entry:
   banned, evicted, left in order or never written off), `Regranted` (the entry spent to another
   key), `SameKey` (the old key again), `Full` (the issuer's window or the table). Applied, the
   entry is spent and holds the certificate, and the same key asking again is answered the same
   certificate, so an answer lost to a crash costs nothing. The answer carries the certificate, the
   issuer's certificate for its chain, and the members' addresses the root range holds. A request
   for a member not yet removed waits at the member holding it until the root range applies that
   member's `Remove`, the fact it needs, or until its connection ends.
   One round per start: a round no member answered, a wait its connection ended, or a refusal ends
   the process with a line saying which (`rejoin unreachable`, `rejoin refused REASON`) and a
   nonzero status. Starting it again is its supervisor's, kiln or the operating system's service
   manager, as for any exit of `mantle serve`; a refusal stands until a person or supervisor acts.
4. **It wipes.** It writes the certificate beside its next key (`identity/next.der`), whole and
   durable, then deletes every volume's files, each range's store, its log, and its presence and
   history files, flushing each parent directory (CLAUDE.md §6). It keeps what is the cell's or the
   machine's rather than the member's: its configuration, the cell's keys (the root key's
   generations, the access keys, the issuer's key), its S3 endpoint's certificate, the machine's
   calibration and its devices' profiles. The wipe is deletion, not a cryptographic erase: each
   file is sealed under its own data key, wrapped under the cell's root key (encryption.md §2), so
   no key is a volume's own to forget as Ceph forgets an encrypted OSD's (research/98 [11]), and
   the rebuilt copies elsewhere are under the same file keys. It is for correctness, not secrecy:
   the machine is a member again, trusted with the same keys, and state written under an old
   identity is never presented under a new one (Raft dissertation §3.8, research/98 [12]; etcd and
   CockroachDB likewise never take the removed member back, [9], [10]). A deletion done is done
   again as nothing, so a crash mid-wipe resumes it.
5. **It takes the new identity.** It rewrites its configuration to name the new certificate and
   key and the members' addresses as its seeds, written whole under a temporary name, flushed,
   renamed over the old and its directory flushed: the one step at which its identity changes. It
   then deletes the old certificate and key, and the marker.
6. **It joins as a new member** (§3): its ranges open empty, it dials its seeds, a member admits it
   by rule 4 (§2), and its `Join` spends the issuer's grant. Its volumes are laid out fresh at
   their first start (§3.1) and enlisted as new capacity, which placement fills (§3 step 5).

A start finds where it stopped from its files alone: no marker, a normal start; the marker and no
`next.der`, step 3 again with the kept key; `next.der` with the configuration naming the old
identity, steps 4 and 5; the configuration naming the new one, the rest of step 5, then step 6.

*As built (slice 3, 2026-11-18;* `crates/node/src/rejoin.rs`*).* The configuration names old or new
by the fingerprint of the certificate it names against the marker's, not by how its path is spelled.
An identity is kept in one of two places in `identity/` beside the configuration, `node.der` and
`node.key` or `next.der` and `next.key`: the configuration names one, and the next identity is
written to the other, so a node written off a second time writes its next identity where its first
switch left room, and "`next.der`" above is the place the configuration does not name. A certificate
there that the marker names is the old identity's, not the cell's answer, and the start asks again.
Each of steps 4 and 5 is an act done whole and durable before the next, chosen from the files alone
(`rejoin::act`), and a start runs them under the node directory's and every volume's lock, at most
as many as there are (the inventory's deletions, the switch, the old certificate and key, the
marker): the wipe deletes one file an act, its log, each range's store the configuration names, its
`run`, `stopped` and `live`, and each volume's `mantle/volume` and `mantle/volume.new`; once none is
left it flushes the node's directory and each volume's `mantle/`, so a deletion a crash left
unflushed cannot come back under the new identity, then switches, rewriting the configuration's
`id`, `certificate` and `key` lines and keeping every other (the seeds are slice 6's, with the
answer that carries them); then it deletes the old place's certificate and key, flushes `identity/`,
and removes the marker last. The switch refuses, the configuration untouched, unless the next
certificate names a node and the next key reads, owner-only, as a node's. An identity the
configuration names elsewhere, at a path its operator wrote, is left there: nothing removes a path a
configuration's value names. The ask and the join are stops until slices 4 to 6: at the ask the
start says `retired` and ends `Retired` with the marker's fingerprint, before anything opens; after
the switch it ends `Rejoined` naming the new node, whose next start is a member's. The writer of
files whole and durable moved from the binary to `mantle_node::files`, which `mantle init` and the
rejoin both reach: the node crate keeps its directory's layout, so the binary driving steps 4 and 5
would have needed every name of it.

*As built (slice 4, 2026-11-18;* `mantle_meta::root`*).* The root range keeps the ledger as one
row per written-off identity under its old id (`key::ledger`): its order of write-off, its grant
and fingerprint, whether it is banned, and once re-granted the new identity's id, issuer's grant,
fingerprint, public key and certificate (`record::Ledgered`); beside it the ledger's own count
(`record::WrittenOff`: the entries ever added, which number each one's order, and those held), the
issuer's grant window (`key::issuer`, a `Grants` as the authority's is) and the `rejoin` setting
(`record::Rejoin`, absent read as `automatic`). `Gone` and `Ban` carry the cell's `max_nodes`, the
ledger's bound, as `Join` carries it for the grant window; an entry past it evicts the entries of
lowest order, one read of at most the entries held. A ban keeps its entry, marked banned, rather
than dropping it, so a later request is refused `Banned`, saying why, and not `Unknown`; a ban of a
member serving or leaving writes it off as `Gone` does, its entry banned from the start, and a ban
of a member written off with no entry (evicted, or left in order) changes nothing, as nothing
re-grants it. `Regrant { old, node, grant, fingerprint, key, certificate, max_nodes }` checks, in
order: the setting (`Off`), the entry (`Unknown`), the ban (`Banned`), a re-grant held (the same key
answered `Granted` with the held identity and nothing spent; another key `Regranted`), the old
member's row (`NotRemoved` unless `Removed` or delisted), the new id (`Conflict` where it is 0, the
old id, a node the table holds or another entry's), the issuer's window (`Stale { top }` for a grant
it spent or passed), and the table's room (`Full` where the table, less the old member it still
holds, has `max_nodes` nodes); applied, the issuer's grant is spent and the entry holds the new
identity. The issuer's grant is spent at the re-grant, which numbers it, so two proposals of one
number are one grant and one `Stale`. A running node takes `ban ID` and `rejoin automatic|off` on
standard input as it takes `gone ID`, each asked of the root range in the order stated and said
back with its outcome (`ban 4 Banned`, `rejoin off Kept`), and says the ledger and the setting as
they move: `ledger ID open`, `ledger ID banned`, `ledger ID regranted NEW`, `ledger ID evicted` and
`rejoin automatic|off`. Left to slice 5: `SameKey` and `Unissued`, which need the handshake's old
certificate and the issuer's key; and `Join`'s use of the issuer's window and its drop of its
predecessor's entry, which need the directory's rule 4 to say the issuer signed the joiner.

*As built (slice 5a, 2026-11-18;* `mantle_cert`*, `mantle init`, the node's start and directory).*
The issuer's certificate (`mantle_cert::issuer`) is the authority's signature over a key `mantle
init` draws beside the authority's, named after the authority (`NAME issuer`), never as it: cA with
pathLenConstraint 0 and keyUsage keyCertSign alone, both critical; it signs `mantle_cert::
granted_node`'s profile, `node-ID` and the grant in serialNumber, as the authority does. Its
certificate and key, the key owner-only, are kept whole and durable as `issuer.der` and `issuer.key`
beside the authority's key and in each founder's `identity/` beside the authority's certificate,
where a node's start finds them (`mantle_node::process::ISSUER`); `mantle init grant` copies them
into a granted node's; `mantle init issuer --cell OUT` makes them for a cell made before, once,
and writes them into each node directory in OUT. A start refuses an issuer not of that profile or
not the authority's, and a key its certificate does not certify (`mantle_cert::public_key`, which
the rejoin's switch and its keeping of the cell's answer also check the next key by). The node's
own certificate is verified by `mantle_cert::node_issued`: the authority's directly, or the issuer's
of a node's profile with the issuer under the authority, a path of at most two; one the issuer
signed is presented with the issuer's certificate, and its `Join` says so (`Join::issued`). A join
the issuer signed is judged by the ledger, not by either window: the entry re-granted to its id
under its grant and fingerprint (`Unknown` where none, `Banned` where the old machine was banned
since, `Conflict` for another grant or certificate), whose issuer's grant the re-grant spent; it
drops that entry, and the authority's window is never touched. The directory, told the authority's
certificate (`Peers::authority`), takes a certificate whose issuer is not the authority as the
issuer's: admitted only as the member the root range holds under its fingerprint (rule 2), never
by the authority's window, as a seed or as a client. Left to slice 5b: rule 4 itself (an issuer's
certificate admitted at a committed `Regrant` fingerprint before it joins), the leader's signing,
`SameKey`, `Unissued` and the enrollment protocol.

*As built (slice 5b, 2026-11-18;* `crate::directory`*, `crate::enroll`, `mantle_cert::issuer_name`*).*
Rule 4: the directory, given the ledger and the setting with the members (`Peers::set_members`),
admits a certificate the issuer signed, before its join, at the new id and fingerprint a committed
re-grant names, its entry not banned, as the join judges it (`root`'s `regranted_as`); another
certificate of that id, one no committed re-grant names, and one whose entry was banned since are
refused, the last given back for its connections to close, and once it joins its member's row
admits it (rule 2). The enrollment protocol is an ALPN of its own, `mantle-enroll/1`
(`mantle_protocol::classes::ENROLL`), which every member's server offers beside the transport's:
hyper-transport's `Tls` carries a server's protocols and tells `Directory::identify` the one a
handshake negotiated, so the transport is unchanged. On it the directory admits only a certificate
of a node's profile, on a connection the peer opened, whose fingerprint is the ledger's entry for
the id it names, not banned, the setting `automatic`, as `Role::Enrolling`, which may send none of
a node's, a gateway's or a client's messages (`NodeClasses::class_of`), and keeps the old id with
the fingerprint and key the handshake proved (`Peers::enrolling`): what the member forwards, never
what a request names. An entry banned, evicted or turned `off` withdraws its enrolling node, given
back for its connection to close. A member serves at most as many enrolling nodes at once as the
commands it holds (`command_asks`), since each is answered by a command of the root range it asks;
past them, the one whose connection came up last is refused `Identities` once up, as a client past
the node's bound is, and asks again after. The leader's signing (`enroll::sign`) takes the issuer as
a member keeps it (`enroll::Issuer`: its certificate, its key, and the name it signs as, read from
its subject by `mantle_cert::issuer_name` rather than spelled from the authority's), signs a
certificate of `granted_node`'s profile for the key asked, under the drawn id and grant, valid from
the signing with no expiry (`mantle_cert::NO_EXPIRY`, moved there from `mantle init`), and makes the
`Regrant` naming that certificate's SHA-256; it refuses before signing `Unissued`, where the member
keeps no issuer, and `SameKey`, where the key asked for is the old certificate's (`enroll::judge`).
A ban, the setting and a re-grant are a node's commands alone, as `gone ID` is, never a gateway's
(`node.rs` `names_only_its_sender`): a node relays the first two from its operator, and the root
range's leader proposes the third. Left to slice 5c: the request's kind and wire (`Regrant { key }` and its answer, the certificate,
the issuer's and the members' addresses, each with golden vectors), the member's forwarding to the
root range's leader, the leader's draw of the new id from the OS and of the issuer window's next
grant with one re-grant out at a time, the start keeping the issuer's key for the node (it verifies
it and drops it still), one request a connection and its close after the answer, and the node's
client call `rejoin::ask`.

*As built (slice 5c, 2026-11-18;* `mantle_protocol::enroll`*, `crate::node::enrollments`,
`crate::rejoin::ask`*).* The request is a message of its own kind, `Enroll` (the request class),
sent by an enrolling node and by a member forwarding one's, never by a gateway or a client. A
request is the exchange's body, its head empty, so no head of the node's grows (`node::MAX_HEAD`):
`Regrant { key }` from the written-off node, `Forward { old, fingerprint, old_key, key }` from a
member to the root range's leader; the answer is the reply's body, `Granted` (the certificate, the
issuer's and each member serving or leaving, its id and its QUIC and plane places, as a `seed`
line names them) or `Refused` with its code, each written at its length into one buffer and each
with golden vectors. Beside the refusals of step 3 the answer carries three the request's path
makes: `Conflict` (the id the leader drew is taken), `Unanswered` (the member's patience passed
with no outcome) and `Unled` (a forward reached a node that no longer leads, which the member
forwards again to the leader it knows then; never answered to the written-off node). A member
takes an `Enroll` request only from a node its directory admits as enrolling, as the identity its
handshake proved (`Peers::enrolling`), or a `Forward` from a member not enrolling; holds at most
`command_asks` requests, a second of one node while its first is held refused `Budget` as one past
the bound is; judges it (`enroll::judge`) before the root range is asked; and answers each once,
by the stock patience (`bounds::STOCK_PATIENCE`) at most, `Unanswered` past it. The root range's
leader, one re-grant out at a time, draws the new id from the OS (`getrandom`, as `mantle init`
draws ids), takes the grant after the higher of the issuer window's top as its rows last held it
and its own last re-grant's, signs and proposes the `Regrant`, and answers `Granted` with the
certificate it signed, or the ledger's where the same key was re-granted before, or the root
range's refusal. A start keeps the issuer it verified (`enroll::Issuer`) and hands it to the node.
`rejoin::ask` dials each member it is given in turn, once, under the old certificate with the
enrollment protocol alone, its directory admitting on that protocol only the member it dialed,
by its name as a seed's (`Peers::seed`), sends `Regrant { key }` and reads the answer, each member given the
stock patience from its dial; the first certificate or refusal of the cell's ends the round, and
`Unanswered` or a member not reached passes it to the next; it closes each connection once its
request is answered, the close after one request being the asker's: a member closing first could
discard its answer in flight (RFC 9000 §10.2). Tested: the wire's golden vectors and lengths
(`mantle_protocol::enroll`), the asker taken as its handshake proved it and the root range's
refusals as the wire's (`node::enrollments`), and `rejoin::ask` against a stub member on the
enrollment protocol, answered once with a certificate `mantle_cert::node_issued` takes as the
issuer's, or `SameKey` for the old key (`crates/node/tests/enroll.rs`). Left to slice 6: the
start's call of `rejoin::ask` with its kept next key and the members it knows, the wait at a
member for a `NotRemoved` member's removal, and the real-process tests through a member's own
handler and the root range's leader.

*As built (slice 6, 2026-11-18;* `crate::process`*'s `resume_rejoin`, `crate::rejoin::seed`,
`crate::node::enrollments`*).* A start that finds the marker says `retired` and, under the node
directory's and every volume's lock, takes the kept next key (`rejoin::next_key`) and asks the cell
once (`rejoin::ask`): the members are its `seed` and `peer` lines that name where they listen, then
the members serving or leaving that its replica of the root range holds, read from its store before
anything opens it, each once, never itself, at most `root.max_nodes`. The ask runs on a runtime of
the node's own shape, from its QUIC address on a port the OS chooses, under the message bounds its
ranges state, the process's one statement of them (`classes::state_bounds`), so the start after it
states the same; a `mantle serve` start derives its values first, under the rejoin's locks. A
refusal says `rejoin refused REASON`, no answer `rejoin unreachable`, and the start ends
`Unrejoined`, nonzero, the marker and the key kept. Granted, the answer's members are written as the
configuration's `seed` lines, whole and durable, before its certificate is kept, so a start that
resumes at the wipe switches to a configuration naming them and one that resumes at the ask asks
them; a member's place a seed line cannot hold as one field is refused before the file is touched.
Then the wipe and the switch, said `rejoined ID`, and the same start reads its configuration again
and joins as the new node (step 6): `Rejoined`, slice 3's stop after the switch, is gone. A node
that learns it is retired as it runs still ends `Retired`; its supervisor's next start asks, since
the compute pool a node's start opens is the process's once. At a member, a request the root range
refused `NotRemoved` waits for the rows to hold the old member removed or delisted, then is
proposed again, at most until its patience less two idle timeouts (RFC 9000 §10.1): the asker gives
each member the stock patience from its dial, which connects within one idle timeout before the
request is read, and the answer reaches it within another; past that it is answered `NotRemoved`.
The re-grant still waits for the removal, so the wipe never precedes the fact that nothing on the
node's disks is needed. Tested: the start's ask with a stub answer, each refusal and no answer to
its line and error with the directory as it was but for the key, and `Granted` to the seeds, the
switch and a member's next start (`process`); the wait's bound (`node::enrollments`). End to end,
written for the coordinator to run: the repair suite's stated-gone test and `membership.rs`'s
stated-gone, rejoin, ban, `rejoin off` and copied-disk tests.

**What stays forbidden.** The old identity is never admitted again: its grant stays spent in its
issuer's window and its member written off. A re-grant draws a new id and refuses the old key. The
issuer signs node certificates alone, never a CA, a gateway's or a client's, and the directory
takes its certificates as nodes' alone.

**What an operator can turn off.** The cell's setting `rejoin`, `automatic` or `off`, held in the
root range: stated at `mantle init --rejoin off`, and changed on a running cell by a `rejoin off`
or `rejoin automatic` line on any member's standard input, a root range command as `gone ID` is.
**The default is `automatic`, the owner's choice.** `off` re-grants none, and a written-off node
comes back by the authority holder's grant, as before (§2). `ban ID` withdraws one machine.

**Tests**, for the slices that build it:

- `mantle_meta::root`: each refusal; a judged and a stated `Gone` each add an entry and a `Leave`
  none; `ban` drops one; the ledger past `root.max_nodes` evicts the oldest; the same key asking
  twice is answered the same certificate and a second key refused `Regranted`; a member not yet
  `Removed` waits; the issuer's window spends each grant once; the old grant is refused after.
- `crate::directory`: rule 4 admits the issuer's certificate only at a committed fingerprint, and
  never as a client's or a gateway's; the enrollment protocol admits only a ledger fingerprint, for
  `Regrant` alone.
- `mantle_cert`: the issuer's certificate (cA, pathLenConstraint zero, keyCertSign) and a node's
  under it verify through the transport's verifier to the authority; a CA under the issuer does
  not.
- The node's steps: a directory left at each crash point resumes, from a start, to one identity.
- End to end, real processes: the repair suite's stated-gone test (`crates/mantle/tests/repair.rs`)
  restarts the killed node on its old directory once its member is removed; it says `retired`,
  comes back under a new id with its old volumes' files gone, says `joined`, its new volumes are
  enlisted, and every acknowledged object reads back through both doors. `crates/mantle/tests/
  membership.rs`: a member judged gone comes back the same way; one banned, and one in a cell
  `rejoin off`, is refused and exits with its disks untouched; a copy of a retired node's directory
  started beside it is refused `Regranted`; a node killed after its answer, mid-wipe and after its
  rename comes back once, under one identity. Each wait is on a line a process says.

**Slices**, in order, each about an hour (implement, unit-test, merge):

| # | Slice | Files |
|---|---|---|
| 1 | The transport tells the side a peer's handshake refused that it was refused: the refusing end closes with a code its peer's endpoint maps to `Refusal::Identity` of its own identity | `vendor/hyper-transport/src/endpoint.rs` (the refusal at `refuse_connection` after `Directory::identify`), its tests |
| 2 | The node learns it is retired (step 1) and stops serving (step 2): the marker, `retired`, no more redials or liveness records | `crates/node/src/process.rs` (`Retirement`, beside `Departure`; the start's refusal), `crates/node/src/layout.rs` (the marker, a record of the node's directory, which the node's process writes and so its crate keeps) |
| 3 | The wipe (step 4) and the identity's switch (step 5), as functions over a node directory with each crash point's files, not yet triggered | `crates/node/src/rejoin.rs`, `crates/node/src/files.rs` (from `crates/mantle/src/files.rs`), `crates/node/src/process.rs`, `crates/node/src/layout.rs` |
| 4 | The root range's records: the ledger, the issuer's window, the setting; `Regrant`, `ban`, `rejoin` and each refusal | `crates/meta/src/root.rs`, `crates/node/src/command.rs`, `crates/node/src/process.rs` (the lines) |
| 5 | The online issuer: the issuer's certificate, `mantle init`'s and `mantle init issuer`'s, the leader's signing, rule 4 and the enrollment protocol's admission | `crates/cert/src/lib.rs`, `crates/mantle/src/init.rs`, `crates/node/src/directory.rs`, `crates/node/src/node/members.rs` |
| 6 | The rejoin: steps 3 and 6 joined to the rest, and the end-to-end tests | `crates/node/src/process.rs`, `crates/mantle/tests/repair.rs`, `crates/mantle/tests/membership.rs` |

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
| `mantle init grant DIR --cell OUT --node quic=ADDR,plane=ADDR,domain=NAME,volume=DIR:SIZE (or DIR:spare:KEEP),...` | a new node's directory in DIR: its id and grant, its certificate and its S3 endpoint's, the cell's keys, its configuration with the cell's ranges and seeds, and for a node with no volume of its own `cell.volume`, the most bytes any founder's volumes state (the cell file's `volume_bytes`), which its engines estimate the cell's rows by (node.md §1.4); `granted ID GRANT DIR` |
| `mantle serve CONFIG` | runs a founder or a granted node; a granted node joins at its first start |
| `mantle serve CONFIG --leave` | runs the node until it has left (§4), then exits 0 |
| `leave`, `gone ID` on a running node's standard input | the same leave; an operator's statement that `ID` is gone (§5) |
| `ban ID`, `rejoin off`, `rejoin automatic` on a running node's standard input | `ID` written off and never re-granted; the cell's setting for written-off nodes coming back (§5.1) |
| `mantle init ... --rejoin off`, `mantle init issuer --cell OUT` | a cell that re-grants no written-off node; the cell's issuer for a cell made before it (§5.1) |
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

*Settled (2026-11-16).* Repair's record answers with §5's rule (repair.md §2.1, §7): the root
range's leader asks `Gone` for a member whose recorded absence passes `1 / repair_rate`
(`members::gone_by`, whose unit test is the judgment's), and a place an absence left is kept only
until then. Repair's end-to-end test of a killed node states it gone rather than waiting the
judgment out, since a repair time short enough to wait for in a test judged a running node of a
loaded host gone as well (`crates/mantle/tests/repair.rs`).

## 9. Bounds

Every map here is keyed by an identity the authority or the cell's issuer issued and bounded by
the cell's statement `root.max_nodes`: the standings and members, each grant window's bits, the
ledger of written-off identities and the requests waiting on it (§5.1), each node's peers and
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
- Whether kiln's certificate authority is the cell's authority, or the cell keeps its own (§2). The
  cell's issuer (§5.1) is signed by the cell's own, `mantle init`'s, either way (decided).
- Whether `x86_64-pc-windows-gnu` joins the targets mantle builds and tests for.

## 11. Testing

`crates/mantle/tests/membership.rs` runs real `mantle serve` processes on loopback under a steady
load of writes and reads through both doors, the S3 listener signed with SigV4 and the native
protocol: a cell of one grows to five by grants and joins, then shrinks to two by leaves; nodes are
held still (`SIGSTOP`) and killed (`SIGKILL`) for long stretches and come back; a node joins while
another is away; and after every step every acknowledged object reads back byte for byte through
both doors. Each wait is on a fact a process says (`joined`, `config`, `left`, `standing`).
`mantle_meta::root`'s and `mantle_range::membership`'s tests pin every transition and every
refusal; `crate::directory`'s, each admission rule. §5.1 states the tests of a written-off node
coming back.

## 12. Open

- The voters of a range a subset of the cell, `2f + 1` one to a failure domain (node.md §6.5),
  rather than every member: placement's. A cell of many sleeping laptops keeps quorum by §5's
  demotion only while its members leave one at a time.
- Certificates' rotation, which would let a grant expire and the window shrink to the grants in
  flight.
- A volume that grows and shrinks with its machine's free space, where §3.1 sizes it once.
- Gateways known by grants as nodes now are, rather than pinned.
- On the node directory's file system the stores' bound is still that file system's total less
  the log's file and the volumes there (node.md §1.4); §3.1 leaves the rows of the cell's objects
  room beside a volume of what it spares, but the stores' own bound is not yet taken from it.
- APFS volumes of one container share its free space under different device names, which
  `Device::may_be` tells apart, so two volumes there each stated to take what it spares are not
  refused; the container's identity is the fact to compare.
- A member removed while leaving that stops for good before it says it holds its removal stays a
  learner of the root range, its row kept: the gone rule judges members serving or leaving only.
- Proof of the machine rather than of its files: a key a TPM or a vendor's enclave keeps, the one
  proof that tells a machine from a copy of its disk (research/98 §1), would let `automatic` refuse
  a stolen disk without a ban. It is each platform's own, and the portable path cannot assume it.
- A key of each volume's own beneath the root key would let a written-off volume be erased by
  forgetting it, as Ceph erases an encrypted OSD (research/98 [11]); §5.1's wipe deletes.
- A returning node's old chunks offered to repair before its wipe, where a rebuild is still running
  (Carbonite's reintegration, repair.md §3, which kept places do for a member away and not yet
  gone): §5.1 waits for the rebuild instead.
