# Repair, rebalancing and failures of several machines

Status: design and as built, 2026-11-14. Sources: docs/research/90 (replica maintenance under
churn: Carbonite, GFS, Dynamo, Ford et al.), research/04 (R2, R3), research/15 and durability.md
(the model and its repair rate), research/63 (placement order), metadata.md (the Block layer and
its reverse rows), node.md §2.4, §3.8, §5.4 (liveness and the wait rules), architecture.md §6–§7.

The first cell this serves is kiln's own machines: always-on Linux boxes and Mac Studios beside
laptops that sleep or leave for hours, machines added and removed over time. A machine away is the
normal case there, not a disaster, and the cell must still keep every acknowledged byte through a
machine's or a disk's loss without anyone stepping in. This design makes four choices to that end:
it judges machines by the cell's own facts and never by a timeout (§2); it keeps the copy a returning
machine holds instead of throwing it away (§3); it waits for an away machine exactly as long as the
durability model allows and no longer (§2.3); and it places around machines that are away, weighted
by how often each is there (§5).

## 1. What repair keeps

A block is `data` chunks and `parity` chunks, its width, each chunk at an index (metadata.md §1;
durability.md). A chunk is **reachable** when one of its places lies on a volume the cell can read:
its node seen, the volume not written off. A block is **readable** while `data` of its chunks are
reachable, and **whole** while all of its width are. Its **margin** is the chunks reachable past
`data`: zero means one more loss loses it. Repair keeps every block whole where the cell has the
volumes for it, every chunk's bytes verified before they are written anywhere, and every change
of where a chunk lives made by the Block range's compare-and-swap, so a stale or concurrent repair
changes nothing it did not read.

## 2. Facts

### 2.1 Present and away

A node is **seen** or **away** as the node leading the root range judges it, and that node states
it to the root range (`root::Command::Seen`), so every node places and repairs by one record. Its
judgment is the cell's liveness facts and nothing else: the node-pair liveness stream's suspicion of
the peer, an NFD-E detector configured from the pair's measured heartbeats and the cell's history
(node.md §2.4), or the peer's announced stop; and the transport's: a keyed connection is a node
seen, and a connection to it that ended with none since, which QUIC's idle timeout derives from the
path (node.md §3.8 clock 1), is a node away. A node it has no link to, or none yet this run, it says
nothing of. It never declares a node gone for the time it has been away: Carbonite shows a remote
watcher cannot tell a transient failure from a permanent one (research/90 §2, [CAR06 §5]), and
Dynamo makes departure an explicit act for the same reason ([DYN07 §4.8.1]). Membership's rule
beside this record does, at the durability model's own time rather than a chosen one: the same
leader asks a member `Gone` once its recorded absence passes `1/ρ` (membership.md §5;
`members::gone_by`), after its chunks fell due (§2.3), as the merge settled (2026-11-16, §7).

The root range keeps each node's presence (`record::Presence`): whether it is seen, since when in
the range's time, and over its enlisted life the time seen and not seen, its absences and the
longest. Its **measured availability** is the time seen over the whole. A node's enlistment as it
starts says it is seen.

### 2.2 Gone and leaving

A volume is **gone** only by its node's own word. A node's enlistment names every volume it holds
(`root::Enlist`), so a volume the table gave it and it no longer names, its node having started
without it (a disk replaced, a directory lost), is gone at that entry; a gone volume named again is
refused, `Conflict`, and the node stops saying so, since its chunks were rebuilt elsewhere and its
contents are no longer the cell's. A node that **leaves** (`mantle serve`'s `leave` line; `root::
Command::Leave`) names its active volumes leaving: each takes no new chunk, repair moves what it
holds (§4), and once no block names it, repair asks the root range to retire it (`Retire`), and it
is gone. A block keeps a chunk to a volume, so a block as wide as the volumes that stay cannot be
drained: its pass counts it waiting, the leaving volume stays named, and the leave completes once a
volume joins (re-coding such a block narrower is open, §8). Where the volumes that stay are enough
but their domains are not, the moved chunk shares a domain with another of its block's, the domain
rule bent as placement bends it, until a domain joins (§4). A gone volume no block names any more leaves the table (`Forget`), so the volumes a cell
makes over its life take no room past those that still matter (CLAUDE.md §2).

### 2.3 When an away node's chunks fall due

A chunk none of whose places is reachable, at least one of them on an away volume, waits for that
volume's node, and falls due once the node has been away for the time the durability model gives a
lost chunk to be rebuilt in, less the time the cell measured a rebuild to take. durability.md §5
counts detection in the repair rate: a block's scheme is chosen to meet its target with chunks
restored at `ρ = durability.repair_rate` (a day's, research/15 §8.4, unless the operator states the
cell's), detection and rebuild together. So the longest a chunk can wait without the block falling
short of its target is `1/ρ` less the time its rebuild takes, and waiting less spends bandwidth on
margin nobody asked for, as the scrub period is the longest that meets the target (chunk-store.md
§9.1). Carbonite's measured warning, durability falling once a timeout passes the point where it
slows the effective repair rate ([CAR06 §5.5]), is exactly the bound this keeps; a timeout picked
against a downtime distribution is what it replaces. The rebuild's time is measured: the blocks the
away volume holds a place of, as the service's last pass counted them, times the longest one
block's rebuild has taken, over the repairs the service runs at once, so the last of them is in
place by the allowance. Until the service has timed a rebuild it cannot plan a wait, and rebuilds
one block at once to time one, said apart (`timing`); the rest wait for its measure. A chunk whose
every place is gone is rebuilt at once.

A deterministic wait of `1/ρ − T_rebuild` is no weaker than the model's exponential repair of mean
`1/ρ`: for a stripe lost after `k ≥ 2` more failures during a repair, the loss probability grows
with `E[T^k]`, which is least for a constant `T` of that mean (Jensen's inequality), and for `k = 1`
with `E[T]`, equal by construction.

## 3. Keeping what an absence left: kept places

A chunk rebuilt because its own place's node is away is named where it was rebuilt, and the place it
left is **kept** (`block::Command::Repair`): the volume holds the same bytes under the same key,
so once its node is back the chunk is reachable there again. This is Carbonite's reintegration
([CAR06 §5.1]): "Carbonite remembers which replicas were stored on nodes that have failed so that
they can be reused if they return." The bandwidth an absence costs is spent once; the copies
converge to what masks the cell's churn, at most `2r_L/a` copies a block where availability is `a`
([CAR06 §5.3]), and placement by availability (§5) keeps `a` near one for the machines that hold
most. A block's places, own and kept, are each on a distinct volume, each with its reverse row
(`block::places`, `block::kept`), at most the cell's volumes. A kept place on a gone volume is
forgotten (`Unkeep`), and a chunk's own place on a gone volume moves onto a readable kept one, no
bytes copied. Nothing else lets a readable place go unless another place of its chunk is known to
hold it: read, and found equal to the chunk the block's CRC-32C checked, or written there by the
repair. A copy let go unchecked could be its chunk's only sound one where another place's bytes
went wrong in a way their volume cannot see (a faulty core at write time, node.md §5.6); the
deterministic simulation found exactly that twice, a kept copy let go to make room and a leaving
one let go as surplus (§7), before the rule held.

A GET reads a chunk from any of its places (`mantle_gateway::get`), its own first; a copy from any
place of the block.

## 4. Plans and the repairer

`mantle_gateway::repair::plan` judges one block against the facts, each place's volume readable,
leaving, away (due or not) or gone, in this order: a kept place on a gone volume forgotten; a
chunk's gone own place moved onto a readable kept one; nothing more while fewer than `data` chunks
are reachable (the block is unreadable now, or lost for good where every place of a chunk it lacks
is gone); a chunk none of whose places is reachable rebuilt (`Commit::Keep` where its own place is
away, `Commit::Move` where gone), once due; a place on a leaving volume moved (`Commit::Move`); and,
while a domain that takes chunks holds no place of the block, a chunk sharing its domain with
another moved there (`spread`), the domain rule of research/04 R2 ("Hard constraint: at most one
chunk per stripe per rack"), which placement can only bend while too few domains are seen (§5).
A rebuild that may write needs a volume that takes chunks and holds no place of the block; where
every one holds a place already, as in a cell of few machines whose blocks kept the places their
absences left, the plan first makes room: a kept place another place of its chunk holds on a
volume that takes chunks moves onto that other once it is read and found to be the chunk. With no
room to make, the block waits for a volume to join (`Plan::Waiting`), counted with the blocks that
wait for their nodes, and nothing is read for a rebuild that has nowhere to go.

`mantle_gateway::repair::Repairer` rebuilds one chunk as the put and get drivers work, naming each
request and moving on with its answer, doing no I/O:

1. **Sources.** A copy is the whole block: read one readable place whole, its bytes checked against
   the block's CRC-32C, a copy that is not the block never a source. A coded chunk is cut from, or
   for parity coded from (`Code::parity_of`), the block a decode of `data` chunks read whole gives,
   once that block matches its CRC-32C, never taken from the bytes read: a data chunk's padding past
   the block is no part of what the CRC-32C covers, and the simulation found a chunk whose padding
   rotted passing a decode. A decode that fails though every chunk verified at its volume reads
   every readable place and tries every set of `data` copies of distinct chunks, at most
   `C(read, data)` decodes, the first whose block matches the CRC-32C being one with no wrong copy
   however many are wrong; one where no set does is refused, `Corrupt`, nothing written (node.md
   §5.6; research/03 R6.3: "Never propagate an unverified replica").
2. **Checked places.** A move that does not spread reads first the chunk's places on volumes that
   take chunks, other than the one it lets go; one found equal to the chunk takes the place let go
   with no write (`Repaired::Verified`), and a place of the chunk found not equal to it is let go for
   one that is. Otherwise the chunk is written; where no volume takes it, a place found not equal to
   it is let go for another place of the chunk, so its volume can take a rebuild: the simulation
   found a full cell whose only room was a kept place beside a copy that had gone wrong.
3. **Placement.** The node offers volumes that take chunks, none holding a place of the block,
   domains holding none first (§5); a rebuild for the domain rule takes only those.
4. **Write.** The rebuilt chunk to the first volume that stores it, with its CRC-32C, which the
   volume checks before it writes (chunk-store.md §3.1); a refusal sends it to the next.
5. **Compare-and-swap.** `Repair { chunk, from, to, most }` or `Move { chunk, from, to }` at the Block
   range, applied only where the chunk's place is still where the plan read it and `to` holds no
   place of the block; any other answer is `Conflict`, and the block is planned again.

Every step can be repeated: a chunk written again under its key with the same bytes is answered as
stored (chunk-store.md §4), and a compare-and-swap retried after it applied answers `Moved`. A
repairer stopped part way leaves at most a chunk no block names (§8).

## 5. The service

The cell's repair runs on the node whose member leads the Block range (`mantle_node::node::repair`),
as the collector runs beside its range's leader (metadata.md §2): one service a range, which a
change of leader moves, and which keeps nothing outside the Block range, so a service begun on a new
leader starts again from what the range holds.

- **Work.** A pass walks the reverse rows (metadata.md §1, Tectonic's per-disk repair index) of
  every volume gone, leaving, or whose node is away, and every block of the range once the domains
  that take chunks grow or once the service begins, for the domain rule. It begins whenever the
  root or Block range's applied index moved since the last pass began, or an away volume's chunks
  fell due; one that ends with both indexes where they began, nothing done, is **settled**. A
  walk plans and counts each block once, however many of the volumes it walks the block has a
  place on, and a pass whose walk is over waits on its answers without turning the node.
- **Order.** By margin, the blocks with fewest chunks to spare first (research/90 §5: GFS gives
  "higher priority to a chunk that has lost two replicas than to a chunk that has lost only one";
  [FORD10 §5.1]): a pass acts at margin zero, then walks again at the least margin above it that
  it found, in bounded memory: one cursor and a level, never a list of the blocks.
- **Bounds.** A repair holds a chunk from each of its block's places at most, and beside them a
  decode or a parity's coding (`repair::most_bytes`). It takes one of the node's operation slots
  (`gateway.objects`, whose memory the node's division counts, `derive::objects`) for each width of
  its block's places, one slot holding a repair of a block's width for every scheme
  (`derive`'s `an_operation_slot_holds_a_repair_of_a_blocks_width_of_places`); a block holds at most
  as many places as every slot holds, `Repair`'s `most`. Client operations and repairs share the
  slots, so neither holds more than the node has; a block whose repair waits for slots holds the
  walk until they free. The repairs at once begin at one, the least that moves the work, and after
  each pass are as many as would rebuild every block the away volumes hold within the durability
  model's repair time, at the longest a block's rebuild has taken (Little's law, research/11 §4); a
  rebuild timed for that measure that names no chunk times none, and the next pass times another.
  Each turn the service plans one block for each repair its room leaves, so a pass's walk is
  bounded per turn.
- **Retiring.** A settled pass asks the root range to retire each leaving volume and forget each gone
  one that no block names (§2.2).

`mantle serve` says the service's state as it changes: `repair LEADING SETTLED ROOT_APPLIED
BLOCK_APPLIED REBUILT TIMING COMMANDS UNPLACED UNREADABLE CORRUPT WAITING SHORT LOST TAKERS RUNNING`,
where `WAITING`, `SHORT` and `LOST` are the last pass's blocks waiting for an away node, readable by
fewer than their data chunks, and of those lost for good, and `TAKERS` the failure domains that took
chunks as it began. Tests and operators wait on these facts. A joining node's domain takes chunks
once the root range holds its volume active and its node seen, which may commit after the joining
node said it enlisted: a settled pass that saw that domain, `TAKERS` counting it, swept every block
for the domain rule, where a pass merely begun past the root range's last commit a test heard may
predate it.

**Placement** (`mantle_node::place::offer`) offers only volumes that take chunks now, active and their
node seen, so a sleeping machine is passed over rather than tried and timed out, as Dynamo's sloppy
quorum writes to "the first N healthy nodes" ([DYN07 §4.6]); each is keyed by highest random weight
(research/63) with its weight its bytes times its node's measured availability, as CRUSH weights a
device by its capacity (research/04 §A7.1), so a machine often away is given less to serve; one of
each domain first, then the others. A new block's scheme is chosen over the domains that take
chunks now (`host::Ask`): a block written while laptops sleep takes the scheme the machines seen
meet the durability target with, a chunk to each of their domains, and needs nothing when they
wake. Chosen over every domain not written off instead, as first built, a sleeping machine of a
cell as wide as its blocks left too few volumes to place a block on, and every write was refused
until it woke; the E2E test of a sleeping node found exactly that.

*As built (2026-11-20).* A new block's chunks (`place::offer_new`) are offered the volumes that take
chunks, as above, and after them the active volumes of nodes held away, asked only where the first
are too few for the block or refuse it. A put's layout is chosen as it starts and its blocks are
placed as they fill, so a put begun while the cell saw three nodes and placed once the root range
held one away needed three volumes where two took chunks: offered only those, it failed `Unplaced`,
`503 SlowDown`, before any chunk went (CI gates run 44667404838: three end-to-end tests on three
platforms, a cell of three healthy under light load; host test
`a_block_placed_while_one_of_three_nodes_is_held_away_asks_its_volume_last`). A node is held away on
one node's judgment (§2.1), an NFD-E detector's suspicion or a connection ended, and a detector
configured from measurement bounds its mistakes but never rules them out, a stall longer than the
heartbeats' period breaking its model (research/81 [11] §5.1, §8.1.2): a loaded host holds a peer
only slow away, as a GET reads a suspected place last rather than taking it for lost (gateway.md §3,
amended 2026-11-19). The away node's volume is still passed over wherever another takes the chunk,
so nothing waits on a sleeping machine that need not; where none other is left, asking it costs a
refusal at once for a node truly gone, no connection to it (`Refusal::NotConnected`,
`Taken::Refused`), and the block fails `Unplaced` as before, a cause that may pass
(`a_block_whose_node_held_away_is_gone_fails_unplaced_at_once`); a node alive takes its chunk.
Repair's rebuilds (`place::offer`) still take only volumes that take chunks: a rebuild has its
block's other places to wait beside.

## 6. Failures of several machines

- **Metadata.** Each range is a Raft group of `2f + 1` voters (architecture §11); while a quorum of
  a range's voters is away, its commands and reads wait out their budget and are answered `503
  SlowDown` (node.md §5.4), and nothing is repaired: repair's compare-and-swap is a command too.
- **Writes.** A block takes a scheme as wide as the domains that take chunks allow and is placed
  only on their volumes, one to a domain; with fewer volumes seen than its width, as when presence
  changes between its scheme and its placement, it is `Unplaced`, `503 SlowDown`, and with every
  volume offered full, `507 InsufficientStorage` (node.md §4.4). Nothing of a refused object
  commits.
- **Reads.** A GET's reply waits for the range's first bytes: a block too few of whose chunks read
  now is answered `503 SlowDown` before any byte goes (`host::get_status`), one whose chunks read
  and do not rebuild it `500 InternalError`; a later block failing cuts the reply, which its client
  hears as the exchange refused (native) or the connection closed short of its length (HTTP), never
  as an object. No answer is ever bytes the cell does not hold.
- **Too few copies.** A block fewer of whose chunks remain anywhere than its data chunks, every place
  of a chunk it lacks gone, is lost: repair says so (`LOST`), reads of it are refused as above, and
  its rows stay as the record of where it was.

## 7. Tests

- **Unit and property tests** of the Block layer's places (`crates/meta/src/block.rs`: every place,
  own and kept, has one reverse row, its key the chunk's, after any commands) and of the root range's
  facts (`crates/meta/src/root.rs`).
- **Deterministic simulation** of the control logic (`crates/gateway/tests/repair_sim.rs`): schedules
  proptest chooses over cells of three to six domains, some nodes with two volumes, blocks of copies
  and codes; nodes away and back, volumes gone and nodes started with fresh ones, nodes leaving,
  chunks rotted at rest where their volumes cannot see it, padding included, time passing,
  repairers stopped part way and never resumed, two repairers of one block racing, and reads and
  writes failing. After every step no repairer wrote other bytes than the chunk's, every place
  named on a volume not gone holds the chunk, and no command took a chunk's last sound place; once
  the faults stop the service settles every block whose chunks survived whole, nothing kept on a
  gone volume, nothing on a leaving one, the domain rule met, and a block left with fewer sound
  chunks than its data is never planned whole. It found the unchecked place drops of §3 and a
  rebuilt chunk taken from bytes whose rotted padding a decode does not see (§4).
- **Real processes** (`crates/mantle/tests/repair.rs`), cells `mantle init` makes and `mantle serve`
  runs, objects through both doors: a cell of three losing a volume mid-write (SIGKILL, its file
  removed, the node started again) and one at rest (an orderly stop), each rebuilt at once and
  forgotten; a cell of five losing two volumes at once, and then more than a block's code spares,
  saying exactly how many blocks it lost and refusing their objects through both doors; a node
  stopped (SIGSTOP) as long as the cell takes to judge it and plan every block, waited for and
  written around, then woken, its return recorded at every node before reads go through it; a
  node killed and stated gone (membership.md §5): its chunks rebuilt, nothing kept on its volumes,
  which are written off and forgotten, and its identity refused once it starts again; a fifth
  founder joining mid-write and taking new blocks; a node leaving mid-write, drained, retired and
  forgotten, then ending well once refused; and a node leaving a cell of three, its chunks doubled
  onto the two domains left, spread onto a fourth founder that joins, the node that left ending
  well before the join or after. After each, every acknowledged object reads back byte-exact
  through both doors and every node, and every block's places are read from their volumes and
  checked against its CRC-32C. They found the scheme chosen over machines asleep (§5), a block
  counted once for each lost volume it had a place on, and the root range's answers to retiring
  never taken, which kept a leave from completing and the service turning at once forever.

As built (2026-11-15): the simulation passes 2,000 schedules a run, and 20,000 in one run on Linux
x86_64; the six end-to-end tests pass on macOS arm64 and Linux x86_64, every scenario in each of
three runs of the final code but the leaving node's once, stopped on macOS before any repair in the
harness's wait for the cell's links to be judged.

Merged with membership (2026-11-16): a member away past `1/ρ` is judged gone (membership.md §5,
the point its §8 leaves to settle), so a place an absence left is kept only until then. The killed
node's test, which stated a repair in a second and waited for the node's places to count again on
its return, now states the node gone: under six cells at once on one machine a repair time that
short judged a running node gone as well, its absence recorded past it, so it was refused at every
peer and its requests answered `503 SlowDown`; the judgment by time is `members::gone_by`'s unit
test. The leaving nodes' tests wait for their ends, which membership's leave now makes
(membership.md §4 step 5).

## 8. Open

- **Reconciling chunks no block names.** A move off a live volume leaves its old chunk there, and a
  repairer stopped after its write leaves its chunk on its target, as a gateway stopped mid-put does
  (metadata.md §6). Deleting them per volume, as GFS's HeartBeat reports garbage ([GFS03 §4.4.1]),
  needs a fence: a rebuild may name a chunk on a volume after the volume found it unnamed, so a
  reconciliation must only delete a chunk whose writer can no longer name it, the deadline the
  handover sets for puts (metadata.md §2), which repair's compare-and-swap does not yet carry.
- **Capacity rebalancing.** Moves for disk space (GFS's "below-average free space") once a volume
  is at risk of refusing writes, judged by its cleaner's runway (chunk-store.md §8); today a joining
  machine fills only as new blocks choose it.
- **Trimming kept places** past what masks the cell's churn, once space is short.
- **Re-coding a block wider than the cell that remains.** A block as wide as the volumes left after
  a machine leaves for good cannot keep a chunk to a volume; re-coding it into a scheme the
  remaining domains hold is a new block, its file's extents swapped by the File range, a
  compare-and-swap across two ranges. Until then such a leave waits for a volume to join.
- **The rebuild time a new leader measures.** The service keeps its measure of a rebuild's time
  itself (§2.3), so a service begun on a new leader rebuilds one block of an away machine at once to
  time one; kept in the Block range, the measure would cost a leader change nothing.
- **Repair's own priority against client work** beyond sharing the operation slots: the device
  dispatcher's background share (node.md §2.7).

Two items this list held are built, by membership (2026-11-14, merged with repair on 2026-11-15):
a node no founder's directory pins joins under a numbered grant from the cell's authority, admitted
by the root range's rows (`mantle init grant`; membership.md §2, §3), where the join §7's tests
make is still a founder started late; and a volume stated `volume = DIR spare KEEP` takes what its
file system spares, less `KEEP` and what the node writes there besides, sized at its first start
and kept as laid out from then (`derive::volume_sizes`; membership.md §3.1). A volume that grows
and shrinks with its machine's free space after that is membership.md §12's.
