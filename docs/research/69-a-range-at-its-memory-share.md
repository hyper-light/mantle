# 69. A range at its memory share

What remains of a node's memory past its floor is shared by each volume's index and each range
engine's resident metadata (node.md §1.4; `derive::floor`, `derive::shares`). The owner chose, on
2026-10-23, to size an engine's extents by the metadata measured at the densest rows a range
admits rather than by its statement for any keys: about twenty times the data a GiB holds, the
engine refusing writes once its metadata reaches its share, the statement bounding how far past
it. This note gathers what a range at its share should refuse, what it should still take, and
when it takes writes again.

Lines marked **Reading** are this note's.

## 1. What is measured and what is stated

An engine's branches held at most 89,208 millionths of their extents' bytes at the densest rows
(8-byte keys without values, three fills, `tests/engine_memory.rs`; `shard_db::MEASURED_METADATA_PPM`),
173,494 at the range's longest keys, and their statement for any keys at the floor's pages is
63,698 bytes an extent at 8-byte keys and 87,430 at 2,132, 1.3 and 1.8 times the extent's bytes
(research/68 §2). Sized by the statement, a GiB of memory holds about 0.56 GiB of rows at the
longest keys; sized by the measurement, about eleven at the densest.

**Reading:** a range sized by the measurement can hold more metadata than its share when its rows
are not the densest, so the share is enforced where the metadata grows, not assumed.

## 2. Refuse, do not stall

- RocksDB's write buffer manager, its memory past its limit: "If set true, it will enable stalling
  of all writers when memory usage exceeds buffer_size (soft limit)", and a stalled writer "will
  wait for flush to complete and memory usage to drop down" (RocksDB wiki, Write Buffer Manager).
  A flush gives that memory back.
- A file system with no room refuses the write: `ENOSPC`, "The device containing the file referred
  to by fd has no room for the data" (write(2), Linux man-pages). Removing a file is not refused
  for room: unlink(2) lists no `ENOSPC`, and the space a file used "is made available for reuse".
- WebDAV's 507: "The 507 (Insufficient Storage) status code means the method could not be performed
  on the resource because the server is unable to store the representation needed to successfully
  complete the request. This condition is considered to be temporary." (RFC 4918 §11.5).

**Reading:** a range's metadata is what the rows it holds take, which no wait gives back: a writer
stalled at its share would wait for good. So a range at its share refuses a put, typed, as a file
system at its room refuses a write; it takes a delete, as a full file system takes an unlink,
since deleting is how its rows shrink; and the refusal is temporary: once compactions bring its
metadata back under its share, it takes puts again.

## 3. Where it is checked, and how far past

A put grows a range's metadata only when its memtable is packed into branches, so the share is
checked where a memtable fills and would rotate: a put that finds its memtable full while the
trunk's metadata is at or past its share is refused (`Error::LimitExceeded`, "a range's resident
metadata"), the memtable kept as it is; a delete rotates as before. The check walks the trunk's
branches once a memtable fills, not once a put, and a range refusing puts walks them again only
once its compactions have changed them.

**Reading:** what a range holds past its share when it refuses is at most the packing of one
memtable admitted before the check and what its compactions hold of their inputs and outputs at
once, each within the statement for the extents they hold (`shard_db::resident_per_extent`).

## 4. A replicated range refuses before it proposes

- Raft's replicated state machines: "Each server stores a log containing a series of commands,
  which its state machine executes in order. Each log contains the same commands in the same order,
  so each state machine processes the same sequence of commands. Since the state machines are
  deterministic, each computes the same state and the same sequence of outputs." (Ongaro and
  Ousterhout, "In Search of an Understandable Consensus Algorithm (Extended Version)", 2014, §2.)

**Reading:** a range's metadata at a moment is what its rows and its compactions' progress leave,
and each member compacts on its own schedule, so two members applying the same entry may stand on
either side of their share: a refusal at apply would make one member's state differ from the
other's, which the paragraph above rules out. A replicated range therefore refuses a put where its
leader admits a command, before the entry is proposed, its members applying every entry they
commit; what a member holds past its share is then bounded by the statement for the extents it
holds (§3). Until the leader's admission is built, a node's engines are sized by their shares
(`derive::engine_within`) and are given no limit (node.md §1.4).
