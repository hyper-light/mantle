# 76. The transfer harness: objects made from a seed, checked exactly, on a fresh cell

Status: research note, 2026-10-27. For `mantle bench transfer`, the data-layer plan's step 9
(`crates/mantle/src/transfer/`, `crates/mantle/tests/transfer.rs`). The plan's research
(kiln research 01a16c10075076d481f43fc5eaced5e3) holds the findings this note's decisions rest
on, each with its sources; this note records what the harness's first build took from them and
what it decided where they left a choice. Sources [1] to [4] were read where named.

## Sources

[1] Amazon S3 User Guide, "Understanding and managing Amazon S3 storage classes",
https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html (read 2026-10-21
for the plan, finding f40): "If the size of an object is less than 128 KB, it is not monitored and
not eligible for auto-tiering." and "If an object is less than 128 KB, Amazon S3 charges you for
128 KB."

[2] NIST SP 800-38A, "Recommendation for Block Cipher Modes of Operation", §6.5, the counter
mode: each block of output is the forward cipher of a counter block, so the output at any block
is made from its counter alone, with no output before it.

[3] aws-lc-rs as vendored (vendor/aws-lc-rs, `src/cipher.rs`, `src/cipher/aes.rs`): `EncryptingKey::ctr`
and `less_safe_encrypt` with an `EncryptionContext::Iv128` naming the first counter block, over
AWS-LC's `AES_ctr128_encrypt`.

[4] The plan's findings: f2, the scale the owner agreed (1k/10k/100k small objects, 10 GB and
100 GB objects, both doors, every object byte-identical, every block's chunks on distinct nodes);
f7, sized runs only on the owner's bench machines, never a shared runner; f8, a deleted object's
space returns only a grace after release, three days by default; f33, checks exact, run live, and
a harness bounded, latency in fixed-size histograms and oracles as bitsets; f34, a cell of three
domains stores each block as three copies (`mantle durability --domains 3` picks R3); f56, AES in
counter mode is in the tree through the vendored AWS-LC, so an object's bytes come from a seed and
its name at hardware speed and a read is checked by making them again; f67, no thread or process
per object.

## Decisions

**An object's bytes are AES-256-CTR's keystream.** The run's key is the SHA-256 of its seed under a
key domain; an object's first counter block is the first 16 bytes of the SHA-256 of its name under
a counter domain; its byte at offset `o` is the keystream's at counter block `start + o / 16`, `o
mod 16` into it [2][3]. So any range of any object is made from its offset alone, a get is checked
a piece at a time as its bytes arrive, and nothing of an object is kept anywhere but the cell
(f56). The counter carries as one 128-bit number, the way AWS-LC's counter mode advances it: blocks
made one at a time across 2^32 equal the keystream made in one call across it
(`generate.rs`'s `the_counter_carries_across_its_low_words`), and the first bytes of run 1's object
`o0` are a recorded vector every platform's test makes the same.

**A small set's sizes are S3's small objects, the layout's boundaries first.** Objects under 128
KiB are the ones S3 itself treats as small [1]; a set's first five objects are an empty object, a
byte, and a 64 KiB segment and a byte either side of it, and the rest are drawn below the line from
the run's keystream of sizes, so every set holds the boundaries whatever its draw.

**A case is a fresh cell of three.** Each case makes its own cell with `mantle init` in a
directory of its own under the run's (one an earlier run left is refused), three founders on
loopback in three failure domains so each block's three copies sit on three nodes (f34), each run
as `mantle serve`, a child of the harness, and told where the others listen once each is bound, as
an operator tells a node its peers. A case never reuses a cell: space a delete frees returns only
after the collector's grace (f8).

**Both doors, each object read back through both.** A set's even objects are put through the
native door by a client of the first node, as `mantle cp` puts one; its odd ones through the third
node's S3 listener by boto3, the stock client, in one driver process a case that `mantle init`'s
profile signs for and the harness feeds and reads over its standard input and output (`boto.py`),
so neither side holds more than one small object (f55, f67). Every acknowledged object is then read
back natively by a client whose first node is the second, and through the third node's listener,
so each is read through another node than it was put through and through the door it was not put
through; a read is exact or it counts as wrong, a put not acknowledged counts as refused. The
doors run in turn, each phase alone, so the driver's blocking pipes never stall the native
clients' runtime, and the driver says `ready` before anything is timed, its start-up no put's.

**Large objects go in four ways, two each door.** Object `i` of a large case is put natively in
the runs the node cuts, as `mantle cp` puts a file (gateway.md §2.1); by the AWS CLI from its
standard input, its length stated so the CLI sizes its parts for S3's 10,000 (`--expected-size`);
natively as an explicit multipart upload; or by boto3 as one, for `i` mod 4 in that order. An
explicit upload's parts are the AWS CLI's 8 MiB where 10,000 of them hold the object, else what
10,000 need (f27), each part's bytes made as they are sent and a refused part ending the upload,
aborted. boto3's driver takes a part's bytes only once it said the step before went, so a refusal
stops the harness making the rest. The CLI is a process a copy and says its failures into a file
the harness reads once it exits, S3's code taken from botocore's `An error occurred (CODE)`, so no
pipe it fills can stall a copy; its copy to standard output heads the object first, whose `404`
names no code and is read as no such key. Every acknowledged object is read back natively through
the second node and by the CLI through the third node's listener.

**A case fills the cell or puts a count.** With a count, a large case puts that many objects. With
none it puts until each door has refused one as out of space, typed (node.md §4.4), at most the
objects a founder's volume holds of one and two past them, a door refused once asked no more.
Each object is asked once: a full volume holds a put for its cleaner's verdict, so the cell's first
answer is the typed one (research/75), and a `SlowDown` counts as refused. The refused objects are
then read through both doors,
which must answer no such key, and listed by neither, which must list every acknowledged object.

**Each block's placement is read as the cell's gateway reads it.** Before any founder starts, the
harness issues a gateway's certificate under the cell's authority and pins it in every founder's
configuration (`peer = ID gateway CERTIFICATE`, node.md §3.6), as an operator pins a gateway; it
states the cell's message bounds, which its object client's messages fit, and reads, for every
acknowledged object, its version from the Name range, its file's extents from the File range a
page at a time, a completed upload's part files after it, each block's chunks' volumes from the
Block range, and each volume's node from the root range's volume table. A block two of whose
chunks share a node, or one on a volume the table does not name, is counted (f34). An object's key
and length are made again from its place in the case, never kept.

**Each founder is killed in turn.** Once a case's chunks are accounted, each founder is SIGKILLed,
the harness waits until every running node names a running node as every range's leader, the fact
a read through the others needs, and every acknowledged object is read back natively through
another node; then the founder is started again on its directory, the others told where it
listens now, and every object is read back through it once the root range takes its enlistment.
The first build met reads ended `Stalled` at a block whose copy the killed node held: a node read
a chunk from a peer until the transport judged it gone, two idle timeouts, as long as the client
waits. A node now reads nothing from a peer its detectors suspect (gateway.md §3's as-built note).

**A lost answer is asked again, and counted.** The native client asks a request again of itself
only where no node can have taken it; one whose exchange ended unanswered is its caller's, and
asked again with its identity it is answered from what the first made (gateway.md §2.1). The
harness asks an explicit part, an upload's creation and completion, and a read again so, within the
client's attempts, a part put again replacing the one before and a completion asked again finding
the version it made (05 §4.3, §4.4), and counts each in its row's `again` and its kill line. On
CI's busy runner, two CPUs beside two busy loops, a 20 MiB object's first part waited past the
client's period for its three durable copies, and the case ended there before (CI run
40580884158): a lost answer is a client's to recover, and a cell slower than its client's period,
STATUS item 8's open half, shows in the count rather than ending the run.

**Each case runs its controls.** A check that cannot fail checks nothing, so after its real
checks each case runs three faults through them and says whether each was caught: its first
acknowledged object of a byte or more read back with its middle byte inverted on the way to the
check, which must name that byte; that object's first block judged with one of its chunks moved
onto another's node in the volume table as read, which the placement check must count; and a key
no case puts, which must read as missing. A control missed leaves the case inexact.

**Volumes can be the machine's.** A case's volumes are stated bytes, or `--volume free`: the run's
file system's free space at the case's start, as the OS gives an unprivileged writer
(`mantle_disk::probe`: `statvfs`, `statfs`, `GetDiskFreeSpaceExW`), less the `--reserve` its
operator keeps free, a third to each founder, each case's line saying which. A capacity case so
fills what the machine can give after the cell's three copies (plan step 9).

**Every node's chunk bytes are accounted.** Once a case is done each node is asked its chunk bytes
on its standard input: received from peers over QUIC, put on its own volumes in-process, held by
its volumes, and of the first two kept by none, a volume's refusal or a body that ended before it
was whole. Nothing removes a chunk yet, so the first two's sum is exactly the last two's on every
node, the check the step names (chunk bytes moved equal the chunks held) made exact where a full
volume refuses some: a filled cell of 96 MiB volumes held 48 MiB a node and kept none of 15.9 MiB,
the two refused parts' chunks. A volume keeps `CLEANER_RESERVE` free segments past what a client's
writes take (research/11 §10.3), which is half of so small a volume.

**Bounded by construction.** A case keeps two fixed-size histograms of latency
(`mantle_disk::histogram`, HdrHistogram's buckets) and counts; a node's status lines are read on a
thread of its own, three in all; the native door drives its objects on the client's one task
(f33, f67). The harness writes nowhere but the directory its operator names, and
runs only where its operator runs it: a case fills the volumes it states (f7).

## Open

- The harness's waits end on the line they wait for or on a process's end: a node that hangs
  silent hangs the run. They are to end, too, once the cell has gone quiet for as long as its nodes
  state they may go without moving (each `status` line's patience, reconnect, measuring and asking,
  as the tests' cell takes them), a node's first start, measuring its devices before it says
  anything, included.
- The memory and threads of a case of 100k objects against their stated bounds; the 10 GB and
  100 GB cases on the owner's machines (plan step 10).
