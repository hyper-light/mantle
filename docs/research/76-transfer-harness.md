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

**Bounded by construction.** A case keeps two fixed-size histograms of latency
(`mantle_disk::histogram`, HdrHistogram's buckets) and counts; a node's status lines are read on a
thread of its own, three in all; the native door drives its objects on the client's one task
(f33, f67). The harness writes nowhere but the directory its operator names, and runs only where
its operator runs it: a case fills the volumes it states (f7).

## Open

- The AWS CLI streaming a large object from standard input and to standard output, and boto3's
  explicit parts, for the 10 GB and 100 GB cases.
- Each block's placement read from the Block range and the volume table; each node SIGKILLed in turn
  with every object read back through the others, then restarted and read back through it; the
  chunk bytes peers received over QUIC against those they hold.
- Large objects put until the cell refuses one as out of space, and the controls that must fail.
- The memory and threads of a case of 100k objects against their stated bounds.
