# Recovery's corrections lived only in memory

Three defects found by the first runs of `crates/chunk/tests/crash.rs`, all in how recovery
reconciles the index log with the data records after a power loss.

## A deleted chunk came back

**Symptom.** After a crash, a chunk whose delete had been acknowledged was readable again.

**Cause.** Roll-forward scans an open segment from its high-water mark, the end of the last
record the log placed there. Recovery computed that mark from the chunks still in the
index, so a chunk written and then deleted did not count, the scan started before its
record, and re-indexed it.

**Fix.** The mark is advanced by every Put the log replays, whether or not a later delete
removes the chunk.

## A dropped record came back one restart later

**Symptom.** A chunk whose last write never finished flushing was correctly absent after
recovery, then reappeared, unreadable, after one more write and a clean reopen.

**Cause.** Recovery drops the records of the last batch whose data does not verify (the
batch's flush may not have completed, and its index frame can reach the disk before its
data). The drop happened only in memory. The frame naming the record was still in the
log, and once a later batch followed it, it was no longer the last batch and was replayed
unchecked. Records indexed by roll-forward had the mirror problem: nothing in the log named
them, so a later replay would forget them.

**Fix.** When recovery drops or rolls forward anything, it writes a checkpoint of the
recovered index and points the superblock at it before the volume serves requests.

## A crash inside one flush looked like damage

**Symptom.** With concurrent writers, recovery refused some volumes as corrupt after an
ordinary crash.

**Cause.** Recovery calls the log damaged when a valid frame exists beyond the first
missing one (Alagappan et al., FAST 2018, §3.3.3). That is sound only if each frame is
flushed before the next is written. A wrap frame and its batch frame, and all frames of a
checkpoint, share one flush, so a crash can tear an earlier frame while a later one of the
same flush survives.

**Fix.** Each frame records the LSN at which its flush group began. A later valid frame
proves damage only if its group began after the missing frame.

## An empty append made fragment offsets ambiguous

**Symptom.** A volume refused to reopen after a checkpoint: "index log corrupt".

**Cause.** An append of zero bytes that did not seal created a zero-length fragment at the
chunk's end, and the next append created another fragment at the same offset. Fragments
are found by their starting offset; replaying the checkpoint found the wrong one.

**Fix.** Appending nothing without sealing writes nothing; a zero-length fragment can only
end a chunk. The index refuses a fragment after an empty one.

**Regression tests.** `tests/crash.rs` (seeds 3, 54, 10006, 10080 reproduced these) and
`tests/volume.rs::appending_nothing_writes_nothing_and_survives_checkpoints`.
