# A reissued segment incarnation brought deleted chunks back

**Symptom.** After every chunk in a segment was deleted, the segment freed, a checkpoint
written and the volume reopened, the next write reused the segment. At the following open,
chunks that had been deleted and acknowledged were back in the index
(`tests/volume.rs`, `deleted_chunks_stay_deleted_when_their_segment_is_reused_after_a_restart`).
No crash was needed: a clean close and two reopens were enough.

**Cause.** Recovery took the next segment incarnation and record sequence from the highest
ones the log still named. A checkpoint records only segments in use, so a free segment's
incarnation left the log with it, and after a restart the counter could fall below numbers
already used. The freed segment was reopened under the incarnation it had before. Its old
records beyond the new write position were still on the device and carried that same
incarnation, so roll-forward, which accepts records of the segment's current incarnation
past the last indexed one, indexed them again. Sequences had the same flaw, and so did
numbers used by a batch whose flush failed, which the log may never name at all.

**Fix.** The superblock records how far sequences and incarnations have been reserved. The
writer raises the reservation, with a superblock write and flush, before any record
carries a number past it, and recovery resumes above the reservation. No number that might
be on the device is issued twice. Incarnations were widened from 32 to 64 bits at the same
time: with small segments on a fast device, 32 bits could run out within months.

**Regression tests.** The test above, which failed before the fix with the segment reopened
under incarnation 2 and deleted chunk 4 read back. `tests/crash.rs` covers power cuts
during the reservation writes.
