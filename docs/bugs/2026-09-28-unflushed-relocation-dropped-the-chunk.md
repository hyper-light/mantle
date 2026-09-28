# An unflushed relocation deleted the chunk it was moving

**Symptom.** With cleaning in the randomized power-loss workload, a chunk whose last write
had been acknowledged was absent after recovery (`tests/crash.rs`, seed 189 with one
writer, 10762 with four).

**Cause.** The cleaner relocates a fragment by writing a copy in its own stream; the index
record for the copy replaces the fragment's old location. Power was cut during a
relocation batch whose index frame reached the disk but whose data did not. Replay applied
the frame, so the index pointed at the new copy; recovery then found the copy unverifiable
in the last batch and removed the fragment, as it does for an unflushed write. For a
relocation that is wrong: the old copy was still intact, because a victim segment is freed
only in a batch after its relocations are durable, and removing the fragment deleted
acknowledged data.

**Fix.** Replay remembers, for each Put in the last batch, the fragment it displaced. When
such a Put's data does not verify, recovery puts the displaced fragment back instead of
removing the chunk's fragment.

**Regression tests.** `tests/crash.rs` with cleaning in the workload; the seeds above
reproduced it before the fix.
