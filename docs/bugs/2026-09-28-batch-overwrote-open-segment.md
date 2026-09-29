# A batch overwrote the records of the batch before it

**Symptom.** Reading a chunk returned `Corrupt { detail: "record identity does not match
the index" }` in every test that wrote more than one batch. No test had yet read a chunk
written in an earlier batch, so the defect was found by the first test that did.

**Cause.** When a batch appended to a segment an earlier batch had left open, the writer
computed the segment's write position while looking for room, but then looked the
position up again in a map that only records positions after a batch's first placement in
the segment. The second lookup fell back to the first record slot after the segment
header, so the batch's records overwrote the acknowledged records at the start of the
segment, and the index pointed two chunks at the same place. The per-record identity
check (docs/design/chunk-store.md §3.1) is what caught it: the bytes at the location did
not name the chunk the index expected.

**Fix.** The position is computed once, stored in the map when the segment is chosen, and
read from there; a missing entry is a refused write, not a fallback. The class is two
derivations of one value from different sources.

**Regression tests.** `tests/volume.rs`: every test that writes more than one batch and
reads back, in particular `concurrent_writers_share_group_commits` and
`reopening_after_many_checkpoints_and_log_wraps_restores_everything`.
