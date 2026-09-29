# Resolving the 2026-09-29 audit

Each finding of [the audit](2026-09-29_audit.md), how it was reproduced, what fixed its cause,
and the test that now fails if the cause returns. A finding is closed only by a reproduction
that failed before the fix and passes after it, and by a check that the test fails when the
fix is removed.

## Standards defects

| Finding | State | Reproduction and regression | Fix |
|---|---|---|---|
| S01 log recovery erases history after header damage | Fixed in 40a6ef5 | `damage_to_any_field_of_an_acknowledged_frame_is_reported` flips a bit in each of the frame header's fields and the payload of an interior frame; `a_damaged_header_of_the_newest_segment_is_reported`, `damage_at_the_end_of_a_segment_the_log_went_past_is_reported`, `stale_frames_in_a_reused_slot_prove_nothing`, `an_opening_whose_header_never_became_durable_is_the_torn_tail` (`crates/log/tests/log.rs`). Removing either check fails its test. | Every stop in the segment that holds the last frame is checked for a later frame, whatever part of the frame no longer reads; a slot whose header no longer reads is checked for this log's frames past an opening's first, from a segment newer than any whose header reads. A failed open writes nothing. |
| S01, the last frame | Open | — | An acknowledged last frame damaged at rest reads as a torn write. Protocol-aware recovery keeps a persist record apart from each frame so the two can be told apart (AGL+18 §3.3.3; docs/research/03 R7.1). |
| S02 frame overflow reorders a group's updates | Fixed in 40a6ef5 | `a_groups_updates_keep_their_order_when_one_waits_for_room` holds the writer in a flush while three updates queue, and failed with the hard state at 1/1 after 2/2; `a_groups_queued_updates_become_durable_in_order` generates rounds of queued updates sized about a frame's room and checks each group's state against its accepted updates applied in order, before and after reopening. Reintroducing the defect fails both. | A group whose update waits for room stays taken for the frame, so its later updates wait behind it. |
| S03 log backlog outside admission | Fixed in a27792a | `the_queue_bounds_every_submission_not_yet_answered` holds the writer in a flush and found a third submission admitted under a bound of two; it now refuses at the bound, and a group at two unanswered submissions is refused while another group is taken. Giving room back when the writer takes a submission fails the test. | A submission holds its room until it is answered, from every answer path, so the bound covers updates waiting, held for a later frame and being written; a group holds two at most, the queue's two batches' worth for a group with one update a frame. |
| S04 replica Ready stranded by a recoverable refusal | Fixed in a27792a | `a_ready_refused_for_room_waits_until_the_group_compacts` (`crates/range/tests/group.rs`) proposes until the log refuses a ready for room: the replica reports it, refuses calls with `Stalled`, and after compaction writes the same ready and goes on. `a_member_refuses_settings_its_log_cannot_hold` covers the bounds checked at open. The replica simulation now runs bounds that make readies wait: 541 waited out over 400 seeds, every run linearizable. `an_update_in_parts_leaves_the_group_as_the_whole_would` checks the parts. Propagating the refusal again fails the group test. | A refused ready waits in the replica with its unwritten parts and its committed entries applied; a ready larger than a frame is written in parts, entries first and hard state last (etcd's order, research/06 §A10.3); a proposal larger than the range's entry bound is refused; a member whose log cannot hold the range's largest entry in a frame, or its largest ready within a group's bounds, refuses to open. Fatal log errors still stop the replica. |
| S05 chunk recovery removes acknowledged data after a bad read | Fixed in 27b2389 | `a_bad_read_at_recovery_never_drops_an_acknowledged_chunk` is the audit's reproduction: a read-only bit flip in an acknowledged put's payload at reopen; the chunk is now kept and reported, and reads back whole once the fault clears, across a second reopen. `a_damaged_chunk_is_reported_and_a_retry_writes_it_again` damages the stored bytes. The power-loss tests now require every chunk reported damaged to be a write that was in flight. Removing the chunk again fails both volume tests. | A record of the last batch that does not verify is never dropped: the volume cannot tell a torn write from damage to an acknowledged one (AGL+18 §3.3.3), so it keeps the record and reports it; a relocation's unverified copy still goes back to the intact copy it moved. |
| S06 calibration and benchmark scratch files | Open | | |
| S07 read admission after allocation | Open | | |
| S08 zero read depth | Open | | |
| S09 calibration ladder past the backend's depth | Open | | |
| S10 thread creation panics | Open | | |
| S11 zero-capacity buffers | Open | | |

## Spec defects

| Finding | State | Reproduction and regression | Fix |
|---|---|---|---|
| B01 overlapping file sweeps | Fixed | `a_delayed_sweep_never_releases_a_file_another_settled` (`crates/meta/tests/orphan_sweep.rs`) replays the audit's table and failed with a referenced file released. The orphan simulation now runs two file sweeps at once with every request delivered late, stopped sweeps' requests arriving after them, and the collector reclaiming against the real ownership graph: after every step each file a version, part or held composite names keeps its rows, blocks and chunks. | The sweep no longer removes marks. A mark is the Name range's record that it holds a file and goes only when the collector reclaims the file, so a delayed check always finds a held file held. |
| B02 losing multipart compositions | Fixed | `a_refused_completion_reclaims_none_of_its_parts` and `a_retried_completion_reclaims_none_of_the_committed_parts` (`crates/meta/src/reclaim.rs`) are the audit's two reproductions, and both failed with the parts taken apart. The orphan simulation's uploaders complete, have completions refused, retry with a new composite, and abort. Releasing a part without the owner check fails the unit tests and the simulation; not recording adoption leaks parts, which the simulation finds. | A committed completion marks each listed part as adopted by the object's file. The reclaimer no longer descends into a composite's parts: it gives each back to the Name range (`Disown`), which releases the part only if the composite being reclaimed adopted it. |
| B03 serial `u64::MAX` | Open | | |
| B04 weak `If-Match` | Open | | |
| B05 CRC collision taken as a retry | Fixed in 27b2389 | `a_crc_collision_is_not_a_retry` uses the audit's colliding pair, durable and within one batch; answering the second done again fails it. | A retry with the same length and CRC-32C reads the stored copy and compares bytes: the same answer done, different ones are refused, and a copy that no longer verifies is written again. A write within the same batch compares with the queued bytes. |
| B06 a refused block-sweep answer drops its request | Fixed in 8969e6f | The block sweep's unit test answers a check with too few verdicts and with an answer of another kind, and then asks the same request again. | The front group is put back on any refusal; groups are formed through a map. |
| B07 a version marker that names no version | Open | | |

## Performance investigations

| Finding | State | Measurement | Change |
|---|---|---|---|
| P01 range reads fetch the payload before them | Open | | |
| P02 multipart cleanup quadratic in apply | Fixed | | The listed parts are validated ascending, so each part is looked up by binary search, making a completion of n parts O(n log n) where it was n(n+1)/2 comparisons. A release benchmark of completions at 1 to 10,000 parts is still to be recorded. |
| P03 coordinator learning rescans | Open | | |
| P04 session answers rewritten per command | Open | | |
| P05 sweep grouping and extent scans | Grouping fixed in 8969e6f | | Due blocks are grouped by file through a map. The repeated extent scans remain. |
| P06 scrub and cleaner scan every segment | Open | | |
| P07 recovery and cold reads not coalesced | Open | | |
| P08 EC copies | Open | | |
| P09 calibration workers and workloads | Open | | |

## Production requirements

Sections 5 to 7 of the audit: each requirement is tracked here as it is designed, built and
tested.

| Section | State | Where |
|---|---|---|
| 5.1 staged Ready | Open | |
| 5.2 aggregate limits | Open | |
| 5.3 engine durability contract | Open | |
| 5.4 streamed snapshots | Open | |
| 5.5 ReadIndex batching | Open | |
| 5.6 fast track | Open | |
| 5.7 metadata voters across failure domains | Open | |
| 5.8 transport defenses | Open | |
| 5.9 failure model and invariants | Open | |
| 6.1–6.6 storage adaptation | Open | |
| 7.1–7.4 S3 conformance and layout | Open | |
