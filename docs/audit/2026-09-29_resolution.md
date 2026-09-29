# Resolving the 2026-09-29 audit

Each finding of [the audit](2026-09-29_audit.md), how it was reproduced, what fixed its cause,
and the test that now fails if the cause returns. A finding is closed only by a reproduction
that failed before the fix and passes after it, and by a check that the test fails when the
fix is removed.

## Standards defects

| Finding | State | Reproduction and regression | Fix |
|---|---|---|---|
| S01 log recovery erases history after header damage | Fixed | `damage_to_any_field_of_an_acknowledged_frame_is_reported` flips a bit in each of the frame header's fields and the payload of an interior frame; `a_damaged_header_of_the_newest_segment_is_reported`, `damage_at_the_end_of_a_segment_the_log_went_past_is_reported`, `stale_frames_in_a_reused_slot_prove_nothing`, `an_opening_whose_header_never_became_durable_is_the_torn_tail` (`crates/log/tests/log.rs`). Removing either check fails its test. | Every stop in the segment that holds the last frame is checked for a later frame, whatever part of the frame no longer reads; a slot whose header no longer reads is checked for this log's frames past an opening's first, from a segment newer than any whose header reads. A failed open writes nothing. |
| S01, the last frame | Open | — | An acknowledged last frame damaged at rest reads as a torn write. Protocol-aware recovery keeps a persist record apart from each frame so the two can be told apart (AGL+18 §3.3.3; docs/research/03 R7.1). |
| S02 frame overflow reorders a group's updates | Fixed | `a_groups_updates_keep_their_order_when_one_waits_for_room` holds the writer in a flush while three updates queue, and failed with the hard state at 1/1 after 2/2; `a_groups_queued_updates_become_durable_in_order` generates rounds of queued updates sized about a frame's room and checks each group's state against its accepted updates applied in order, before and after reopening. Reintroducing the defect fails both. | A group whose update waits for room stays taken for the frame, so its later updates wait behind it. |
| S03 log backlog outside admission | Open | | |
| S04 replica Ready stranded by a recoverable refusal | Open | | |
| S05 chunk recovery removes acknowledged data after a bad read | Open | | |
| S06 calibration and benchmark scratch files | Open | | |
| S07 read admission after allocation | Open | | |
| S08 zero read depth | Open | | |
| S09 calibration ladder past the backend's depth | Open | | |
| S10 thread creation panics | Open | | |
| S11 zero-capacity buffers | Open | | |

## Spec defects

| Finding | State | Reproduction and regression | Fix |
|---|---|---|---|
| B01 overlapping file sweeps | Open | | |
| B02 losing multipart compositions | Open | | |
| B03 serial `u64::MAX` | Open | | |
| B04 weak `If-Match` | Open | | |
| B05 CRC collision taken as a retry | Open | | |
| B06 a refused block-sweep answer drops its request | Fixed in 8969e6f | The block sweep's unit test answers a check with too few verdicts and with an answer of another kind, and then asks the same request again. | The front group is put back on any refusal; groups are formed through a map. |
| B07 a version marker that names no version | Open | | |

## Performance investigations

| Finding | State | Measurement | Change |
|---|---|---|---|
| P01 range reads fetch the payload before them | Open | | |
| P02 multipart cleanup quadratic in apply | Open | | |
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
