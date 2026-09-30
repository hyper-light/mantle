# Operating constants

Every number the code runs with is either a fact of a format or an interface, derived from
other numbers, taken from a cited source, measured on the running hardware, or a bound whose
sufficiency is argued (CLAUDE.md §4). This file records which, for every numeric constant in
production code, and for the parameters that are not constants but set how the system or a
benchmark runs (audit §12.6).

A row's kind is one of:

- **format**: a field width, tag, version, magic or length of an on-disk or wire encoding.
  Changing it changes the format.
- **external**: set by a specification or interface: the AWS S3 API reference, an RFC, an
  operating system's interface, an algorithm's definition.
- **derived**: computed from other values, or a unit conversion.
- **cited**: taken from a source cited for this value.
- **measured**: measured on hardware, with the measurement named.
- **bound**: a resource or safety guard whose value is not a performance choice and whose
  sufficiency is argued.
- **open**: an operating choice nothing yet establishes. Each says what would.

`scripts/check-contracts.py` fails when a numeric constant in production code has no row
here, when a row names a constant that no longer exists, and when the rows marked `open`
outnumber its ceiling, which only falls: a new unfounded constant cannot land, and one that
gains a basis lowers the ceiling.

## Constants

| Constant | Kind | Basis |
|---|---|---|
| `crates/chunk/src/frame.rs` `CRC_AT` | format | Byte offset of the CRC-32C inside the 48-byte frame header. |
| `crates/chunk/src/frame.rs` `DELETE_LEN` | format | Encoded delete-record length, the sum of its field widths. |
| `crates/chunk/src/frame.rs` `FRAME_HEADER` | format | Byte length of the index-log frame header, fixed by its encoding. |
| `crates/chunk/src/frame.rs` `FRAME_VERSION` | format | Version field of the index-log frame header; changing it changes the on-disk frame format (docs/design/chunk-store.md §3.2). |
| `crates/chunk/src/frame.rs` `KIND_BATCH` | format | Frame kind code for a group-commit batch frame. |
| `crates/chunk/src/frame.rs` `KIND_CHECKPOINT_BEGIN` | format | Frame kind code for a checkpoint's first frame. |
| `crates/chunk/src/frame.rs` `KIND_CHECKPOINT_CHUNK` | format | Frame kind code for a checkpoint body frame. |
| `crates/chunk/src/frame.rs` `KIND_CHECKPOINT_END` | format | Frame kind code for a checkpoint's end frame. |
| `crates/chunk/src/frame.rs` `KIND_WRAP` | format | Frame kind code marking the end of a log lap. |
| `crates/chunk/src/frame.rs` `PUT_LEN` | format | Encoded put-record length, the sum of its field widths (76 bytes). |
| `crates/chunk/src/frame.rs` `SEGMENT_LEN` | format | Encoded segment-record length, the sum of its field widths (18 bytes). |
| `crates/chunk/src/frame.rs` `TAG_DELETE` | format | Record tag for a delete record inside a frame. |
| `crates/chunk/src/frame.rs` `TAG_PUT` | format | Record tag for a put record inside a frame. |
| `crates/chunk/src/frame.rs` `TAG_SEGMENT` | format | Record tag for a segment-state record inside a frame. |
| `crates/chunk/src/key.rs` `ENCODED_LEN` | format | Encoded ChunkKey width: u128 block + u32 epoch + u16 index = 22 bytes. |
| `crates/chunk/src/layout.rs` `FRAME_PAYLOAD` | derived | MAX_FRAME_BYTES less FRAME_HEADER. |
| `crates/chunk/src/layout.rs` `MAX_FRAME_BYTES` | bound | 4 MiB reader and writer frame ceiling checked by `Config::check` (audit S13); audit §12.6 asks to name the header and alignment terms and prove every legal frame fits. |
| `crates/chunk/src/record.rs` `CRC_AT` | format | Byte offset of the header CRC-32C within the 96-byte record header. |
| `crates/chunk/src/record.rs` `FLAG_FINAL` | format | Flag bit marking a chunk sealed at the end of the record. |
| `crates/chunk/src/record.rs` `HEADER_LEN` | format | Byte length of a data record header before its checksum table (docs/design/chunk-store.md §3.1). |
| `crates/chunk/src/record.rs` `KIND_PAYLOAD` | format | Record kind code for a payload record. |
| `crates/chunk/src/record.rs` `RECORD_ALIGN` | format | Records start 8-byte aligned within a batch; a layout fact of the segment format. |
| `crates/chunk/src/scrub.rs` `MAX_DAMAGED` | bound | Damaged-chunk list cap; past it the volume is drained whole, argued from BGPS07 (most disks with latent sector errors have under 50) in research 03 and research 11 §12.4. |
| `crates/chunk/src/scrub.rs` `REGION_STEPS` | cited | 128 steps, SDG10's 128 MiB staggered region (FAST 2010 §5.2.3; research 03 S2). |
| `crates/chunk/src/scrub.rs` `STEP` | cited | 1 MiB scrub step from Schroeder, Damouras and Gill, FAST 2010 §5.2.3, staggered scrubbing (docs/design/chunk-store.md §9; research 03 R9.2). |
| `crates/chunk/src/superblock.rs` `OFFSET_A` | format | Superblock A sits at byte 0 of the volume. |
| `crates/chunk/src/superblock.rs` `OFFSET_B_COMPACT` | format | Superblock B at 64 KiB for compact test volumes; a layout fact of the compact format with no clustering argument. |
| `crates/chunk/src/superblock.rs` `OFFSET_B_STANDARD` | format | Superblock B at 16 MiB, past the 10 MB clustering of latent sector errors (BGPS07 §5; chunk-store.md §2). |
| `crates/chunk/src/superblock.rs` `VERSION` | format | Superblock format version. |
| `crates/chunk/src/writer.rs` `CLEANER_RESERVE` | bound | One free segment kept for the cleaner (RO92 §3.6); research 11 §10.3 argues one suffices while one victim is relocated at a time. |
| `crates/chunk/src/writer.rs` `INCARNATION_RESERVE` | bound | 2^16, about 1,000 times the need at 11 segments a second by the same cost calculation (research 11 §11.3); audit §12.6 asks for the measured rate and an overhead goal. |
| `crates/chunk/src/writer.rs` `SEQUENCE_RESERVE` | bound | 2^24 keeps reservation cost under 10^-3 at the measured 4.7 ms flush (research 11 §11.3); audit §12.6 asks to parameterize it by the measured rate and fence cost. |
| `crates/crc/src/lib.rs` `POLY` | external | The bit-reflected generator polynomial of CRC-32C (0x82F63B78), CRC-32/ISO-HDLC (0xEDB88320) and CRC-64/NVME (0x9A6C9329AC4BC9B5), one per `reflected_crc!` instance. |
| `crates/crc/src/lib.rs` `TOP` | derived | `1 << (bits - 1)`: x^0 in the reflected register of the macro's width, as in zlib crc32.c `multmodp`/`x2nmodp`. |
| `crates/disk/src/buf.rs` `MAX_ALIGNMENT` | bound | 1 MiB alignment cap above the 512 B–64 KiB direct-I/O alignments of every target (open(2) O_DIRECT notes; Windows File Buffering). |
| `crates/disk/src/buf.rs` `MAX_BUFFER` | bound | Turns a corrupt length into a refusal; sufficient for every I/O, since the chunk store and the Raft log refuse a segment larger than one buffer before any I/O and every other I/O is a frame or batch. |
| `crates/disk/src/calibrate.rs` `P50_SAMPLES` | open | 16 from n ≥ 1.96²q(1−q)/δ² at δ = (1−q)/2 (research 11 §13.3); the 95% level and the δ rule are chosen, not cited. |
| `crates/disk/src/calibrate.rs` `P99_SAMPLES` | open | 1,522 from the same binomial normal approximation; the 95% confidence and δ = (1−q)/2 tolerance have no cited basis. |
| `crates/disk/src/commit.rs` `RATE_ONE` | open | 2^16 fixed-point unit for return counts and the share p; the resolution p needs is not derived. |
| `crates/disk/src/histogram.rs` `BUCKETS` | derived | OVERFLOW + 1. |
| `crates/disk/src/histogram.rs` `MAX_EXP` | open | Rows up to 2^41 ns (about 37 minutes); research 11 §14.3 says the top should be the longest latency a consumer acts on. |
| `crates/disk/src/histogram.rs` `OVERFLOW` | derived | ROWS × SUB, the overflow bucket's index. |
| `crates/disk/src/histogram.rs` `ROWS` | derived | (MAX_EXP − SUB_BITS) + 2 rows. |
| `crates/disk/src/histogram.rs` `SUB` | derived | 2^SUB_BITS sub-buckets per row. |
| `crates/disk/src/histogram.rs` `SUB_BITS` | open | 5 bits (3.1% error) by research 11 §14.3's α ≤ ε/2 rule, but ε = ±10% is an example resolution, not measured or cited. |
| `crates/disk/src/histogram.rs` `SUB_MASK` | derived | SUB − 1 as a bit mask. |
| `crates/disk/src/measure.rs` `MAX_DEPTH` | open | 256 threads per measurement job; audit §12.6 says it is a backend ceiling, not the device's depth, to be derived from CPU, RAM and thread allowances. |
| `crates/disk/src/node/macos.rs` `BLOCK_COUNT` | external | DKIOCGETBLOCKCOUNT, _IOR('d', 25, uint64_t), macOS <sys/disk.h>. |
| `crates/disk/src/node/macos.rs` `BLOCK_SIZE` | external | DKIOCGETBLOCKSIZE, _IOR('d', 24, uint32_t), macOS <sys/disk.h>. |
| `crates/disk/src/node/macos.rs` `SYNCHRONIZE` | external | DKIOCSYNCHRONIZE, _IOW('d', 22, dk_synchronize_t), macOS <sys/disk.h>. |
| `crates/disk/src/probe/linux.rs` `MAX_DEPTH` | bound | Stack bound on the recursive sysfs walk, past the three-deep dm-crypt, LVM and md stack; a deeper composite's medium is left unknown, never guessed (CLAUDE.md rule 5). |
| `crates/disk/src/probe/macos.rs` `CF_NUMBER_SINT64` | external | kCFNumberSInt64Type = 4, CoreFoundation CFNumber.h. |
| `crates/disk/src/probe/macos.rs` `ITERATE_PARENTS` | external | kIORegistryIterateParents = 0x2, IOKit IOKitKeys.h. |
| `crates/disk/src/probe/macos.rs` `ITERATE_RECURSIVELY` | external | kIORegistryIterateRecursively = 0x1, IOKit IOKitKeys.h. |
| `crates/disk/src/probe/macos.rs` `MAIN_PORT` | external | kIOMainPortDefault = 0, IOKit. |
| `crates/disk/src/probe/macos.rs` `MAX_STRING` | bound | 256-byte buffer for IOKit string properties, far above IOKit names and model strings; a longer one fails without writing. |
| `crates/disk/src/probe/macos.rs` `UTF8` | external | kCFStringEncodingUTF8 (0x08000100), CoreFoundation CFString.h. |
| `crates/disk/src/probe/windows.rs` `VOLUME_GUID_CHARS` | external | 50 characters, the size GetVolumeNameForVolumeMountPointW's documentation gives for the largest volume GUID path (Microsoft Learn). |
| `crates/disk/src/rounds.rs` `MAX_ROUNDS` | bound | 120 rounds, the most for which the exact binomial and runs-test sums fit in u128 (40 · 2^n). |
| `crates/disk/src/sim.rs` `MAX_SIM_LEN` | bound | 1 GiB cap on the simulated file, which tests hold twice in memory. |
| `crates/ec/src/durability.rs` `PRECISION` | bound | Relative tolerance of 1e-3 on loss probabilities, argued in the code: the report prints two significant figures, and 1e-3 keeps the third correct (docs/research/15 §4.4–4.5). |
| `crates/ec/src/durability.rs` `YEAR` | derived | Unit conversion: 365.25 days × 24 h = 8766 hours. |
| `crates/gateway/src/layout.rs` `CHUNK` | cited | Tectonic's "typically 8 MiB" chunk (docs/research/01 §1.14). docs/design/gateway.md says measured transfer sizing will replace it, and audit §12.6/§16.2 requires per-upload sizing. |
| `crates/gateway/src/layout.rs` `SEALED` | derived | `seal::SEGMENT + seal::TAG`: a 64 KiB plaintext segment plus its 16-byte AEAD tag (docs/design/gateway.md §1). |
| `crates/gateway/src/put.rs` `RENEWALS` | cited | Quarter-lease renewal from Centrifuge's 15 s renewals of 60 s leases (docs/research/09 §7.2.2). Audit §12.6 requires deriving it from control-delay and outage distributions. |
| `crates/log/src/format.rs` `DAMAGED` | format | Record kind code 8 in the frame payload encoding. |
| `crates/log/src/format.rs` `ENTRIES` | format | Record kind code 1 in the frame payload encoding. |
| `crates/log/src/format.rs` `ENTRY_HEADER_BYTES` | format | The same 16-byte entry header length as a u64 for byte accounting. |
| `crates/log/src/format.rs` `ENTRY_HEADER_LEN` | format | Entry header of term, length and CRC (16 bytes) in the frame payload encoding. |
| `crates/log/src/format.rs` `FORMAT` | format | Version byte of the log's on-disk format (3: a confirmation rewrites its frame's record in its own slot, and an open restoring a lost frame copies its record to the other slot; 2: persist area first, `Uncertain` records); raft-log.md §2, §6. |
| `crates/log/src/format.rs` `FRAME_HEADER_BYTES` | format | The same 68-byte frame header length as a u64 for byte accounting. |
| `crates/log/src/format.rs` `FRAME_HEADER_LEN` | format | Byte length of the encoded frame header, fixed by the frame layout in format.rs and raft-log.md §2. |
| `crates/log/src/format.rs` `HARD_STATE` | format | Record kind code 3 in the frame payload encoding. |
| `crates/log/src/format.rs` `HAS_ENTRIES` | format | Bit 2 of a persist group's field flags byte. |
| `crates/log/src/format.rs` `HAS_HARD_STATE` | format | Bit 0 of a persist group's field flags byte. |
| `crates/log/src/format.rs` `HAS_PROPOSALS` | format | Bit 5 of a persist group's field flags byte. |
| `crates/log/src/format.rs` `HAS_START` | format | Bit 1 of a persist group's field flags byte. |
| `crates/log/src/format.rs` `HAS_UNCERTAIN` | format | Bit 3 of a persist group's field flags byte. |
| `crates/log/src/format.rs` `IS_DAMAGED` | format | Bit 6 of a persist group's field flags byte. |
| `crates/log/src/format.rs` `IS_REMOVED` | format | Bit 4 of a persist group's field flags byte. |
| `crates/log/src/format.rs` `PERSIST_GROUP_LEN` | format | Encoded length of one group in a persist record (ID, flags, hard state, start, entries, uncertainty). |
| `crates/log/src/format.rs` `PERSIST_HEADER_LEN` | format | Persist record header length (magic, format, padding, log ID, sequence, confirms, count), as its doc comment lists the fields. |
| `crates/log/src/format.rs` `PROPOSAL` | format | Record kind code 5 in the frame payload encoding. |
| `crates/log/src/format.rs` `RELOCATED` | format | Record kind code 2 in the frame payload encoding. |
| `crates/log/src/format.rs` `REMOVED` | format | Record kind code 6 in the frame payload encoding. |
| `crates/log/src/format.rs` `SEGMENT_HEADER_LEN` | format | Byte length of the encoded segment header before padding, fixed by the layout in format.rs and raft-log.md §2. |
| `crates/log/src/format.rs` `START` | format | Record kind code 4 in the frame payload encoding. |
| `crates/log/src/format.rs` `UNCERTAIN` | format | Record kind code 7 in the frame payload encoding. |
| `crates/log/src/lib.rs` `GROUP_SUBMISSIONS` | derived | The most a replica has unanswered at once: its ready's part in flight and a compaction, each waiting for its answer (replica.md §3–§4). A replica with more than one ready in flight (§7) raises it. |
| `crates/log/src/state.rs` `DAMAGED_BYTES` | format | Encoded size of a `Damaged` record (kind and group) in the frame payload. |
| `crates/log/src/state.rs` `HARD_STATE_BYTES` | format | Encoded size of a `HardState` record in the frame payload. |
| `crates/log/src/state.rs` `PROPOSAL_EXTRA` | format | A proposal's encoded bytes beyond an entry's: kind, group and index (1 + 16 + 8). |
| `crates/log/src/state.rs` `START_BYTES` | format | Encoded size of a `Start` record (kind, group, index, term) in the frame payload. |
| `crates/log/src/state.rs` `UNCERTAIN_BYTES` | format | Encoded size of an `Uncertain` record in the frame payload. |
| `crates/mantle/src/bench.rs` `WORKERS` | open | 64 concurrent deleters "so they share flushes"; the count lacks a derivation or measurement. |
| `crates/mantle/src/bench_gateway.rs` `HANDOVER` | bound | 2^40 ticks of a cell clock that moves one tick a command, past any number of commands a bounded run applies, so no deadline expires in a benchmark. |
| `crates/mantle/src/bench_hash.rs` `CHUNKED_BODY` | open | 16 MiB signed-chunk body measured; no derivation in code or the measurement docs. |
| `crates/mantle/src/bench_hash.rs` `FORM_FILE` | open | 16 MiB form file measured, set equal to CHUNKED_BODY; lacks a derivation. |
| `crates/mantle/src/bench_log.rs` `KEEP` | open | A replica compacts every 64 entries, keeping 64 behind; it stands in for a follower window but lacks a derivation. |
| `crates/mantle/src/bench_meta.rs` `HISTORY` | open | Session answer history of 256; the metadata-apply measurement states it without a derivation. |
| `crates/meta/src/collector.rs` `GRACE_NS` | cited | GFS keeps deleted files for three days (GGL03 §4.4, docs/research/22 §1.1). A recovery-point policy, not a safety bound (22 §10.4). |
| `crates/meta/src/file.rs` `MAX_EXTENTS` | external | The S3 limit of 10,000 parts per upload (docs/research/05 §4.1). Audit §12.6 says single-PUT block manifests reuse this count and need their own derived fanout limit. |
| `crates/meta/src/key.rs` `ATTEMPT` | format | Marker byte `a` after `LOCAL` for Bucket-range create/delete attempts. |
| `crates/meta/src/key.rs` `CHUNK` | format | Block-layer row tag 2 for a block's chunk places. |
| `crates/meta/src/key.rs` `CLOCK` | format | Marker byte `c` after `LOCAL` for the range clock row. |
| `crates/meta/src/key.rs` `CONFIGURATION` | format | Marker byte `r` after `LOCAL` for the group configuration row. |
| `crates/meta/src/key.rs` `DATA` | format | Key-space prefix byte 0x01 for the layer's rows (docs/design/metadata.md §1). |
| `crates/meta/src/key.rs` `EXPIRY` | format | Marker byte `e` after `LOCAL` for session last-use order. |
| `crates/meta/src/key.rs` `EXTENT` | format | File-layer row tag 2 for a file's extents. |
| `crates/meta/src/key.rs` `FLOOR` | format | Marker byte `f` after `LOCAL` for the Name range's gate floor. |
| `crates/meta/src/key.rs` `GATE` | format | Marker byte `g` after `LOCAL` for gate rows. |
| `crates/meta/src/key.rs` `HEADER` | format | File-layer row tag 1 for a file's header. |
| `crates/meta/src/key.rs` `INSTALLED` | format | Marker byte `p` after `LOCAL` for the last installed snapshot. |
| `crates/meta/src/key.rs` `LINEAGE` | format | Marker byte `l` after `LOCAL` for the Name range's lineage row. |
| `crates/meta/src/key.rs` `LOCAL` | format | Key-space prefix byte 0x00 for a range's own rows (docs/design/metadata.md §1). |
| `crates/meta/src/key.rs` `MARK` | format | Component tag 1 after a routing key in the marks space; below 0xFF so it is not read as an escaped 0x00 (FDB tuple-layer byte string). |
| `crates/meta/src/key.rs` `MARKS` | format | Key-space prefix byte 0x03 for the Name range's file marks (docs/design/metadata.md §2). |
| `crates/meta/src/key.rs` `NULL` | format | Name-row tag 1: null-version pointer, sorting first under `(bucket, key)`. |
| `crates/meta/src/key.rs` `ORIGIN` | format | Block-layer row tag 3 for a block's origin. |
| `crates/meta/src/key.rs` `PART` | format | Tag 1 after an upload ID for its part rows. |
| `crates/meta/src/key.rs` `RELEASED` | format | Marker byte `q` after `LOCAL` for the queue of released files. |
| `crates/meta/src/key.rs` `REVERSE` | format | Key-space prefix byte 0x02 for reverse-index rows. |
| `crates/meta/src/key.rs` `SESSION` | format | Marker byte `s` after `LOCAL` for session rows. |
| `crates/meta/src/key.rs` `SESSIONS` | format | Marker byte `n` after `LOCAL` for the session count row. |
| `crates/meta/src/key.rs` `UNSETTLED` | format | Marker byte `u` after `LOCAL` for files and blocks whose handover is unsettled. |
| `crates/meta/src/key.rs` `UPLOAD` | format | Name-row tag 3: upload rows, sorting after versions. |
| `crates/meta/src/key.rs` `VERSION` | format | Name-row tag 2: version rows, sorting after the null pointer. |
| `crates/meta/src/name.rs` `MAX_PART` | external | The S3 maximum part size of 5 GiB (docs/research/05 §4.1, AWS S3 multipart limits). |
| `crates/meta/src/name.rs` `MAX_PART_NUMBER` | external | S3 part numbers run from 1 to 10,000 (docs/research/05 §4.3, AWS UploadPart reference). |
| `crates/meta/src/name.rs` `MIN_PART` | external | The S3 minimum part size of 5 MiB, with no minimum for the last part (docs/research/05 §4.1, AWS S3 multipart limits). |
| `crates/meta/src/record.rs` `DAY_MS` | derived | Unit conversion: 86,400 s × 1,000 ms per day, for S3 Object Lock retention days and years (docs/research/18 §2.4, §2.6). |
| `crates/meta/src/record.rs` `FORMAT` | format | Version byte 1 that begins every row value written by this code. |
| `crates/meta/src/record.rs` `LISTING` | external | Bytes of a completion's listing digest: SHA-256's 256-bit output (FIPS 180-4 §1). |
| `crates/meta/src/wire.rs` `MAX_BUCKET` | external | A bucket name of at most 63 characters (research/05 §10.3). |
| `crates/meta/src/wire.rs` `MAX_KEY` | external | An object key of at most 1,024 bytes (research/05 §10.1). |
| `crates/meta/src/wire.rs` `MAX_HEADERS` | external | A request's headers within 8 KB (research/05 §10.2), which bound its preconditions. |
| `crates/meta/src/wire.rs` `MAX_PARTS` | external | 10,000 parts to an upload (research/05 §4.1). |
| `crates/meta/src/wire.rs` `ETAG_HEX` | external | A part's ETag, the hex of its 16-byte MD5 (research/05 §4.5). |
| `crates/meta/src/wire.rs` `FORMAT` | format | Version byte 1 of an encoded log entry. |
| `crates/meta/src/wire.rs` `MAX_COMMANDS` | bound | 2^16 command places per entry, so a session ID is index × 2^16 + place (session.rs `register`); the audit (§12.6) keeps it as a versioned encoding bound with byte and work budgets beside it. |
| `crates/range/src/conf.rs` `FORMAT` | format | Version byte of the encoded configuration row. |
| `crates/range/src/image.rs` `FORMAT` | format | Version byte of the encoded snapshot image. |
| `crates/range/src/replica.rs` `DRIVE_BUDGET` | open | 64 readies per `drive` call; audit §12.6 calls it a counted termination guard with no wall-time or work proof and asks for slices derived from control deadlines. |
| `crates/range/src/store.rs` `ENTRY_OVERHEAD` | format | An entry encoding's kind byte plus u32 context length (1 + 4), per `encode_entry`. |
| `crates/s3/src/acl.rs` `MAX_GRANTS` | external | AWS S3 API reference via docs/research/13 §6.8: "An ACL can have up to 100 grants". |
| `crates/s3/src/body.rs` `ACL_LIMIT` | derived | (prolog + owner fields escaped + MAX_GRANTS × GRANT); s3-protocol.md §2 gives 662,054 bytes. |
| `crates/s3/src/body.rs` `BUCKET_TAGGING_LIMIT` | derived | (TAGGING + 50 bucket tags × TAG). |
| `crates/s3/src/body.rs` `CHECKSUMS` | derived | Computed at compile time by summing each `Algorithm`'s element tags and base64 width. |
| `crates/s3/src/body.rs` `CLASS` | derived | Length of "INTELLIGENT_TIERING", the longest name in `lifecycle::CLASSES`. |
| `crates/s3/src/body.rs` `COMPLETE_LIMIT` | derived | (prolog + 10,000 parts × (part markup + ETAG + CHECKSUMS)); s3-protocol.md §2 gives 14,040,274 bytes. |
| `crates/s3/src/body.rs` `CORS_LIMIT` | derived | Equal to `cors::LIMIT`, S3's 64 KB CORS document limit. |
| `crates/s3/src/body.rs` `CREATE_BUCKET_LIMIT` | derived | (prolog + location constraint of MAX_REGION + 50 bucket tags × TAG). |
| `crates/s3/src/body.rs` `DATE_TIME` | derived | Length of the longest ISO 8601 time a serializer writes, with nanoseconds and a zone offset. |
| `crates/s3/src/body.rs` `DELETE_LIMIT` | derived | (prolog + 1,000 objects × (escaped key, version ID, ETag, time and size fields at their longest)); s3-protocol.md §2. |
| `crates/s3/src/body.rs` `ESCAPED` | external | Six bytes, the longest one-byte escape (`&quot;`, `&apos;`, `&#x0D;`) under XML 1.0 §2.4 and S3's key rules (05 §10.1); docs/design/s3-protocol.md §2. |
| `crates/s3/src/body.rs` `ETAG` | derived | Length of the longest ETag mantle writes (a 10,000-part multipart ETag) with its quotes escaped. |
| `crates/s3/src/body.rs` `GRANT` | derived | Length of the longest grant markup (email grantee with xsi type) plus ESCAPED × (MAX_EMAIL + MAX_DISPLAY_NAME). |
| `crates/s3/src/body.rs` `INT` | external | Length of "-2147483648", the longest `xs:int` a serializer writes (XML Schema Part 2 int range). |
| `crates/s3/src/body.rs` `LEGAL_HOLD_LIMIT` | derived | (prolog + the LegalHold markup). |
| `crates/s3/src/body.rs` `LIFECYCLE_FILTER` | derived | Filter markup + ESCAPED × MAX_KEY + 2 × LONG + MAX_FILTER_TAGS × TAG. |
| `crates/s3/src/body.rs` `LIFECYCLE_LIMIT` | derived | (prolog + 1,000 rules × LIFECYCLE_RULE). |
| `crates/s3/src/body.rs` `LIFECYCLE_RULE` | derived | Rule markup + escaped ID + LIFECYCLE_FILTER + dates, ints and one transition per class, each at its longest. |
| `crates/s3/src/body.rs` `LONG` | external | Length of "-9223372036854775808", the longest `xs:long` (XML Schema Part 2 long range). |
| `crates/s3/src/body.rs` `MAX_DISPLAY_NAME` | derived | Equal to MAX_EMAIL, because S3's sample display names are email addresses (13 §6.8). |
| `crates/s3/src/body.rs` `MAX_EMAIL` | external | SMTP's 256-octet path less its angle brackets, 254 octets (RFC 5321 §4.5.3.1.3). |
| `crates/s3/src/body.rs` `MAX_ID` | external | A canonical user ID is 64 hex digits in every AWS sample (13 §6.8); here it is counted as arbitrary text. |
| `crates/s3/src/body.rs` `MAX_OBJECTS` | external | AWS S3 API reference, DeleteObjects via 05 §8.1: "a list of up to 1,000 keys". |
| `crates/s3/src/body.rs` `MAX_PARTS` | external | AWS S3 API reference via docs/research/05 §4.1: a multipart upload holds at most 10,000 parts. |
| `crates/s3/src/body.rs` `MAX_REGION` | external | A region name is a DNS label, at most 63 octets (RFC 1035 §2.3.4). |
| `crates/s3/src/body.rs` `MAX_UPLOAD` | external | AWS S3 limits via 05 §4.1: a part runs 5 MiB to 5 GiB and a single PUT to 5 GB; 5 << 30 is the binary reading, so it never refuses an upload S3 accepts. |
| `crates/s3/src/body.rs` `MAX_VERSION_ID` | external | AWS S3 docs via 13 §6.4: version IDs are opaque strings "no more than 1,024 bytes long". |
| `crates/s3/src/body.rs` `OBJECT_LOCK_LIMIT` | derived | (prolog + ObjectLockConfiguration markup with the longest mode and an `xs:int` period). |
| `crates/s3/src/body.rs` `OBJECT_TAGGING_LIMIT` | derived | (TAGGING + 10 object tags × TAG). |
| `crates/s3/src/body.rs` `OWNERSHIP_CONTROLS_LIMIT` | derived | (prolog + a rule holding the longest ObjectOwnership value). |
| `crates/s3/src/body.rs` `PROLOG` | derived | Length of the XML declaration and root `xmlns` declaration plus `NAMESPACE.len()`. |
| `crates/s3/src/body.rs` `PUBLIC_ACCESS_BLOCK_LIMIT` | derived | (prolog + the four boolean settings' markup). |
| `crates/s3/src/body.rs` `RETENTION_LIMIT` | derived | (prolog + Retention markup + DATE_TIME). |
| `crates/s3/src/body.rs` `SSE_LIMIT` | derived | (prolog + the longest rule markup + 2048, botocore's longest KMS key ID); docs/research/20 §4.1. |
| `crates/s3/src/body.rs` `TAG` | derived | Tag markup plus ESCAPED × UTF8_PER_UTF16 × (128 + 256) code units, the key and value limits from 13 §6.7. |
| `crates/s3/src/body.rs` `TAGGING` | derived | PROLOG plus the length of the `Tagging`/`TagSet` markup. |
| `crates/s3/src/body.rs` `UTF8_PER_UTF16` | external | At most 3 UTF-8 bytes per UTF-16 code unit (RFC 3629 §3; RFC 2781 §2.1). |
| `crates/s3/src/body.rs` `VERSIONING_LIMIT` | derived | (prolog + the longest Status and MfaDelete markup). |
| `crates/s3/src/chunked.rs` `MAX_LINE` | derived | 16 hex size digits + 17 bytes of `;chunk-signature=` + 64 signature hex digits, the longest aws-chunked framing line (05 §1.8–§1.9). |
| `crates/s3/src/chunked.rs` `MIN_CHUNK` | external | S3 developer guide, Transfer Payload in Multiple Chunks (05 §1.8): each chunk except the last is at least 8 KB. |
| `crates/s3/src/cors.rs` `LIMIT` | external | AWS S3 CORS docs via 16 §1.1: "The document is limited to 64 KB in size"; the binary reading is used. |
| `crates/s3/src/cors.rs` `MAX_ID` | external | AWS S3 CORS docs via 16 §1.3: a rule ID "cannot be longer than 255 characters". |
| `crates/s3/src/cors.rs` `MAX_RULES` | external | AWS S3 CORS docs via docs/research/16 §1.3: "up to 100 rules". |
| `crates/s3/src/form.rs` `LIMIT` | derived | CRLF.len() + MAX_PRE_DATA. |
| `crates/s3/src/form.rs` `MAX_BOUNDARY` | external | RFC 2046 §5.1.1: a boundary is at most 70 characters. |
| `crates/s3/src/form.rs` `MAX_PRE_DATA` | external | AWS POST Object docs via docs/research/19 §2.1: form data and boundaries excluding the file "cannot exceed 20KB"; the larger 1,024-byte reading is used. |
| `crates/s3/src/json.rs` `MAX_DEPTH` | derived | One level past the deepest document read, a bucket policy at six (17 §3.1), so a check finds a container where a value belongs and refuses it with S3's message. |
| `crates/s3/src/lifecycle.rs` `DAY` | derived | 86,400,000, the milliseconds in a day. |
| `crates/s3/src/lifecycle.rs` `MAX_FILTER_SIZE` | external | 1000 × 2^40, the range S3 gave in its error "should be between 1 and 1099511627776000" (13 §6.9). |
| `crates/s3/src/lifecycle.rs` `MAX_FILTER_TAGS` | derived | Equal to `Tagged::Object.limit()` (10), since a filter asking for more tags than an object holds matches nothing. |
| `crates/s3/src/lifecycle.rs` `MAX_ID` | external | AWS S3 lifecycle docs via 13 §6.9: "ID length is limited to 255 characters". |
| `crates/s3/src/lifecycle.rs` `MAX_RULES` | external | AWS S3 lifecycle docs via 13 §6.9: "up to 1,000 rules. This limit is not adjustable". |
| `crates/s3/src/lifecycle.rs` `SECONDS_PER_DAY` | derived | 86,400, the seconds in a day. |
| `crates/s3/src/list.rs` `MAX_KEYS` | external | AWS S3 API reference, ListObjectsV2 via 05 §6.2: up to 1,000 keys per page; s3-protocol.md §3 reuses it as the bound on keys a page passes over. |
| `crates/s3/src/lock.rs` `DAY` | derived | 86,400,000, the milliseconds in a day. |
| `crates/s3/src/lock.rs` `MAX_DAYS` | external | S3 Object Lock docs via docs/research/18 §2.4, §2.6: a 100-year maximum at 365 days per year. |
| `crates/s3/src/lock.rs` `MAX_YEARS` | external | S3 Object Lock docs via 18 §2.4: "The maximum retention period is 100 years". |
| `crates/s3/src/policy/mod.rs` `BODY_LIMIT` | open | Twice the 20 KB compact limit, as a raw bound on a policy kept as sent (17 §2.2); what S3 keeps of a policy's white space is not recorded, and a recording of S3 would settle it. |
| `crates/s3/src/policy/mod.rs` `MAX_SIZE` | external | S3's 20 KB bucket policy limit; S3 reported "maximum allowed size of 20480 bytes" (docs/research/17 §2.3). |
| `crates/s3/src/route.rs` `MAX_KEY` | external | AWS S3 object key docs via 05 §10.1: keys are at most 1,024 bytes of UTF-8. |
| `crates/s3/src/seal.rs` `LAST` | format | 1 << 63, the top bit of the nonce counter that marks a file's last segment (encryption.md nonce layout, STREAM construction). |
| `crates/s3/src/seal.rs` `PLAIN_SEGMENT` | cited | A second literal copy of SEGMENT's 64 KiB as u64, with the same GFS [GGL03 §5.2] basis; it is not written in terms of SEGMENT. |
| `crates/s3/src/seal.rs` `SEALED_SEGMENT` | derived | 64 KiB segment plus the 16-byte GCM tag. |
| `crates/s3/src/seal.rs` `SEGMENT` | cited | 64 KiB, the chunk store's checksum block, from GFS [GGL03 §5.2] (docs/design/encryption.md, chunk-store.md §3.1); it also fixes the sealed at-rest format. |
| `crates/s3/src/seal.rs` `TAG` | external | GCM's full 128-bit tag, 16 bytes (NIST SP 800-38D §5.2.1.2). |
| `crates/s3/src/seal.rs` `TAG_BYTES` | external | GCM's 16-byte tag as u64 (NIST SP 800-38D §5.2.1.2), a second copy of TAG. |
| `crates/s3/src/seal.rs` `WRAPPED` | external | A 32-byte key plus one 8-byte semiblock under AES key wrap (NIST SP 800-38F §6.2). |
| `crates/s3/src/sigv4.rs` `MAX_EXPIRES_SECS` | external | S3 developer guide, Using Query Parameters (05 §1.7): `X-Amz-Expires` runs 1 to 604,800 seconds. |
| `crates/s3/src/sigv4.rs` `MAX_SKEW_SECS` | external | S3 developer guide, Authenticating Requests (05 §1.6): signed requests are valid within 15 minutes of their timestamp. |
| `crates/s3/src/tagging.rs` `MAX_KEY` | external | AWS S3 tagging docs via 13 §6.7: a tag key is up to 128 Unicode characters, counted in UTF-16 units. |
| `crates/s3/src/tagging.rs` `MAX_VALUE` | external | AWS S3 tagging docs via 13 §6.7: a tag value is up to 256 UTF-16 units. |
| `crates/s3/src/time.rs` `SECONDS_PER_DAY` | derived | 86,400, the seconds in a day. |

## Parameters that are not constants

Defaults of configuration structures, calibration plans and the command line's benchmark
parameters. The gate does not parse these; they are kept here with the same kinds.

| Where | Kind | Basis |
|---|---|---|
| `crates/mantle/src/bench.rs:193` chunk alignment floor 4096 | open | A 4 KiB floor under the OS-reported logical and physical block when choosing I/O alignment; no cited or measured basis. |
| `crates/mantle/src/bench.rs:101` Plan::standard volume cap 4 GiB | open | research/11 §16.3 and §18.11 say the scratch volume should come from the measured write-cache size; none is measured. |
| `crates/mantle/src/bench.rs` Plan::standard free / 10 | bound | The tenth-of-free-space cap guards the user's disk; research/11 §16.3 names it a safety cap. |
| `crates/mantle/src/bench.rs:105` Plan::standard sizes | open | Chunk sizes 4K, 64K, 1M, 8M; research/11 §16.3 asks for the design's chunk-size distribution. |
| `crates/mantle/src/bench.rs:106` Plan::standard workers | open | Concurrency {1, 4, 16, 64}; research/11 §16.2 asks for depth 1, the measured N* and a point past saturation. |
| `crates/mantle/src/bench.rs:298` run max_fragments | open | Index room for a third of the volume in the smallest chunks, twice over, at least 1024; the margin and the floor lack a derivation. |
| `crates/mantle/src/bench.rs:346` run fill_size and fill workers 8 | open | The first pass fills with 1 MiB chunks from eight writers; neither is derived. |
| `crates/mantle/src/bench.rs:366` run budget | open | A put round writes at most a third of the volume; the fraction lacks a derivation. |
| `crates/mantle/src/bench_log.rs:58` sizes and replicas | open | Entry sizes 128 B, 1 KiB, 16 KiB and replicas 1–256 are recorded in the raft-log benchmark without a derivation. |
| `crates/mantle/src/bench_log.rs:107` log Config | open | 16 MiB segments, 64 segments, 2^20 entries and 1 GiB per group, 64 KiB cache, queue of twice the replicas and 1 GiB: set above use, not derived. |
| `crates/mantle/src/bench_log.rs:54` log alignment floor 4096 | open | As the chunk benchmark's floor. |
| `crates/mantle/src/bench_meta.rs:96` at least 5 runs | open | The minimum lacks a statistical derivation. |
| `crates/mantle/src/bench_meta.rs:124` parts, commands, ranges, pages, extents | open | Ladders ending at the production limits they exercise (10,000 parts is S3's); the ladders themselves are not derived. |
| `crates/mantle/src/bench_meta.rs:283` Rules and coordinator bounds | open | 16 sessions and expiries, a probe budget of 64 rows, 2^20 ranges, 1 MiB extents: not derived. |
| `crates/mantle/src/disk.rs:158` measured alignment floor 4096 | open | As the chunk benchmark's floor. |
| `crates/mantle/src/main.rs` Durability target 1e-11 | external | S3's designed 99.999999999% annual durability; docs/design/durability.md §1, research/15 §5. |
| `crates/mantle/src/main.rs` bench rounds default | cited | `Policy::STANDARD.max` = 30, from docs/research/21 §9.5 (crates/disk/src/rounds.rs). |
| `crates/mantle/src/main.rs:91` bench step seconds (chunk 1, log 1, ec 0.5, hash 0.5, meta 0.5) | open | research/11 §16.3 asks for step length from the samples the reported quantiles need and a steady-state window. |
| `crates/mantle/src/main.rs:296` ec and hash size ladders | open | 64K–8M chunk and 8K–8M buffer ladders; not derived from the design's chunk and part distributions. |
| `crates/chunk/src/layout.rs:34` Limits default batch_requests 1,024 | open | Audit §12.6: derive n* = F0/c_r or a latency-bounded version from measured per-batch (b, n, S) (research 11 §5.3). |
| `crates/chunk/src/layout.rs:35` Limits default batch_bytes 32 MiB | open | Research 11 §5 gives b* = F0·BW, which does not set it; audit §12.6 asks for a derivation from a measured T(b). |
| `crates/chunk/src/layout.rs:36` Limits default fragments_per_chunk 4,096 | open | Research 11 §6.2 finds no source; derive from read amplification f ≤ ε·size/(BW·t_frag) with a measured t_frag. |
| `crates/chunk/src/layout.rs` Reads default | bound | One read at a time, no byte allowance and no gap for a device not measured, the conservative defaults of CLAUDE.md rule 5; measured values come from calibration. |
| `crates/chunk/src/layout.rs` Config default segment_size 256 MiB | cited | A 256 MiB host-managed SMR zone, AWK+19 §3.3 (research 11 §7.2); audit §12.6 asks for zone geometry read where there is one and the tradeoffs measured elsewhere. |
| `crates/chunk/src/layout.rs` Config default checksum_shift 16 | cited | 64 KiB checksum block from GFS, SOSP 2003 §5.2 (research 11 §8). |
| `crates/chunk/src/layout.rs:172` Config default max_fragments 2^22 | open | Research 11 §6.2 finds no source; derive from capacity, heap per entry, checkpoint bandwidth and restart deadline (audit §12.6). |
| `crates/chunk/src/layout.rs` Config default scrub_period 7 days | cited | Field practice of one or two weeks (SDG10 §5; Ceph weekly, AWK+19 §5.1; research 03 §15.9); research 11 §12.4 notes it is practice, not a derivation. |
| `crates/disk/src/calibrate.rs:72` Rounds STANDARD min 3, max 6, precision ±5% | open | The confidence level, tolerance and round counts are chosen, not cited or derived from a startup budget. |
| `crates/disk/src/calibrate.rs` Plan standard small | derived | Device alignment with a 4 KiB floor, the flash alignment unit of Haas and Leis, PVLDB 2023 §2.2. |
| `crates/disk/src/calibrate.rs:99` Plan standard span, step, depth ladders, large, durable_writes | open | Research 11 §13.4 asks for sizes from quantile sample counts, SNIA steady state, scratch and wear budgets and a transfer-size sweep. |
| `crates/disk/src/calibrate.rs` Plan standard max_read_depth 65,535 | external | NVMe's 16-bit zero-based CAP.MQES less one slot (NVM Express Base Specification). |
| `crates/disk/src/calibrate.rs:108` Plan standard durable_large 32 MiB | open | A copy of the writer's batch cap, so it cannot establish that cap (research 11 §5.1, audit §12.6). |
| `crates/disk/src/rounds.rs` Policy STANDARD min 10 | derived | The fewest rounds where the exact runs test has a two-sided 5% rejection region (research 21 §4; measurement.md §5). |
| `crates/disk/src/rounds.rs:88` Policy STANDARD max 30, precision ±5% | open | The one-fifth minority share it resolves and the ±5% resolution are choices with no source. |

## What the open ones need

- The chunk writer's batch shape (`Limits`: 1,024 requests, 32 MiB, 4,096 fragments a chunk)
  and the index's 2^22 fragments: the derivations research 11 §5–§6 sets out, from a
  measured `T(bytes, records, depth)`, heap per entry, checkpoint bandwidth and a restart
  deadline. Calibration's `durable_large` copies the batch cap it should establish.
- Calibration's statistics (`P50_SAMPLES`, `P99_SAMPLES`, the rounds' counts and ±5%
  precision, the histogram's resolution and range, `commit::RATE_ONE`): a confidence level
  and tolerance from the service objective they serve, or from a cited source, and a round
  budget from the time a startup may take.
- `measure::MAX_DEPTH`: an executor credit from the node's CPU, memory and thread allowance.
- `replica::DRIVE_BUDGET`: the node's scheduler (audit §5.1), which shares work among its
  ranges by deadline.
- A bucket policy's raw body limit (`policy::BODY_LIMIT`): a recording of how much white space
  S3 keeps in a policy it gives back as set.
- The benchmarks' ladders and step lengths: the regimes each exercises, in their
  measurement records, or step lengths from the samples their quantiles need
  (research 11 §16).
