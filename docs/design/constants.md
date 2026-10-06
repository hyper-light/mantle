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

The shared crates mantle vendors from hyper-raft (`vendor/UPSTREAM.md`) are not mantle's
production code here: the Raft log (`vendor/hyper-log`) and the block layer under it and the
chunk store (`vendor/hyper-block`), with their format constants, the queue's pipeline depth,
the process thread budget's ceilings and `UNDESCRIBED_QUEUE_DEPTH`. hyper-raft's rules give
every one of their constants its derivation or citation where it is defined.

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
| `crates/disk/src/calibrate.rs` `P50_SAMPLES` | open | 16 from n ≥ 1.96²q(1−q)/δ² at δ = (1−q)/2 (research 11 §13.3); the 95% level and the δ rule are chosen, not cited. |
| `crates/disk/src/calibrate.rs` `P99_SAMPLES` | open | 1,522 from the same binomial normal approximation; the 95% confidence and δ = (1−q)/2 tolerance have no cited basis. |
| `crates/disk/src/histogram.rs` `BUCKETS` | derived | OVERFLOW + 1. |
| `crates/disk/src/histogram.rs` `MAX_EXP` | open | Rows up to 2^41 ns (about 37 minutes); research 11 §14.3 says the top should be the longest latency a consumer acts on. |
| `crates/disk/src/histogram.rs` `OVERFLOW` | derived | ROWS × SUB, the overflow bucket's index. |
| `crates/disk/src/histogram.rs` `ROWS` | derived | (MAX_EXP − SUB_BITS) + 2 rows. |
| `crates/disk/src/histogram.rs` `SUB` | derived | 2^SUB_BITS sub-buckets per row. |
| `crates/disk/src/histogram.rs` `SUB_BITS` | open | 5 bits (3.1% error) by research 11 §14.3's α ≤ ε/2 rule, but ε = ±10% is an example resolution, not measured or cited. |
| `crates/disk/src/histogram.rs` `SUB_MASK` | derived | SUB − 1 as a bit mask. |
| `crates/disk/src/probe/linux.rs` `MAX_DEPTH` | bound | Stack bound on the recursive sysfs walk, past the three-deep dm-crypt, LVM and md stack; a deeper composite's medium is left unknown, never guessed (CLAUDE.md rule 5). |
| `crates/disk/src/probe/macos.rs` `CF_NUMBER_SINT64` | external | kCFNumberSInt64Type = 4, CoreFoundation CFNumber.h. |
| `crates/disk/src/probe/macos.rs` `ITERATE_PARENTS` | external | kIORegistryIterateParents = 0x2, IOKit IOKitKeys.h. |
| `crates/disk/src/probe/macos.rs` `ITERATE_RECURSIVELY` | external | kIORegistryIterateRecursively = 0x1, IOKit IOKitKeys.h. |
| `crates/disk/src/probe/macos.rs` `MAIN_PORT` | external | kIOMainPortDefault = 0, IOKit. |
| `crates/disk/src/probe/macos.rs` `MAX_STRING` | bound | 256-byte buffer for IOKit string properties, far above IOKit names and model strings; a longer one fails without writing. |
| `crates/disk/src/probe/macos.rs` `UTF8` | external | kCFStringEncodingUTF8 (0x08000100), CoreFoundation CFString.h. |
| `crates/disk/src/probe/windows.rs` `VOLUME_GUID_CHARS` | external | 50 characters, the size GetVolumeNameForVolumeMountPointW's documentation gives for the largest volume GUID path (Microsoft Learn). |
| `crates/disk/src/rounds.rs` `MAX_ROUNDS` | bound | 120 rounds, the most for which the exact binomial and runs-test sums fit in u128 (40 · 2^n). |
| `crates/ec/src/durability.rs` `DISK_FAILURES` | cited | 6.3% a year: the highest per-model annualized failure rate in Backblaze's 2025 Drive Stats (Toshiba MG08ACA16TEY); research/15 §8.1, design/durability.md §5. |
| `crates/ec/src/durability.rs` `FLASH_FAILURES` | cited | 2.7% a year: Schroeder et al. FAST 2016 Table 5's worst four-year replacement fraction, 10.31%, as a constant hazard; research/15 §8.2. |
| `crates/ec/src/durability.rs` `POWER_LOSSES` | cited | One node-losing power-on restart a year, Cidon et al. ATC 2013 ("once or twice per year"); UNVERIFIED at its own cited source (Chansler 2012), so the least-qualified field input, until mantle's node history of restarts replaces it; research/15 §8.4. |
| `crates/ec/src/durability.rs` `POWER_LOSS_FRACTION` | cited | 1% of nodes: the upper end of HDFS's "one-half to one percent of the nodes will not survive a full power-on restart" (Shvachko et al. MSST 2010); research/15 §8.4. |
| `crates/ec/src/durability.rs` `MAX_SQUARINGS` | bound | 1100 halvings of Λt: a finite double is below 2¹⁰²⁴, so no finite Λt needs more to reach ½; research/15 §4.6. |
| `crates/ec/src/durability.rs` `MAX_TERMS` | bound | 170 series terms at Λτ ≤ ½: past it the tail bound 2·(½)^(K+1)/(K+1)! is below every positive double (170! is the largest factorial a double holds); research/15 §4.6. |
| `crates/ec/src/durability.rs` `MOST_COPIES` | cited | Three copies, the replication of a block still being written; research/04 §R1.1. |
| `crates/ec/src/durability.rs` `YEAR` | derived | Unit conversion: 365.25 days × 24 h = 8766 hours. |
| `crates/engine/src/table/format.rs` `CONTEXT_CHECKSUM_FORMAT_VERSION` | format | The first block-based table format_version whose block and footer checksums carry the context modifier [R table/format.h:218-220] (docs/research/24 §1.2). |
| `crates/engine/src/table/format.rs` `LAST_BYTE_PRIME` | format | `kRandomPrime` 0x6b9083d9 of `ModifyChecksumForLastByte`, folding a block's compression-type byte into its XXH3 checksum [R table/format.cc:606-612] (docs/research/24 §1.2). |
| `crates/engine/src/util/coding.rs` `CONTINUATION` | format | 0x80, the LEB128 varint's continuation bit [R util/coding.cc:27] (docs/research/24 §1.1). |
| `crates/engine/src/util/coding.rs` `MAX_VARINT32_LENGTH` | format | 5 bytes, the longest varint32: 32 bits at 7 per byte [R util/coding.h:156] (docs/research/24 §1.1). |
| `crates/engine/src/util/coding.rs` `MAX_VARINT64_LENGTH` | format | 10 bytes, `kMaxVarint64Length` [R util/coding.h:36] (docs/research/24 §1.1). |
| `crates/engine/src/util/coding.rs` `PAYLOAD` | format | 0x7F, the 7 payload bits of a varint byte [R util/coding.cc:63]. |
| `crates/engine/src/util/coding.rs` `VARINT32_LAST_BYTE_MAX` | derived | 0x0F: the 4 bits a fifth varint32 byte can carry after 28; larger values are refused where RocksDB drops the bits (docs/research/24 §1.1 DECISION). |
| `crates/engine/src/util/coding.rs` `VARINT64_LAST_BYTE_MAX` | derived | 0x01: the 1 bit a tenth varint64 byte can carry after 63; larger values are refused (docs/research/24 §1.1 DECISION). |
| `crates/engine/src/util/crc32c.rs` `MASK_DELTA` | format | `kMaskDelta` 0xa282ead8, added to a stored CRC after rotating it [R util/crc32c.h:37] (docs/research/24 §1.2). |
| `crates/engine/src/util/crc32c.rs` `MASK_ROTATION` | format | 15, the right rotation of `crc32c::Mask` [R util/crc32c.h:44-47]. |
| `crates/engine/src/util/hash.rs` `AVALANCHE` | format | 0x165667919E3779F9, XXH3's avalanche multiplier, in `BijectiveHash2x64` [R util/hash.cc:132-137]; shapes the SST unique ID [R table/unique_id.cc]. |
| `crates/engine/src/util/hash.rs` `AVALANCHE_INVERSE` | derived | 0x8da8ee41d6df849, the inverse of `AVALANCHE` modulo 2^64 [R util/hash.cc:141]. |
| `crates/engine/src/util/hash.rs` `BITFLIP_HIGH` | format | 0xc202797692d63d58, the part of XXH3's secret `BijectiveHash2x64` adds the seed to [R util/hash.cc:152]. |
| `crates/engine/src/util/hash.rs` `BITFLIP_LOW` | format | 0x59973f0033362349, the part of XXH3's secret `BijectiveHash2x64` subtracts the seed from [R util/hash.cc:151]. |
| `crates/engine/src/util/hash.rs` `BLOOM_HASH_SEED` | format | 0xbc9f1d34, `BloomHash`'s seed, keying the legacy Bloom filter [R util/hash.h:93-95] (docs/research/24 §1.2, §5 R2). |
| `crates/engine/src/util/hash.rs` `HIGH_WORD` | derived | 0xFFFFFFFF00000000, the high 32 bits of a 64-bit word, in `BijectiveUnhash2x64` [R util/hash.cc:184-185]. |
| `crates/engine/src/util/hash.rs` `LEN16_MARK` | format | 0x3c0000000000000 = (16 - 1) << 54, XXH3's length term for a 16-byte input [R util/hash.cc:157]. |
| `crates/engine/src/util/hash.rs` `MURMUR_M` | format | 0xc6a4a793, `Hash`'s multiplier [R util/hash.cc:29] (docs/research/24 §1.2). |
| `crates/engine/src/util/hash.rs` `MURMUR_R` | format | 24, `Hash`'s final shift [R util/hash.cc:30]. |
| `crates/engine/src/util/hash.rs` `PRIME32_2_INVERSE` | derived | 0xb6c92f47, the inverse of 0x85EBCA77 modulo 2^32 [R util/hash.cc:182]. |
| `crates/engine/src/util/hash.rs` `PRIME32_2_MINUS_1` | format | 0x85EBCA76, XXH3's PRIME32_2 - 1 as `BijectiveHash2x64` uses it [R util/hash.cc:159]. |
| `crates/engine/src/util/hash.rs` `PRIME64_1` | format | 0x9E3779B185EBCA87, XXH3's PRIME64_1 [R util/hash.cc:154]. |
| `crates/engine/src/util/hash.rs` `PRIME64_1_INVERSE` | derived | 0x887493432badb37, the inverse of `PRIME64_1` modulo 2^64 [R util/hash.cc:180]. |
| `crates/engine/src/util/hash.rs` `PRIME64_2` | format | 0xC2B2AE3D27D4EB4F, XXH3's PRIME64_2 [R util/hash.cc:161]. |
| `crates/engine/src/util/hash.rs` `PRIME64_2_INVERSE` | derived | 0xba79078168d4baf, the inverse of `PRIME64_2` modulo 2^64 [R util/hash.cc:175]. |
| `crates/engine/src/util/hash.rs` `SLICE_HASH_SEED` | format | 397, `GetSliceHash`'s seed, keying the data-block hash index [R util/hash.h:121-123] (docs/research/24 §1.2, §5 R2). |
| `crates/engine/src/util/prefix_varint.rs` `MAX_PREFIX_VARINT32_LENGTH` | format | 5 bytes, `kMaxPrefixVarint32Length` [R util/prefix_varint.h:58]. |
| `crates/engine/src/util/prefix_varint.rs` `MAX_PREFIX_VARINT64_LENGTH` | format | 9 bytes, `kMaxPrefixVarint64Length`: a zero byte and a fixed64 [R util/prefix_varint.h:65]. |
| `crates/engine/src/util/xxph3.rs` `BLOCK_LEN` | derived | `STRIPE_LEN × STRIPES_PER_BLOCK` = 1024 bytes between scrambles [R util/xxph3.h:1520]. |
| `crates/engine/src/util/xxph3.rs` `MIDSIZE_LASTOFFSET` | format | 17, the secret offset back from `SECRET_SIZE_MIN` for the mid-size path's last 16 bytes [R util/xxph3.h:1690] (docs/research/24 §5 R1). |
| `crates/engine/src/util/xxph3.rs` `MIDSIZE_MAX` | format | 240, the longest input on XXPH3's mid-size path [R util/xxph3.h:1679]. |
| `crates/engine/src/util/xxph3.rs` `MIDSIZE_STARTOFFSET` | format | 3, the secret offset of the mid-size path's rounds after the eighth [R util/xxph3.h:1689]. |
| `crates/engine/src/util/xxph3.rs` `PRIME32_1` | format | 0x9E3779B1, xxHash's PRIME32_1 [R util/xxph3.h:564]. |
| `crates/engine/src/util/xxph3.rs` `PRIME32_2` | format | 0x85EBCA77, xxHash's PRIME32_2 [R util/xxph3.h:565]. |
| `crates/engine/src/util/xxph3.rs` `PRIME32_3` | format | 0xC2B2AE3D, xxHash's PRIME32_3 [R util/xxph3.h:566]. |
| `crates/engine/src/util/xxph3.rs` `PRIME64_1` | format | 0x9E3779B185EBCA87, xxHash's PRIME64_1 [R util/xxph3.h:642]. |
| `crates/engine/src/util/xxph3.rs` `PRIME64_2` | format | 0xC2B2AE3D27D4EB4F, xxHash's PRIME64_2 [R util/xxph3.h:643]. |
| `crates/engine/src/util/xxph3.rs` `PRIME64_3` | format | 0x165667B19E3779F9, xxHash's PRIME64_3 [R util/xxph3.h:644]. |
| `crates/engine/src/util/xxph3.rs` `PRIME64_4` | format | 0x85EBCA77C2B2AE63, xxHash's PRIME64_4 [R util/xxph3.h:645]. |
| `crates/engine/src/util/xxph3.rs` `PRIME64_5` | format | 0x27D4EB2F165667C5, xxHash's PRIME64_5 [R util/xxph3.h:646]. |
| `crates/engine/src/util/xxph3.rs` `SECRET_CONSUME_RATE` | format | 8 secret bytes consumed per stripe [R util/xxph3.h:1146]. |
| `crates/engine/src/util/xxph3.rs` `SECRET_DEFAULT_SIZE` | format | 192, the length of XXPH3's default secret `kSecret` [R util/xxph3.h:914-935]; the secret's bytes are in `SECRET` beside it (docs/research/24 §1.19). |
| `crates/engine/src/util/xxph3.rs` `SECRET_LASTACC_START` | format | 7, the secret offset of the long path's last stripe [R util/xxph3.h:1541]. |
| `crates/engine/src/util/xxph3.rs` `SECRET_MERGEACCS_START` | format | 11, the secret offset of the accumulators' merge [R util/xxph3.h:1580]. |
| `crates/engine/src/util/xxph3.rs` `SECRET_SIZE_MIN` | format | 136, `XXPH3_SECRET_SIZE_MIN`, the base of the mid-size path's last offset [R util/xxph3.h:283]. |
| `crates/engine/src/util/xxph3.rs` `STRIPES_PER_BLOCK` | derived | `(SECRET_DEFAULT_SIZE - STRIPE_LEN) / SECRET_CONSUME_RATE` = 16 stripes per block [R util/xxph3.h:1519]. |
| `crates/engine/src/util/xxph3.rs` `STRIPE_LEN` | format | 64 bytes hashed per accumulation [R util/xxph3.h:1145]. |
| `crates/engine/src/util/xxhash.rs` `BLOCK_LEN` | derived | `STRIPE_LEN × STRIPES_PER_BLOCK` = 1024 bytes between scrambles [R util/xxhash.h:5136-5137]. |
| `crates/engine/src/util/xxhash.rs` `MIDSIZE_LASTOFFSET` | format | 17, the secret offset back from `SECRET_SIZE_MIN` for the mid-size path's last 16 bytes [R util/xxhash.h:4098]. |
| `crates/engine/src/util/xxhash.rs` `MIDSIZE_MAX` | format | 240, the longest input on XXH3's mid-size path [R util/xxhash.h:4087]. |
| `crates/engine/src/util/xxhash.rs` `MIDSIZE_STARTOFFSET` | format | 3, the secret offset of the mid-size path's rounds after the eighth [R util/xxhash.h:4097]. |
| `crates/engine/src/util/xxhash.rs` `PRIME32_1` | format | 0x9E3779B1, xxHash's PRIME32_1 [R util/xxhash.h:2219]. |
| `crates/engine/src/util/xxhash.rs` `PRIME32_2` | format | 0x85EBCA77, xxHash's PRIME32_2 [R util/xxhash.h:2220]. |
| `crates/engine/src/util/xxhash.rs` `PRIME32_3` | format | 0xC2B2AE3D, xxHash's PRIME32_3 [R util/xxhash.h:2221]. |
| `crates/engine/src/util/xxhash.rs` `PRIME32_4` | format | 0x27D4EB2F, xxHash's PRIME32_4 [R util/xxhash.h:2222]. |
| `crates/engine/src/util/xxhash.rs` `PRIME32_5` | format | 0x165667B1, xxHash's PRIME32_5 [R util/xxhash.h:2223]. |
| `crates/engine/src/util/xxhash.rs` `PRIME64_1` | format | 0x9E3779B185EBCA87, xxHash's PRIME64_1 [R util/xxhash.h:2749]. |
| `crates/engine/src/util/xxhash.rs` `PRIME64_2` | format | 0xC2B2AE3D27D4EB4F, xxHash's PRIME64_2 [R util/xxhash.h:2750]. |
| `crates/engine/src/util/xxhash.rs` `PRIME64_3` | format | 0x165667B19E3779F9, xxHash's PRIME64_3 [R util/xxhash.h:2751]. |
| `crates/engine/src/util/xxhash.rs` `PRIME64_4` | format | 0x85EBCA77C2B2AE63, xxHash's PRIME64_4 [R util/xxhash.h:2752]. |
| `crates/engine/src/util/xxhash.rs` `PRIME64_5` | format | 0x27D4EB2F165667C5, xxHash's PRIME64_5 [R util/xxhash.h:2753]. |
| `crates/engine/src/util/xxhash.rs` `PRIME_MX1` | format | 0x165667919E3779F9, `XXH3_avalanche`'s multiplier [R util/xxhash.h:3870]. |
| `crates/engine/src/util/xxhash.rs` `PRIME_MX2` | format | 0x9FB21C651E98DF25, `XXH3_rrmxmx`'s multiplier [R util/xxhash.h:3884], also `XXH3_len_4to8_128b`'s [R util/xxhash.h:5786]. |
| `crates/engine/src/util/xxhash.rs` `SECRET_CONSUME_RATE` | format | 8 secret bytes consumed per stripe [R util/xxhash.h:4152]. |
| `crates/engine/src/util/xxhash.rs` `SECRET_LASTACC_START` | format | 7, the secret offset of the long path's last stripe [R util/xxhash.h:5157]. |
| `crates/engine/src/util/xxhash.rs` `SECRET_MERGEACCS_START` | format | 11, the secret offset of the accumulators' merge [R util/xxhash.h:5213]. |
| `crates/engine/src/util/xxhash.rs` `SECRET_SIZE_MIN` | format | 136, `XXH3_SECRET_SIZE_MIN`, the base of the mid-size path's last offset [R util/xxhash.h:968]. |
| `crates/engine/src/util/xxhash.rs` `STRIPE32` | format | 16 bytes, XXH32's four 4-byte lanes [R util/xxhash.h:2415-2445]. |
| `crates/engine/src/util/xxhash.rs` `STRIPE64` | format | 32 bytes, XXH64's four 8-byte lanes [R util/xxhash.h:2854-2885]. |
| `crates/engine/src/util/xxhash.rs` `STRIPES_PER_BLOCK` | derived | `(SECRET_DEFAULT_SIZE - STRIPE_LEN) / SECRET_CONSUME_RATE` = 16 stripes per block [R util/xxhash.h:5136]. |
| `crates/engine/src/util/xxhash.rs` `STRIPE_LEN` | format | 64 bytes hashed per accumulation [R util/xxhash.h:4151]. |
| `crates/engine/src/db/dbformat.rs` `DISABLE_GLOBAL_SEQUENCE_NUMBER` | format | `kDisableGlobalSequenceNumber`, u64::MAX: an ingested file with no global sequence number [R db/dbformat.h:131-132]. |
| `crates/engine/src/db/dbformat.rs` `MAX_SEQUENCE_NUMBER` | format | `kMaxSequenceNumber`, 2^56 − 1: the trailer's 56 bits above the type byte [R db/dbformat.h:129]. |
| `crates/engine/src/db/dbformat.rs` `NUM_INTERNAL_BYTES` | format | `kNumInternalBytes`, 8: the internal key's trailer [R db/dbformat.h:134]. |
| `crates/engine/src/db/dbformat.rs` `RANGE_TOMBSTONE_SENTINEL` | format | `kRangeTombstoneSentinel`: `Pack(kMaxSequenceNumber, kTypeRangeDeletion)` [R db/dbformat.h:201-202]. |
| `crates/engine/src/db/dbformat.rs` `TYPE_BITS` | format | 8: the type's width in the packed trailer [R db/dbformat.h:129]. |
| `crates/engine/src/db/memtable.rs` `ARENA_BLOCK_ALIGN` | cited | 4 KiB, the alignment `SanitizeOptions` rounds a derived arena block up to [R db/column_family.cc:245-248]. |
| `crates/engine/src/db/memtable.rs` `DEFAULT_WRITE_BUFFER_SIZE` | cited | 64 MiB, `write_buffer_size`'s default [R include/rocksdb/options.h:191]. |
| `crates/engine/src/db/memtable.rs` `HEADER` | format | 8: the header word before a memtable entry's internal key (the port's node layout, memtable/inlineskiplist.rs). |
| `crates/engine/src/db/memtable.rs` `MAX_DERIVED_ARENA_BLOCK_SIZE` | cited | 1 MiB, the largest arena block `SanitizeOptions` derives from the write buffer size [R db/column_family.cc:241-243]. |
| `crates/engine/src/db/memtable.rs` `OVER_ALLOCATION_DENOMINATOR` | cited | 5: `kAllowOverAllocationRatio` = 0.6 as 3/5 [R db/memtable.cc:316], so the flush test is integer arithmetic. |
| `crates/engine/src/db/memtable.rs` `OVER_ALLOCATION_NUMERATOR` | cited | 3: as `OVER_ALLOCATION_DENOMINATOR`. |
| `crates/engine/src/db/memtable.rs` `PACKED_WRITE_TIME` | format | 8: the fixed64 write time ending a `kTypeValuePreferredSeqno` value [R db/seqno_to_time_mapping.cc:567-570]. |
| `crates/engine/src/db/wide/wide_column_serialization.rs` `VERSION1` | format | `kVersion1`: inline values only [R db/wide/wide_column_serialization.h:124]. |
| `crates/engine/src/db/wide/wide_column_serialization.rs` `VERSION2` | format | `kVersion2`: columns may be blob references [R db/wide/wide_column_serialization.h:125]. |
| `crates/engine/src/db/write_batch.rs` `COUNT_OFFSET` | format | 8: the count follows the header's fixed64 sequence number [R db/write_batch_internal.h:81]. |
| `crates/engine/src/db/write_batch.rs` `DEFERRED` | format | `ContentFlags::DEFERRED` [R db/write_batch.cc:80-95]. |
| `crates/engine/src/db/write_batch.rs` `HAS_BEGIN_PREPARE` | format | `ContentFlags::HAS_BEGIN_PREPARE` [R db/write_batch.cc:80-95]. |
| `crates/engine/src/db/write_batch.rs` `HAS_BEGIN_UNPREPARE` | format | `ContentFlags::HAS_BEGIN_UNPREPARE` [R db/write_batch.cc:80-95]. |
| `crates/engine/src/db/write_batch.rs` `HAS_BLOB_INDEX` | format | `ContentFlags::HAS_BLOB_INDEX` [R db/write_batch.cc:80-95]. |
| `crates/engine/src/db/write_batch.rs` `HAS_COMMIT` | format | `ContentFlags::HAS_COMMIT` [R db/write_batch.cc:80-95]. |
| `crates/engine/src/db/write_batch.rs` `HAS_DELETE` | format | `ContentFlags::HAS_DELETE` [R db/write_batch.cc:80-95]. |
| `crates/engine/src/db/write_batch.rs` `HAS_DELETE_RANGE` | format | `ContentFlags::HAS_DELETE_RANGE` [R db/write_batch.cc:80-95]. |
| `crates/engine/src/db/write_batch.rs` `HAS_END_PREPARE` | format | `ContentFlags::HAS_END_PREPARE` [R db/write_batch.cc:80-95]. |
| `crates/engine/src/db/write_batch.rs` `HAS_MERGE` | format | `ContentFlags::HAS_MERGE` [R db/write_batch.cc:80-95]. |
| `crates/engine/src/db/write_batch.rs` `HAS_PUT` | format | `ContentFlags::HAS_PUT` [R db/write_batch.cc:80-95]. |
| `crates/engine/src/db/write_batch.rs` `HAS_PUT_ENTITY` | format | `ContentFlags::HAS_PUT_ENTITY` [R db/write_batch.cc:80-95]. |
| `crates/engine/src/db/write_batch.rs` `HAS_ROLLBACK` | format | `ContentFlags::HAS_ROLLBACK` [R db/write_batch.cc:80-95]. |
| `crates/engine/src/db/write_batch.rs` `HAS_SINGLE_DELETE` | format | `ContentFlags::HAS_SINGLE_DELETE` [R db/write_batch.cc:80-95]. |
| `crates/engine/src/db/write_batch.rs` `HAS_TIMED_PUT` | format | `ContentFlags::HAS_TIMED_PUT` [R db/write_batch.cc:80-95]. |
| `crates/engine/src/db/write_batch.rs` `HEADER` | format | 12: `WriteBatchInternal::kHeader`, the fixed64 sequence and fixed32 count [R db/write_batch_internal.h:81]. |
| `crates/engine/src/db/write_batch.rs` `MAX_KEY_SIZE` | format | `kMaxWriteBatchKeySize`: u32::MAX less the 8-byte trailer, so a memtable entry's varint32 length holds key and trailer [R db/write_batch.cc:99-100]. |
| `crates/engine/src/db/write_batch.rs` `MAX_SAVE_POINTS` | bound | 2^16 save points at once (1.5 MiB of offsets): RocksDB's stack is unbounded; mantle's writes set none, and the bound passes any nesting a caller builds; past it `set_save_point` refuses (CLAUDE.md §2). |
| `crates/engine/src/db/write_batch.rs` `MAX_VALUE_SIZE` | format | u32::MAX: a value, operand, entity or end key's length is a varint32 [R db/write_batch.cc:863-865]. |
| `crates/engine/src/memory/arena.rs` `ALIGN_UNIT` | derived | `WORD`: `allocate_aligned` aligns to one storage word. |
| `crates/engine/src/memory/arena.rs` `DIRECTORY_SEGMENTS` | derived | 29: 64 − `OFFSET_BITS` + 1, a segment of 2^s slots for each bit of the largest block number an address carries, and one for block 0, so the directory holds exactly the blocks an address names. |
| `crates/engine/src/memory/arena.rs` `INLINE_SIZE` | cited | 2 KiB, `Arena::kInlineSize` [R memory/arena.h:31]. |
| `crates/engine/src/memory/arena.rs` `MAX_BLOCK_SIZE` | cited | 2 GiB, `Arena::kMaxBlockSize` [R memory/arena.h:33]. |
| `crates/engine/src/memory/arena.rs` `MIN_BLOCK_SIZE` | cited | 4 KiB, `Arena::kMinBlockSize` [R memory/arena.h:32]. |
| `crates/engine/src/memory/arena.rs` `OFFSET_BITS` | derived | 36: a 64 GiB offset, above the largest allocation the memtable makes (a key and a value each under 4 GiB, with their headers; docs/research/24 §1.4). |
| `crates/engine/src/memory/arena.rs` `SLOT_BYTES` | derived | The size of a directory slot, the bytes RocksDB counts per block as `sizeof(char*)`. |
| `crates/engine/src/memory/arena.rs` `WORD` | derived | 8: the size of the `AtomicU64` a block stores. |
| `crates/engine/src/memtable/inlineskiplist.rs` `DEFAULT_BRANCHING_FACTOR` | cited | 4: the default `branching_factor` [R memtable/inlineskiplist.h:76-78], Pugh's recommended p = 1/4 (CACM 1990, §4). |
| `crates/engine/src/memtable/inlineskiplist.rs` `DEFAULT_MAX_HEIGHT` | cited | 12: the default `max_height` [R memtable/inlineskiplist.h:76-78]. |
| `crates/engine/src/memtable/inlineskiplist.rs` `GOLDEN_GAMMA` | external | SplitMix64's increment (Steele, Lea and Flood, OOPSLA 2014). |
| `crates/engine/src/memtable/inlineskiplist.rs` `HEAD` | format | u64::MAX − 1: the head's node number, outside every arena address. |
| `crates/engine/src/memtable/inlineskiplist.rs` `HEIGHT_BITS` | format | 8: the header word's bits below the key length that hold the height. |
| `crates/engine/src/memtable/inlineskiplist.rs` `HEIGHT_MASK` | derived | 0xFF: the low `HEIGHT_BITS` of the header word. |
| `crates/engine/src/memtable/inlineskiplist.rs` `MAX_POSSIBLE_HEIGHT` | cited | 32, `kMaxPossibleHeight` [R memtable/inlineskiplist.h:70]. |
| `crates/engine/src/memtable/inlineskiplist.rs` `MIX_1` | external | SplitMix64's first finalizer multiplier (Steele, Lea and Flood, OOPSLA 2014). |
| `crates/engine/src/memtable/inlineskiplist.rs` `MIX_2` | external | SplitMix64's second finalizer multiplier (Steele, Lea and Flood, OOPSLA 2014). |
| `crates/engine/src/memtable/inlineskiplist.rs` `NIL` | format | u64::MAX: no node, the end of a level. |
| `crates/engine/src/memtable/inlineskiplist.rs` `WORD` | derived | 8: the arena's storage word. |
| `crates/engine/src/port/mmap.rs` `WORD` | derived | The size of `AtomicU64`, the unit a mapping is counted in. |
| `crates/engine/src/version.rs` `PLACE` | format | 1000: the decimal places each of the minor and patch numbers takes in `ROCKSDB_VERSION_INT` [R include/rocksdb/version.h:24-25]. |
| `crates/engine/src/version.rs` `ROCKSDB_MAJOR` | format | 11 [R include/rocksdb/version.h:14]: the release the port converts. |
| `crates/engine/src/version.rs` `ROCKSDB_MINOR` | format | 8 [R include/rocksdb/version.h:15]. |
| `crates/engine/src/version.rs` `ROCKSDB_PATCH` | format | 1 [R include/rocksdb/version.h:16]. |
| `crates/engine/src/version.rs` `ROCKSDB_VERSION_INT` | derived | `ROCKSDB_VERSION_INT`: major·10^6 + minor·10^3 + patch [R include/rocksdb/version.h:24-25]. |
| `crates/engine/src/codec/zstd/decoder.rs` `BLOCK_HEADER` | format | 3 bytes, a block header (RFC 8878 §3.1.1.2, Table 8). |
| `crates/engine/src/codec/zstd/decoder.rs` `BLOCK_MAX` | format | 128 KB, the largest block decoded or compressed: Block_Maximum_Size is the smaller of Window_Size and 128 KB (RFC 8878 §3.1.1.2.4). |
| `crates/engine/src/codec/zstd/decoder.rs` `DICTIONARY_MAGIC` | format | 0xEC30A437, a formatted dictionary's magic number (RFC 8878 §5). |
| `crates/engine/src/codec/zstd/decoder.rs` `FRAME_MAGIC` | format | 0xFD2FB528, a Zstandard frame's magic number (RFC 8878 §3.1.1). |
| `crates/engine/src/codec/zstd/decoder.rs` `RAW_DICTIONARY_MIN` | format | 8 bytes, the smallest raw-content dictionary (RFC 8878 §5). |
| `crates/engine/src/codec/zstd/decoder.rs` `SKIPPABLE_MAGIC` | format | 0x184D2A50, the first of the 16 skippable frames' magic numbers 0x184D2A50 to 0x184D2A5F (RFC 8878 §3.1.2). |
| `crates/engine/src/codec/zstd/decoder.rs` `SKIPPABLE_MASK` | format | 0xFFFFFFF0, the bits the 16 skippable magic numbers share (RFC 8878 §3.1.2). |
| `crates/engine/src/codec/zstd/decoder.rs` `WINDOW_LOG_BASE` | format | 10: windowLog = 10 + Exponent (RFC 8878 §3.1.1.1.2). |
| `crates/engine/src/codec/zstd/fse.rs` `LITERALS_LENGTH_DEFAULT_LOG` | format | 6, the accuracy log of the predefined literals length distribution (RFC 8878 §3.1.1.3.2.2.1). |
| `crates/engine/src/codec/zstd/fse.rs` `MATCH_LENGTH_DEFAULT_LOG` | format | 6, the accuracy log of the predefined match length distribution (RFC 8878 §3.1.1.3.2.2.2). |
| `crates/engine/src/codec/zstd/fse.rs` `MIN_ACCURACY_LOG` | format | 5: Accuracy_Log = low4bits + 5 (RFC 8878 §4.1.1). |
| `crates/engine/src/codec/zstd/fse.rs` `OFFSET_DEFAULT_LOG` | format | 5, the accuracy log of the predefined offset distribution (RFC 8878 §3.1.1.3.2.2.3). |
| `crates/engine/src/codec/zstd/huffman.rs` `DIRECT` | format | 128: a Huffman tree header byte at or above it writes the weights directly (RFC 8878 §4.2.1.1). |
| `crates/engine/src/codec/zstd/huffman.rs` `JUMP_TABLE` | format | 6 bytes, the jump table before four Huffman streams (RFC 8878 §3.1.1.3.1.6). |
| `crates/engine/src/codec/zstd/huffman.rs` `MAX_BITS` | format | 11, the longest Huffman prefix code (RFC 8878 §4.2.1). |
| `crates/engine/src/codec/zstd/huffman.rs` `MAX_WEIGHTS` | derived | 255: weights for literals 0 to 254, the last literal's deduced (RFC 8878 §4.2.1.2). |
| `crates/engine/src/codec/zstd/huffman.rs` `WEIGHTS_MAX_LOG` | format | 6, the largest accuracy log of the Huffman weights' FSE table (RFC 8878 §4.2.1.2). |
| `crates/engine/src/codec/zstd/huffman.rs` `WEIGHT_SYMBOLS` | derived | `MAX_BITS` + 1 = 12: the weights 0 to 11 an FSE description of them can name. |
| `crates/engine/src/codec/zstd/sequences.rs` `LITERALS_LENGTH_MAX_LOG` | format | 9, the largest accuracy log of a literals length table (RFC 8878 §3.1.1.3.2.1, FSE_Compressed_Mode). |
| `crates/engine/src/codec/zstd/sequences.rs` `MATCH_LENGTH_MAX_LOG` | format | 9, the largest accuracy log of a match length table (RFC 8878 §3.1.1.3.2.1). |
| `crates/engine/src/codec/zstd/sequences.rs` `MAX_OFFSET_CODE` | external | 31, the reference decoder's largest offset code N; a decoder may limit N, at least 22 recommended (RFC 8878 §3.1.1.3.2.1.1). |
| `crates/engine/src/codec/zstd/sequences.rs` `OFFSET_MAX_LOG` | format | 8, the largest accuracy log of an offset table (RFC 8878 §3.1.1.3.2.1). |
| `crates/engine/src/codec/deflate.rs` `DIST_SYMBOLS` | format | 32, the fixed code's distance symbols (RFC 1951 §3.2.6; 30 and 31 never occur). |
| `crates/engine/src/codec/deflate.rs` `DYNAMIC_DIST` | format | 30, the most distance symbols a dynamic block describes (RFC 1951 §3.2.7, HDIST + 1). |
| `crates/engine/src/codec/deflate.rs` `DYNAMIC_LITLEN` | format | 286, the most literal/length symbols a dynamic block describes (RFC 1951 §3.2.7, HLIT + 257). |
| `crates/engine/src/codec/deflate.rs` `END_OF_BLOCK` | format | 256, the end-of-block symbol (RFC 1951 §3.2.5). |
| `crates/engine/src/codec/deflate.rs` `HASH_BITS` | cited | 15, zlib's hash size at its default memLevel 8 (`hash_bits = memLevel + 7`, deflate.c), the level RocksDB calls deflateInit2 with. |
| `crates/engine/src/codec/deflate.rs` `HASH_SHIFT` | derived | 32 − `HASH_BITS`: the shift that keeps a hash's top `HASH_BITS` bits. |
| `crates/engine/src/codec/deflate.rs` `LITLEN_SYMBOLS` | format | 288, the fixed code's literal/length symbols (RFC 1951 §3.2.6). |
| `crates/engine/src/codec/deflate.rs` `MAX_BITS` | format | 15, the longest Huffman code (RFC 1951 §3.2.2). |
| `crates/engine/src/codec/deflate.rs` `MAX_MATCH` | format | 258, the longest match (RFC 1951 §3.2.5). |
| `crates/engine/src/codec/deflate.rs` `MIN_MATCH` | format | 3, the shortest match (RFC 1951 §3.2.5). |
| `crates/engine/src/codec/deflate.rs` `WINDOW_MAX` | format | 2^15, the largest window (RFC 1951 §2). |
| `crates/engine/src/codec/lz4.rs` `DISTANCE_MAX` | format | 65,535, lz4's `LZ4_DISTANCE_MAX`: the farthest a two-byte offset reaches (lz4_Block_format.md). |
| `crates/engine/src/codec/lz4.rs` `HASH_LOG` | cited | 12, lz4's table at its default `LZ4_MEMORY_USAGE` 14 (lz4.h): 2^12 four-byte positions. |
| `crates/engine/src/codec/lz4.rs` `HASH_MUL` | cited | 2654435761, the multiplier of lz4's `LZ4_hash4` (lz4.c), Knuth's multiplicative hash. |
| `crates/engine/src/codec/lz4.rs` `HASH_SHIFT` | derived | 32 − `HASH_LOG`: the shift that keeps a hash's top `HASH_LOG` bits. |
| `crates/engine/src/codec/lz4.rs` `LAST_LITERALS` | format | 5, lz4's `LASTLITERALS`: a block's last five bytes are literals (lz4_Block_format.md). |
| `crates/engine/src/codec/lz4.rs` `MATCH_LIMIT` | format | 12, lz4's `MFLIMIT`: no match begins within a block's last twelve bytes (lz4_Block_format.md). |
| `crates/engine/src/codec/lz4.rs` `MIN_MATCH` | format | 4, lz4's `MINMATCH`: a match's least length (lz4_Block_format.md). |
| `crates/engine/src/codec/lz4.rs` `SKIP_TRIGGER` | cited | 6, lz4's `LZ4_skipTrigger` (lz4.c): misses before the fast compressor's step grows by one. |
| `crates/engine/src/codec/snappy.rs` `FRAGMENT` | cited | 2^16, snappy's `kBlockSize` (snappy-internal.h): the encoder matches within 64 KiB, so an offset fits the two-byte copy. |
| `crates/engine/src/codec/snappy.rs` `HASH_MUL` | cited | 0x1e35a7bd, the multiplier of snappy's `HashBytes` (snappy.cc), the encoder's hash of four bytes. |
| `crates/engine/src/codec/snappy.rs` `TABLE_MAX` | cited | 2^14, snappy's `kMaxHashTableSize` (snappy.cc): the most positions the encoder's table holds. |
| `crates/engine/src/codec/snappy.rs` `TABLE_MIN` | cited | 2^8, snappy's `kMinHashTableSize` (snappy.cc): the least. |
| `crates/engine/src/codec/zstd/encoder.rs` `HASHED` | derived | 4: the bytes the match finder hashes, a u32 read; a level's shorter minimum match (`L` of 3) is raised to it. |
| `crates/engine/src/codec/zstd/encoder.rs` `NONE` | format | u32::MAX: no position, the hash table's and chains' empty value (positions are below the 128 KB-block frame's length, far under it). |
| `crates/engine/src/codec/zstd/encoder.rs` `OFFSET_DEFAULT_MAX_CODE` | format | 28, the highest offset code the predefined offset table holds (RFC 8878 §3.1.1.3.2.2.3: N = 28). |
| `crates/engine/src/db/log_format.rs` `BLOCK_SIZE` | format | 32768, `kBlockSize`, the WAL's block [R db/log_format.h:54]. |
| `crates/engine/src/db/log_format.rs` `FIRST_TYPE` | format | 2, `kFirstType` [R db/log_format.h:28]. |
| `crates/engine/src/db/log_format.rs` `FULL_TYPE` | format | 1, `kFullType` [R db/log_format.h:25]. |
| `crates/engine/src/db/log_format.rs` `HEADER_SIZE` | format | 7, `kHeaderSize`: checksum (4), length (2), type (1) [R db/log_format.h:57]. |
| `crates/engine/src/db/log_format.rs` `LAST_TYPE` | format | 4, `kLastType` [R db/log_format.h:30]. |
| `crates/engine/src/db/log_format.rs` `MIDDLE_TYPE` | format | 3, `kMiddleType` [R db/log_format.h:29]. |
| `crates/engine/src/db/log_format.rs` `PREDECESSOR_WAL_INFO_TYPE` | format | 130, `kPredecessorWALInfoType` [R db/log_format.h:47]. |
| `crates/engine/src/db/log_format.rs` `RECORD_TYPE_SAFE_IGNORE_MASK` | format | 0x80, `kRecordTypeSafeIgnoreMask`: an unknown type with it set is skipped [R db/log_format.h:51]. |
| `crates/engine/src/db/log_format.rs` `RECYCLABLE_FIRST_TYPE` | format | 6, `kRecyclableFirstType` [R db/log_format.h:34]. |
| `crates/engine/src/db/log_format.rs` `RECYCLABLE_FULL_TYPE` | format | 5, `kRecyclableFullType` [R db/log_format.h:33]. |
| `crates/engine/src/db/log_format.rs` `RECYCLABLE_HEADER_SIZE` | format | 11, `kRecyclableHeaderSize`: the legacy header and a 4-byte log number [R db/log_format.h:61]. |
| `crates/engine/src/db/log_format.rs` `RECYCLABLE_LAST_TYPE` | format | 8, `kRecyclableLastType` [R db/log_format.h:36]. |
| `crates/engine/src/db/log_format.rs` `RECYCLABLE_MIDDLE_TYPE` | format | 7, `kRecyclableMiddleType` [R db/log_format.h:35]. |
| `crates/engine/src/db/log_format.rs` `RECYCLABLE_USER_DEFINED_TIMESTAMP_SIZE_TYPE` | format | 11, `kRecyclableUserDefinedTimestampSizeType` [R db/log_format.h:44]. |
| `crates/engine/src/db/log_format.rs` `RECYCLE_PREDECESSOR_WAL_INFO_TYPE` | format | 131, `kRecyclePredecessorWALInfoType` [R db/log_format.h:48]. |
| `crates/engine/src/db/log_format.rs` `SET_COMPRESSION_TYPE` | format | 9, `kSetCompressionType` [R db/log_format.h:39]. |
| `crates/engine/src/db/log_format.rs` `TIMESTAMP_SIZE_ENTRY_SIZE` | format | 6: a fixed32 column family id and a fixed16 timestamp size, `kSizePerColumnFamily` [R util/udt_util.h:80]. |
| `crates/engine/src/db/log_format.rs` `USER_DEFINED_TIMESTAMP_SIZE_TYPE` | format | 10, `kUserDefinedTimestampSizeType` [R db/log_format.h:43]. |
| `crates/engine/src/db/log_format.rs` `ZERO_TYPE` | format | 0, `kZeroType`, preallocated space [R db/log_format.h:24]. |
| `crates/engine/src/file/writable_file_writer.rs` `MAX_BUFFER_SIZE` | cited | 1 MiB, `writable_file_max_buffer_size`'s default [R include/rocksdb/options.h:1292]: the most the writer holds before writing. |
| `crates/engine/src/util/compression.rs` `WAL_LEVEL` | cited | 3, `ZSTD_CLEVEL_DEFAULT` (zstd 1.5.7 lib/zstd.h:134), the level of the context RocksDB's WAL compressor creates. |
| `crates/engine/src/util/compression.rs` `WAL_WINDOW_MAX` | cited | 2^27 bytes, `ZSTD_WINDOWLOG_LIMIT_DEFAULT` (zstd 1.5.7 lib/zstd.h:1287), the reference decoder's default window bound, which RocksDB keeps. |
| `crates/engine/src/db/kv_checksum.rs` `SEED_K` | cited | 0, `ProtectionInfo`'s key seed [R db/kv_checksum.h:84]; per-entry checksums must equal RocksDB's to verify its blocks' as it does. |
| `crates/engine/src/db/kv_checksum.rs` `SEED_V` | cited | 0xD28AAD72F49BD50B, `ProtectionInfo`'s value seed [R db/kv_checksum.h:85]. |
| `crates/engine/src/table/block_based/block.rs` `GUARD_LEN` | cited | 8 restarts: the window below which interpolation search turns to binary search [R table/block_based/block.cc:863]; a search choice, not a format, kept so seeks touch what RocksDB's touch. |
| `crates/engine/src/table/block_based/block.rs` `MAX_POOR_SEARCHES` | cited | 8 guesses in a row that did not halve the window before interpolation search turns to binary [R table/block_based/block.cc:864]. |
| `crates/engine/src/table/block_based/block_prefix_index.rs` `BLOCK_ARRAY_MASK` | format | 0x80000000, `kBlockArrayMask`: the bucket bit marking an index into the block array [R table/block_based/block_prefix_index.cc:39]. |
| `crates/engine/src/table/block_based/block_prefix_index.rs` `NONE_BLOCK` | format | 0x7FFFFFFF, `kNoneBlock`: a bucket no prefix hashed to [R table/block_based/block_prefix_index.cc:38]. |
| `crates/engine/src/table/block_based/data_block_footer.rs` `HASH_INDEX_BIT` | format | Bit 31 of a data block's packed footer word: a hash index follows [R table/block_based/data_block_footer.cc:12]. |
| `crates/engine/src/table/block_based/data_block_footer.rs` `MAX_ENCODED_LENGTH` | format | 8 bytes, `DataBlockFooter::kMaxEncodedLength`: the values offset and the packed word [R table/block_based/data_block_footer.h:57]. |
| `crates/engine/src/table/block_based/data_block_footer.rs` `MAX_NUM_RESTARTS` | format | 2^28 - 1, `kMaxNumRestarts`: the packed word's low 28 bits [R table/block_based/data_block_footer.h:52]. |
| `crates/engine/src/table/block_based/data_block_footer.rs` `MIN_ENCODED_LENGTH` | format | 4 bytes, `kMinEncodedLength`: the packed word alone [R table/block_based/data_block_footer.h:60]. |
| `crates/engine/src/table/block_based/data_block_footer.rs` `SEPARATED_KV_BIT` | format | Bit 28: keys and values in separate sections [R table/block_based/data_block_footer.cc:16]. |
| `crates/engine/src/table/block_based/data_block_footer.rs` `UNIFORM_KEYS_BIT` | format | Bit 29: the restart keys are uniformly spread [R table/block_based/data_block_footer.cc:14]. |
| `crates/engine/src/table/block_based/data_block_hash_index.rs` `COLLISION` | format | 254, `kCollision`: a bucket keys of two restart intervals hashed to [R table/block_based/data_block_hash_index.h:58]. |
| `crates/engine/src/table/block_based/data_block_hash_index.rs` `DEFAULT_UTIL_RATIO` | cited | 0.75, `kDefaultUtilRatio`, taken for a ratio of zero or less [R table/block_based/data_block_hash_index.h:64]; it sets the bucket count, so the port's blocks equal RocksDB's. |
| `crates/engine/src/table/block_based/data_block_hash_index.rs` `MAX_BLOCK_SIZE_SUPPORTED_BY_HASH_INDEX` | format | 2^16 bytes: the hash index's 16-bit offsets [R table/block_based/data_block_hash_index.h:62]. |
| `crates/engine/src/table/block_based/data_block_hash_index.rs` `MAX_RESTART_SUPPORTED_BY_HASH_INDEX` | format | 253: the restart indexes a bucket byte holds besides `NO_ENTRY` and `COLLISION` [R table/block_based/data_block_hash_index.h:59]. |
| `crates/engine/src/table/block_based/data_block_hash_index.rs` `NO_ENTRY` | format | 255, `kNoEntry`: a bucket no key hashed to [R table/block_based/data_block_hash_index.h:57]. |
| `crates/engine/src/table/block_based/index_builder.rs` `TOP_LEVEL_ENTRY_GUESS` | cited | 70 bytes, RocksDB's guess of a top-level index entry in a two-level index's size estimate [R table/block_based/index_builder.cc:396-398]; kept so the port cuts table files where RocksDB does. |
| `crates/engine/src/table/format.rs` `BLOCK_BASED_TABLE_MAGIC_NUMBER` | format | 0x88e241b785f4cff7, `kBlockBasedTableMagicNumber` [R table/block_based/block_based_table_builder.cc:146]. |
| `crates/engine/src/table/format.rs` `BLOCK_TRAILER_SIZE` | format | 5 bytes, `kBlockTrailerSize`: a block's compression type and 32-bit checksum [R table/block_based/block_based_table_reader.h:133]. |
| `crates/engine/src/table/format.rs` `CUCKOO_TABLE_MAGIC_NUMBER` | format | 0x926789d0c5f17873, `kCuckooTableMagicNumber` [R table/cuckoo/cuckoo_table_builder.cc:47]; refused until P20. |
| `crates/engine/src/table/format.rs` `FOOTER_CHECKSUM_AT` | derived | Where a format_version 6 footer's checksum lies: after the checksum type and the extended magic [R table/format.cc:280-288]. |
| `crates/engine/src/table/format.rs` `FOOTER_ENCODED_LENGTH` | derived | 53 bytes, `Footer::kNewVersionsEncodedLength`: the checksum type, part 2, the format version and the magic [R table/format.h:304-306]. |
| `crates/engine/src/table/format.rs` `FOOTER_PART2_SIZE` | derived | 40 bytes, `kFooterPart2Size`: two block handles at their longest [R table/format.cc:224]. |
| `crates/engine/src/table/format.rs` `FOOTER_VERSION0_ENCODED_LENGTH` | derived | 48 bytes, `Footer::kVersion0EncodedLength` [R table/format.h:296-298]. |
| `crates/engine/src/table/format.rs` `LATEST_BBT_FORMAT_VERSION` | format | 7, `kLatestBbtFormatVersion` [R table/format.h:172]. |
| `crates/engine/src/table/format.rs` `LEGACY_BLOCK_BASED_TABLE_MAGIC_NUMBER` | format | 0xdb4775248b80fb57, the magic of block-based tables before format_version 2, which RocksDB 11 no longer reads [R table/format.cc:344]. |
| `crates/engine/src/table/format.rs` `LEGACY_PLAIN_TABLE_MAGIC_NUMBER` | format | 0x4f3418eb7a8f13b8, `kLegacyPlainTableMagicNumber` [R table/plain/plain_table_builder.cc:56]; refused until P20. |
| `crates/engine/src/table/format.rs` `MAGIC_NUMBER_LENGTH` | format | 8 bytes, `kMagicNumberLengthByte` [R table/format.h:42]. |
| `crates/engine/src/table/format.rs` `MAX_ENCODED_LENGTH` | derived | 20 bytes, `BlockHandle::kMaxEncodedLength`: two varint64s [R table/format.h:83]. |
| `crates/engine/src/table/format.rs` `MIN_SUPPORTED_BBT_FORMAT_VERSION` | format | 2, `kMinSupportedBbtFormatVersionForRead` and `ForWrite` [R table/format.h:179-187]. |
| `crates/engine/src/table/format.rs` `PLAIN_TABLE_MAGIC_NUMBER` | format | 0x8242229663bf9564, `kPlainTableMagicNumber` [R table/plain/plain_table_builder.cc:55]; refused until P20. |
| `crates/engine/src/table/table_properties.rs` `UNKNOWN_COLUMN_FAMILY` | format | INT32_MAX, `kUnknownColumnFamily`, stored as a table's column family id when none is known [R table/table_properties.cc:22-23]. |
| `crates/engine/src/util/block_compression.rs` `COMPRESSION_SIZE_LIMIT` | cited | INT32_MAX bytes, `kCompressionSizeLimit`: blocks this large are not compressed [R table/block_based/block_based_table_builder.h:224]. |
| `crates/engine/src/util/block_compression.rs` `ZSTD_DEFAULT_LEVEL` | external | 3, zstd's `ZSTD_CLEVEL_DEFAULT` (zstd 1.5.7 lib/zstd.h:134), what RocksDB's default level means for ZSTD [R util/compression.h:65-75]. |
| `crates/engine/src/util/block_compression.rs` `ZSTD_WINDOW_MAX` | bound | 2^27 bytes: the largest window a block's ZSTD frame may ask of the decoder, the reference decoder's default limit (`ZSTD_WINDOWLOG_LIMIT_DEFAULT`, zstd 1.5.7 lib/zstd.h:1287), under which RocksDB's `ZSTD_decompressDCtx` reads. |
| `crates/engine/src/codec/zstd/encoder.rs` `HASH_LOG_MIN` | external | 6, `ZSTD_HASHLOG_MIN` (zstd 1.5.7 lib/zstd.h:1268): the smallest window and hash logs the reference fits a small input's parameters to (`ZSTD_adjustCParams_internal`). |
| `crates/engine/src/codec/zstd/bits.rs` `READ_MAX` | format | 32 bits: the widest single read of a ZSTD bitstream (an offset's at most 31 extra bits, §3.1.1.3.2.1.1), so the reader refills its 64-bit word once fewer remain. |
| `crates/engine/src/codec/zstd/decoder.rs` `SLACK` | derived | `2 * WILD_COPY`: the bytes past a block's end the decoder's piece-wise copies may write, as the reference's `WILDCOPY_OVERLENGTH` is twice its 16-byte copy (zstd 1.5.7 lib/common/zstd_internal.h). |
| `crates/engine/src/codec/zstd/decoder.rs` `WILD_COPY` | external | 16 bytes, the reference's `COPY16` piece (zstd 1.5.7 lib/common/zstd_internal.h, `ZSTD_wildcopy`): one 128-bit register on both targets' vector units (NEON, SSE2). |
| `crates/engine/src/codec/zstd/fse.rs` `CELLS_MAX` | format | 2^9 cells: the largest FSE accuracy log a sequences table may state (RFC 8878 §3.1.1.3.2.1, literals and match lengths 9). |
| `crates/engine/src/codec/zstd/fse.rs` `SYMBOLS_MAX` | format | 64: the most symbols a distribution describes rounded up to a power of two, above the match length codes' 53 (RFC 8878 §3.1.1.3.2.1.1). |
| `crates/engine/src/codec/zstd/huffman.rs` `LITERALS` | format | 256: every byte value is a literal a Huffman table codes (RFC 8878 §4.2.1). |
| `crates/engine/src/codec/zstd/huffman.rs` `PACKAGE` | format | `u16::MAX`: the marker for a package in a package-merge row, above every literal (0 to 255) it stands beside. |
| `crates/engine/src/codec/zstd/huffman.rs` `ROW_MAX` | derived | `2 * LITERALS`: a package-merge row holds every leaf and at most one package for each pair of the row before (Larmore and Hirschberg, JACM 37(3), 1990). |
| `crates/gateway/src/layout.rs` `CHUNK` | cited | Tectonic's "typically 8 MiB" chunk (docs/research/01 §1.14). docs/design/gateway.md says measured transfer sizing will replace it, and audit §12.6/§16.2 requires per-upload sizing. |
| `crates/gateway/src/layout.rs` `SEALED` | derived | `seal::SEGMENT + seal::TAG`: a 64 KiB plaintext segment plus its 16-byte AEAD tag (docs/design/gateway.md §1). |
| `crates/gateway/src/put.rs` `RENEWALS` | cited | Quarter-lease renewal from Centrifuge's 15 s renewals of 60 s leases (docs/research/09 §7.2.2). Audit §12.6 requires deriving it from control-delay and outage distributions. |
| `crates/mantle/src/bench_gateway.rs` `HANDOVER` | bound | 2^40 ticks of a cell clock that moves one tick a command, past any number of commands a bounded run applies, so no deadline expires in a benchmark. |
| `crates/mantle/src/bench_hash.rs` `CHUNKED_BODY` | open | 16 MiB signed-chunk body measured; no derivation in code or the measurement docs. |
| `crates/mantle/src/bench_hash.rs` `FORM_FILE` | open | 16 MiB form file measured, set equal to CHUNKED_BODY; lacks a derivation. |
| `crates/mantle/src/bench_log.rs` `KEEP` | open | A replica compacts every 64 entries, keeping 64 behind; it stands in for a follower window but lacks a derivation. |
| `crates/mantle/src/bench_meta.rs` `LARGEST_FILE` | derived | 646 blocks, the most one PUT's file names: 5 GiB in the smallest blocks any layout makes, pinned by `the_largest_upload_fits_a_file_under_every_layout` (`crates/gateway/src/layout.rs`; audit B08). |
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
| `crates/meta/src/key.rs` `NAMED` | format | File-layer row tag 4 for the row saying a file names a block, after the extents' tag so a scan of extents stops before it. |
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
| `crates/meta/src/key.rs` `TERM` | format | Marker byte `t` after `LOCAL` for the term of the last entry the range applied, which its replica keeps beside the index (docs/design/replica.md §4). |
| `crates/meta/src/key.rs` `UNSETTLED` | format | Marker byte `u` after `LOCAL` for files and blocks whose handover is unsettled. |
| `crates/meta/src/key.rs` `UPLOAD` | format | Name-row tag 3: upload rows, sorting after versions. |
| `crates/meta/src/key.rs` `VERSION` | format | Name-row tag 2: version rows, sorting after the null pointer. |
| `crates/meta/src/name.rs` `MAX_PART` | external | The S3 maximum part size of 5 GiB (docs/research/05 §4.1, AWS S3 multipart limits). |
| `crates/meta/src/name.rs` `MAX_PART_NUMBER` | external | S3 part numbers run from 1 to 10,000 (docs/research/05 §4.3, AWS UploadPart reference). |
| `crates/meta/src/name.rs` `MIN_PART` | external | The S3 minimum part size of 5 MiB, with no minimum for the last part (docs/research/05 §4.1, AWS S3 multipart limits). |
| `crates/meta/src/record.rs` `DAY_MS` | derived | Unit conversion: 86,400 s × 1,000 ms per day, for S3 Object Lock retention days and years (docs/research/18 §2.4, §2.6). |
| `crates/meta/src/record.rs` `FORMAT` | format | Version byte 6 that begins every row value written by this code; 6 since a file keeps a row for each block it names, 5 since a part's row records its file's handover deadline, which the completion adopting it gives the part's mark without reading it, 4 since a mark records its write's handover deadline and a queue row whether its ID names a file, 3 since a file's mark records how the range took it (a version's order, a part, an adopting composite, or refused), 2 since a version records its completion's part listing. |
| `crates/meta/src/record.rs` `LISTING` | external | Bytes of a completion's listing digest: SHA-256's 256-bit output (FIPS 180-4 §1). |
| `crates/meta/src/wire.rs` `MAX_BUCKET` | external | A bucket name of at most 63 characters (research/05 §10.3). |
| `crates/meta/src/wire.rs` `MAX_KEY` | external | An object key of at most 1,024 bytes (research/05 §10.1). |
| `crates/meta/src/wire.rs` `MAX_HEADERS` | external | A request's headers within 8 KB (research/05 §10.2), which bound its preconditions. |
| `crates/meta/src/wire.rs` `MAX_PARTS` | external | 10,000 parts to an upload (research/05 §4.1). |
| `crates/meta/src/wire.rs` `ETAG_HEX` | external | A part's ETag, the hex of its 16-byte MD5 (research/05 §4.5). |
| `crates/meta/src/wire.rs` `FORMAT` | format | Version byte 5 of an encoded log entry; 5 since a registration refused at the bound of sessions is answered `SessionsFull` with the time a place frees, 4 since a part carries its file's handover deadline in place of the command beside it, 3 since a PUT and a completion carry the ID of their request, which names a write with no file, and a mark kept past an unmark is answered with its deadline, 2 since a completion carries its part listing's digest. |
| `crates/meta/src/wire.rs` `MAX_COMMANDS` | bound | 2^16 command places per entry, so a session ID is index × 2^16 + place (session.rs `register`); the audit (§12.6) keeps it as a versioned encoding bound with byte and work budgets beside it. |
| `crates/range/src/conf.rs` `FORMAT` | format | Version byte of the encoded configuration row. |
| `crates/range/src/image.rs` `FORMAT` | format | Version byte of the encoded snapshot image. |
| `crates/range/src/replica.rs` `QUIET` | derived | `Duration::MAX`, no quiet write: the durable shell writes a commit no write stated after its owner's period so that a member acting on applied state at its next start reopens with it (hyper-raft docs/durable.md §4.1); no entry of a range is acted on at start (`RangeMachine::acts_at_start`), and the engine's durable index is part of the durable commit, so the write is never due. |
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
| `crates/mantle/src/bench_log.rs:107` log Config | open | 16 MiB segments, 64 segments, 2^20 entries and 1 GiB per group, 64 KiB cache, queue of twice the replicas: set above use, not derived. |
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
- A bucket policy's raw body limit (`policy::BODY_LIMIT`): a recording of how much white space
  S3 keeps in a policy it gives back as set.
- The benchmarks' ladders and step lengths: the regimes each exercises, in their
  measurement records, or step lengths from the samples their quantiles need
  (research 11 §16).
