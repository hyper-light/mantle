# 24 — RocksDB 11.8.1 source map: the specification of a Rust port

Research note and port contract. It specifies a conversion of RocksDB 11.8.1 to Rust inside
mantle: the on-disk formats byte for byte (§1), a map of the engine's modules with what the
port keeps, replaces and drops (§2), a dependency-ordered sequence of phases with the tests and
differential checks that close each (§3), the concurrency and memory model the port preserves
or deliberately changes (§4), and the hard parts, sized (§5).

It builds on note 12, which recommends RocksDB as the engine under a metadata range and keeps a
mantle-built LSM as the long-term option (12 §5.6, §5.8). A port is that option taken with
RocksDB's own formats and behaviour as the definition of done: the port reads every file
RocksDB 11.8.1 writes in the configurations §1.18 lists, and RocksDB 11.8.1 reads every file
the port writes. The engine runs as note 12 §6.1–§6.2 describe: one instance per range
replica, many instances per process under shared budgets, and the WAL off, with the per-disk
Raft log (docs/design/raft-log.md) as the only write-ahead log and each batch carrying the
applied index. The WAL format is still ported (§1.5), because a directory RocksDB wrote may
hold one and because the fidelity checks of §3 run with it on.

Compiled 2026-09-30. This is research input, not a decision record: the DECISION items are
proposals for `docs/design/`.

---

## How to read this document

**Source.** RocksDB at tag `v11.8.1`, commit `abeebd9630f11bd08c28b7bd43c7bdfc62050654`
("Additional HISTORY.md update for 11.8.1"), checked out at `~/Projects/rocksdb`. Tag
`v11.8.0` points at the same commit. `include/rocksdb/version.h:14-16` gives 11.8.1. The
working tree holds build artifacts (`*.o`, `*.d`) from an earlier build; they are excluded
from every count below and nothing was built for this note.

**Citation tags.**

- `[R path:line]` or `[R path:start-end]`: RocksDB source at the commit above. Several
  citations in one bracket are separated by `;` and a bare `:line` repeats the last path.
  Every cited line was read.
- "note N §x": earlier mantle research notes. `CLAUDE.md §n`: mantle's rules.
- mantle source: `crates/<crate>/src/<file>` at the working tree of 2026-09-30.

**Labels.**

- *(no label)* or SOURCE: what the RocksDB code does, at the cited line.
- **DERIVED**: counted or computed by this note from the source (line counts, test counts,
  arithmetic).
- **DECISION**: what the port does, with the reason. A proposal until recorded in
  `docs/design/`.
- **UNVERIFIED**: not established from the source.

**Counts.** Line counts are `wc -l` of `.cc`, `.h` and `.c` files. A "test definition" is one
`TEST`, `TEST_F`, `TEST_P`, `TYPED_TEST` or `TYPED_TEST_P` macro at the start of a line
(`grep -cE '^\s*(TEST|TEST_F|TEST_P|TYPED_TEST|TYPED_TEST_P)\('`); a parameterized test counts
once however many parameter sets instantiate it. All counts are DERIVED.

**Terms.** *fv* is the block-based table `format_version`. *LP* is a length-prefixed byte
string: varint32 length, then the bytes [R util/coding.h:210-213]. *CF* is a column family.
All fixed-width integers are little-endian unless a line says otherwise.

---

## 0. Summary

1. **Formats.** Everything RocksDB 11.8.1 persists is specified in §1 from the code: the
   integer coding, three checksum families, the WAL/log container, WriteBatch, internal keys,
   the block-based table for fv 2–7 with all index and filter types, MANIFEST/VersionEdit,
   CURRENT, IDENTITY, OPTIONS, blob files and wide-column entities. Default writes are fv 7
   with XXH3 block checksums [R include/rocksdb/table.h:374, :739].
2. **Two traps a crate cannot fill.** `Hash64`, which keys the FastLocalBloom and Ribbon
   filters, is the xxHash 0.7.2 *preview* of XXH3 (`XXPH3`), not the released XXH3
   [R util/hash.cc:81-88; util/xxph3.h:39-43]. The legacy 32-bit `Hash`, which keys the legacy
   bloom filter and the data-block hash index, sign-extends its tail bytes
   [R util/hash.cc:45-59]. Both must be ported bit for bit: a wrong hash makes filters and
   hash indexes answer "absent" for keys that exist (§5 R1, R2).
3. **Scope.** About 210,000 lines of C++ source are kept (DERIVED from the verdicts of §2).
   The Java binding (116,064 lines), C API, transactions (25,449), backup engine, legacy
   BlobDB, alternative table formats, secondary/follower instances, remote and tiered
   compaction, user-defined timestamps, the experimental trie index and the `Customizable`
   framework are dropped, each with its reason (§2).
4. **Order.** Fifteen phases (§3): coding and checksums, WriteBatch and memtable, WAL, blocks
   and tables, filters, MANIFEST, open/flush/recovery, reads, block cache, leveled compaction,
   universal/FIFO, range deletions/merge/filters, checkpoint/ingest/export/import, shared
   budgets, db_bench. Each is checked two ways against the C++ build (`sst_dump`, `ldb`,
   `db_bench`): the C++ tools read what the port wrote, and the port reads what the C++ wrote.
   The phases name 118 RocksDB test files (≈219,600 lines, ≈3,360 test definitions) to port.
5. **Model changes.** One writer per instance (the Raft apply thread) replaces concurrent
   memtable insert and group-commit machinery, which removes the memtable's need for
   `unsafe` (the block cache's lock-free read is the other place the C++ relies on raw
   pointers, §4.8). Stalls and full queues become typed refusals instead of sleeps and
   unbounded waits; background workers and budgets are process-wide; there is no auto-resume
   after a background error (§4).
6. **Largest risks** (§5): the scale (XL), byte-identical tables across fv 2–7 (L), a safe
   memtable as fast as the C++ one (L), 5,634 asserts to turn into typed errors (L),
   range tombstones in the read path (L), compression codecs with a C dependency for ZSTD (M),
   and a directory-level crash simulator mantle-disk does not have yet (M).

---

## 1. On-disk formats

Each subsection gives the layout, the writer, the reader's checks, and what the port does.
The port writes exactly the bytes RocksDB writes for the same input and options, and returns a
typed error where RocksDB returns `Status::Corruption`, `NotSupported` or `InvalidArgument`.
Every `assert` on a decode path in RocksDB becomes a checked condition with a typed error in
the port (CLAUDE.md §1; §5 R6).

### 1.1 Integer and string coding (util/coding.h, util/coding_lean.h, util/coding.cc)

- **Fixed width.** `EncodeFixed16/32/64` and `PutFixed16/32/64` write 2, 4, 8 bytes
  little-endian [R util/coding_lean.h:6-8, :24-57; util/coding.h:119-150]; `GetFixed*` fail if
  the input is short [R util/coding.h:246-271].
- **Varint32.** LEB128, 7 bits per byte, bit 0x80 set on every byte but the last, at most
  5 bytes [R util/coding.cc:24-50; util/coding.h:156]. The decoder's fast path takes one byte
  when bit 0x80 is clear [R util/coding.h:106-116]; the fallback reads at most 5 bytes
  (`shift <= 28`) and does **not** reject high bits in the fifth byte, which are silently lost
  in the 32-bit shift [R util/coding.cc:55-71]. It returns null when input runs out or five
  bytes all carry 0x80.
- **Varint64.** Same, at most `kMaxVarint64Length = 10` bytes [R util/coding.h:36, :163-178;
  util/coding.cc:73-88]; no overflow check on the tenth byte.
- **Length prefix.** `PutLengthPrefixedSlice` = varint32 length + bytes
  [R util/coding.h:210-213]; `GetLengthPrefixedSlice` fails unless `len` bytes follow
  [R :309-318]. `PutLengthPrefixedSliceParts` prefixes the summed length [R :215-229].
- **Signed varint.** Zigzag (`(l << 1) ^ (l >> 63)`) then varint64
  [R util/coding.h:73-78, :180-185]; used by index value delta encoding (§1.10).
- **Big-endian** appears in one place only: the file checksum (§1.2).

**DECISION.** The port's decoders reject what RocksDB silently truncates: a fifth varint32
byte with bits above 2^32, a tenth varint64 byte above 1, are `Corruption`. RocksDB never
writes either, so no valid file is refused. Encoders are exact.

### 1.2 Checksums and hashes

**CRC-32C** (Castagnoli, reflected polynomial `0x82f63b78`) [R util/crc32c.cc:1197,
:274-314]. `Value(data) = Extend(0, data)` [R util/crc32c.h:26, :35]. Stored CRCs are
*masked*:

```
kMaskDelta = 0xa282ead8                          [R util/crc32c.h:37]
Mask(c)    = ((c >> 15) | (c << 17)) + kMaskDelta  (rotate right 15, add)   [R :44-47]
Unmask(m)  = r = m - kMaskDelta; (r >> 17) | (r << 15)                      [R :50-53]
```

`Crc32cCombine(crc1, crc2, len2)` combines by GF(2) powers [R util/crc32c.cc:1278-1293];
mantle-crc already implements zlib's equivalent (`crates/crc/src/lib.rs`).

**Block checksum types** [R include/rocksdb/table.h:117-123]:

| Value | Name | `ComputeBuiltinChecksum(data, n)` [R table/format.cc:615-639] |
|---|---|---|
| 0x0 | kNoChecksum | 0 |
| 0x1 | kCRC32c | `Mask(crc32c(data))` |
| 0x2 | kxxHash | `XXH32(data, n, seed 0)` |
| 0x3 | kxxHash64 | `Lower32of64(XXH64(data, n, seed 0))` |
| 0x4 | kXXH3 (default, [R table.h:374]) | `n == 0 ? 0 : ModifyChecksumForLastByte(Lower32of64(XXH3_64bits(data, n-1)), data[n-1])` |

`ModifyChecksumForLastByte(c, b) = c ^ (u8(b) * 0x6b9083d9)` [R table/format.cc:606-612].
`ComputeBuiltinChecksumWithLastByte` computes the same value over `data ‖ last_byte` without
concatenating (CRC extended by one byte then masked; XXH32/XXH64 streamed; XXH3 over all of
`data` then the last-byte modifier) [R table/format.cc:641-682; format.h:376-385]. XXH32,
XXH64 and XXH3_64bits are xxHash 0.8.1, namespaced `ROCKSDB_` [R util/xxhash.h:11-13,
:474-476]: the released algorithms, which `twox-hash` (a mantle workspace dependency)
implements.

**Context checksum** (fv ≥ 6) [R table/format.h:143-170, :218-220]:

```
ChecksumModifierForContext(base, offset) =
    (base ^ (Lower32of64(offset) + Upper32of64(offset))) & (base != 0 ? 0xFFFFFFFF : 0)
stored = checksum + modifier    (wrapping u32 add)
```

The builder draws `base_context_checksum` from a random generator until non-zero
[R table/block_based/block_based_table_builder.cc:1438-1447]; the offset is the block's file
offset [R :2259], or the footer's own offset for the footer [R table/format.cc:311-312].

**Hashes that shape on-disk bytes** (not checksums):

- `Hash(data, n, seed)`: 32-bit, Murmur-like, `m = 0xc6a4a793`, `r = 24`,
  `h = seed ^ (n*m)`; per 4-byte LE word `h += w; h *= m; h ^= h >> 16`; tail bytes are added
  **sign-extended through `int8_t`**, then `h *= m; h ^= h >> r` [R util/hash.cc:25-65].
  `BloomHash = Hash(·, 0xbc9f1d34)` [R util/hash.h:93-95]; `GetSliceHash = Hash(·, 397)`
  [R :121-123].
- `Hash64(data, n[, seed]) = XXPH3_64bits[_withSeed]` [R util/hash.cc:81-88], the xxHash
  0.7.2 preview of XXH3, vendored as `util/xxph3.h` with its own secret [R util/xxph3.h:39-43,
  :133-135, :920]. `GetSliceHash64 = Hash64(data, size)` [R util/hash.h:97-99].
- `FastRange32(hash, range) = (u64(range) * hash) >> 32` [R util/fastrange.h:42-48].

**File checksum** (`file_checksum_gen_factory` with the built-in generator): name
`"FileChecksumCrc32c"`; value is the **unmasked** CRC-32C of the whole file, stored as 4
**big-endian** bytes (`PutFixed32(EndianSwapValue(crc))`) [R util/file_checksum_helper.h:30-49].
Unknown checksum: value `""`, name `"Unknown"` [R include/rocksdb/file_checksum.h:23, :27].
This is also mantle's transfer checksum for snapshots and moves (note 12 §6.3), so a table's
file checksum in the MANIFEST (§1.14) can be checked against mantle's own.

**DECISION.** CRC-32C from mantle-crc; XXH32/XXH64/XXH3 from `twox-hash`; `Hash`, `Hash64`
(XXPH3, the 64-bit path only), `FastRange32/64` ported bit for bit with golden vectors from the
C++ build (§3 P1). Hash arithmetic uses explicit `wrapping_*` operations, the stated meaning of
a hash (as mantle-crc and mantle-ec already do).

### 1.3 Internal keys, value types, sequence numbers (db/dbformat.h, db/dbformat.cc)

**Internal key** = `user_key ‖ fixed64(seq << 8 | type)` [R db/dbformat.h:181-199;
db/dbformat.cc:57-81]; `kNumInternalBytes = 8` [R dbformat.h:134];
`kMaxSequenceNumber = 2^56 − 1` [R :129]; `kDisableGlobalSequenceNumber = u64::MAX`
[R :131-132]. Parsing fails on size < 8 or a type that is not an extended value type
[R :519-541].

**Order** (`InternalKeyComparator::Compare`): user key ascending by the user comparator, then
the 8-byte trailer as a u64 **descending** — sequence descending, then type descending
[R db/dbformat.h:1159-1178]. The built-in user comparators are named
`leveldb.BytewiseComparator` and `rocksdb.ReverseBytewiseComparator`, with a `.u64ts` suffix
for their timestamped forms [R util/comparator.cc:33, :155, :313].

**ValueType** [R db/dbformat.h:41-78]:

| Hex | Name | Where it appears |
|---|---|---|
| 0x00 | kTypeDeletion | batch, memtable, table |
| 0x01 | kTypeValue | batch, memtable, table |
| 0x02 | kTypeMerge | batch, memtable, table |
| 0x03 | kTypeLogData | batch only |
| 0x04 | kTypeColumnFamilyDeletion | batch only |
| 0x05 | kTypeColumnFamilyValue | batch only |
| 0x06 | kTypeColumnFamilyMerge | batch only |
| 0x07 | kTypeSingleDeletion | batch, memtable, table |
| 0x08 | kTypeColumnFamilySingleDeletion | batch only |
| 0x09 | kTypeBeginPrepareXID | batch only |
| 0x0A | kTypeEndPrepareXID | batch only |
| 0x0B | kTypeCommitXID | batch only |
| 0x0C | kTypeRollbackXID | batch only |
| 0x0D | kTypeNoop | batch only |
| 0x0E | kTypeColumnFamilyRangeDeletion | batch only |
| 0x0F | kTypeRangeDeletion | batch, range-del table and block |
| 0x10 | kTypeColumnFamilyBlobIndex | batch only |
| 0x11 | kTypeBlobIndex | batch, memtable, table |
| 0x12 | kTypeBeginPersistedPrepareXID | batch only |
| 0x13 | kTypeBeginUnprepareXID | batch only |
| 0x14 | kTypeDeletionWithTimestamp | memtable, table (never a batch tag) |
| 0x15 | kTypeCommitXIDAndTimestamp | batch only |
| 0x16 | kTypeWideColumnEntity | batch, memtable, table |
| 0x17 | kTypeColumnFamilyWideColumnEntity | batch only |
| 0x18 | kTypeValuePreferredSeqno | batch, memtable, table |
| 0x19 | kTypeColumnFamilyValuePreferredSeqno | batch only |
| 0x1A | kTypeMaxValid (implicit) | never stored |
| 0x7F | kMaxValue | never stored |

`kValueTypeForSeek = kTypeValuePreferredSeqno` (0x18), `kValueTypeForSeekForPrev =
kTypeDeletion` [R db/dbformat.cc:28-29]. `kRangeTombstoneSentinel =
Pack(kMaxSequenceNumber, kTypeRangeDeletion)` [R dbformat.h:201-202].

**User-defined timestamps** sit at the end of the user key, before the 8-byte trailer; min is
`ts_sz` bytes of 0x00, max `ts_sz` bytes of 0xFF [R db/dbformat.h:331-361;
db/dbformat.cc:83-139]; the built-in u64 comparator stores the timestamp fixed64 LE and sorts
newer first [R util/comparator.cc:262-308, :344-362].

**DECISION.** The port supports the bytewise comparator (and its reverse) and no
user-defined timestamps. A MANIFEST whose comparator name is not one the port knows, or a CF
with `kPersistUserDefinedTimestamps` / a `.u64ts` comparator, is refused at open with
`Unsupported { feature: "user-defined timestamps" }`; a WAL timestamp-size record (§1.5) with
a non-zero size for an open CF likewise.

### 1.4 WriteBatch (db/write_batch.cc, db/write_batch_internal.h)

**Header** (`kHeader = 12`) [R db/write_batch_internal.h:80-81]: `fixed64 sequence ‖ fixed32
count`, followed by records in order [R db/write_batch.cc:10-37, :782-796]. A WAL record's
payload is exactly this byte string [R db/write_batch_internal.h:171;
db/db_impl/db_impl_write.cc:2270, :2293]. `count` counts Put, Delete, SingleDelete,
DeleteRange, Merge, BlobIndex, PutEntity and TimedPut records, not LogData, Noop or XID
markers; `Iterate` fails "WriteBatch has wrong count" on mismatch and "malformed WriteBatch
(too small)" under 12 bytes [R db/write_batch.cc:517-524, :766-771].

**Records** (tag byte, then payload; `[CF]` is a varint32 CF id present only in the CF
variant, which is written only for CF ≠ 0) [decoder R db/write_batch.cc:377-515]:

| Tag (default CF / other CF) | Payload | Encoder |
|---|---|---|
| 0x01 / 0x05 Put | [CF] LP key, LP value | [R :858-892, :1024-1052] |
| 0x00 / 0x04 Delete | [CF] LP key | [R :1281-1307] |
| 0x07 / 0x08 SingleDelete | [CF] LP key | [R :1413+] |
| 0x0F / 0x0E DeleteRange | [CF] LP begin key, LP end key | [R :1549-1570] |
| 0x02 / 0x06 Merge | [CF] LP key, LP operand | [R :1695-1716] |
| 0x11 / 0x10 PutBlobIndex | [CF] LP key, LP BlobIndex (§1.16) | [R :1833-1858] |
| 0x16 / 0x17 PutEntity | [CF] LP key, LP wide-column entity (§1.17) | [R :1105-1145] |
| 0x18 / 0x19 TimedPut | [CF] LP key, LP (value ‖ fixed64 write_unix_time) | [R :894-934; db/seqno_to_time_mapping.cc:521-548] |
| 0x03 LogData | LP blob (not counted, not applied) | [R :1860-1865] |
| 0x0D Noop | none | [R :1190-1193] |
| 0x09 / 0x12 / 0x13 BeginPrepare (by write policy) | none | [R :1195-1212] |
| 0x0A EndPrepare | LP xid | [R :1214-1221] |
| 0x0B Commit | LP xid | [R :1250-1257] |
| 0x15 CommitWithTimestamp | LP commit_ts, LP xid | [R :1259-1270] |
| 0x0C Rollback | LP xid | [R :1272-1279] |

Any other tag: `Corruption("unknown WriteBatch tag")` [R :509-512]. A TimedPut with
`write_unix_time == u64::MAX` is written as a plain Put [R :903-905]. Key size is limited to
`u32::MAX − 8` and value size to `u32::MAX` [R :99-100, :863-865]. Sequence numbers are
assigned to records in order, one per counted record [R :2203-2207]. Per-key protection info
(`protection_bytes_per_key`) lives beside the batch in memory and never enters the bytes
[R include/rocksdb/write_batch.h:492, :537; db/write_batch.cc:187-192].

**DECISION.** The port decodes every tag. It writes Put, Delete, SingleDelete, DeleteRange,
Merge, PutEntity and LogData (mantle's applied index is written as an ordinary Put in the same
batch, note 12 §6.1). On WAL replay (§1.5) a batch holding any XID marker (0x09–0x0C, 0x12,
0x13, 0x15) is refused with `Unsupported { feature: "two-phase commit" }`: those markers come
only from TransactionDB, which is not ported (§2), and applying them correctly needs its
recovery logic. TimedPut and blob index records are decoded and applied (P13).

### 1.5 The log format: WAL and MANIFEST container (db/log_format.h, log_writer.cc, log_reader.cc)

**Blocks.** The file is a sequence of `kBlockSize = 32768` byte blocks [R db/log_format.h:54];
a physical record never crosses a block boundary (a logical record that does not fit is
fragmented), and a block tail shorter than a header is zero-filled
[R db/log_writer.cc:111-128].

**Physical record header** [R db/log_writer.h:37-74; db/log_writer.cc:309-360]:

```
legacy     (kHeaderSize = 7):           crc:fixed32 | length:u16 LE | type:u8 | payload
recyclable (kRecyclableHeaderSize = 11): crc:fixed32 | length:u16 LE | type:u8 | log_number:fixed32 | payload
crc = Mask(crc32c(type ‖ [log_number] ‖ payload))
```

[R db/log_format.h:57, :61; db/log_writer.cc:317-347; db/log_reader.cc:624]. `log_number` is
the low 32 bits of the file number. Length ≤ 0xFFFF [R log_writer.cc:311].

**Record types** [R db/log_format.h:22-52]:

| Value | Type | Header |
|---|---|---|
| 0 | kZeroType (preallocated space) | — |
| 1 / 2 / 3 / 4 | kFullType / kFirstType / kMiddleType / kLastType | legacy |
| 5 / 6 / 7 / 8 | kRecyclableFull / First / Middle / Last | recyclable |
| 9 | kSetCompressionType | legacy (always) |
| 10 / 11 | kUserDefinedTimestampSizeType / recyclable | legacy / recyclable |
| 130 / 131 | kPredecessorWALInfoType / recyclable | legacy / recyclable |

`kRecordTypeSafeIgnoreMask = 1 << 7`: an unknown type with bit 7 set is skipped, otherwise it
is corruption [R db/log_format.h:51; log_reader.cc:339-350]. The legacy header is used for
types < 5 and for 9, 10, 130 [R log_writer.cc:322-326].

**Fragmentation** [R db/log_writer.cc:87-189]: `avail = kBlockSize − block_offset −
header_size`; a record larger than `avail` is split First / Middle… / Last; an empty payload
still emits one Full record. Meta records (9, 10/11, 130/131) are never fragmented: if they do
not fit, the rest of the block is zero-filled first [R :377-398].

**Meta record payloads.**
- kSetCompressionType: `fixed32 compression_type` [R util/compression.h:592-595]; must be the
  first record of the file [R log_writer.cc:191-234; log_reader.cc:173-194]. Only kZSTD
  (0x07) is accepted for WAL compression [R util/compression.h:470-480, :597-612].
- kUserDefinedTimestampSizeType: repeated `fixed32 cf_id ‖ fixed16 ts_sz`; size must be a
  multiple of 6 [R util/udt_util.h:38-66, :80].
- kPredecessorWALInfoType: `fixed64 log_number ‖ fixed64 size_bytes ‖ fixed64
  last_seqno_recorded` (24 bytes) [R db/dbformat.h:1292-1312]; written only with
  `track_and_verify_wals` [R log_writer.cc:236-272].

**WAL compression** (ZSTD streaming, `ZSTD_c_checksumFlag = 1`): the logical record is
compressed as a stream and each compressed chunk (≤ `kBlockSize − header_size` bytes) is one
physical fragment; the reader decompresses fragment by fragment [R db/log_writer.cc:139-156,
:219-227; util/compression.h:710-716; util/compression.cc:190-231; log_reader.cc:646-693].

**Reader** [R db/log_reader.cc:75-354, :508-695]. Physical-read outcomes are the pseudo-types
kEof (132), kBadRecord (133), kBadHeader (134), kOldRecord (135), kBadRecordLen (136),
kBadRecordChecksum (137) [R db/log_reader.h:193-208]:
- A recyclable record whose 32-bit log number differs from the file's is kOldRecord (the tail
  of a recycled file) [R :603-609]; a recyclable record after a non-recyclable one is
  kBadRecord [R :577-580].
- `type == 0 && length == 0` is kBadRecord with nothing dropped (preallocated tail)
  [R :611-619].
- Checksum mismatch drops the rest of the buffered block [R :622-634]; a length past the
  buffer is kBadRecordLen [R :592-601]; a short header at EOF is kBadHeader [R :508-547].
- What each outcome means depends on `WALRecoveryMode` [R include/rocksdb/options.h:411-448]:
  kTolerateCorruptedTailRecords (0), kAbsoluteConsistency (1), kPointInTimeRecovery (2, the
  default [R options.h:1480]), kSkipAnyCorruptedRecords (3); the state machine is
  [R db/log_reader.cc:238-350].
- When the caller asks for it, the reader also returns an XXH3 of each logical record
  (`XXH3_64bits` over its fragments, streamed for fragmented records); this is an in-memory
  check, not a stored field [R db/log_reader.cc:101-171].

**MANIFEST** files use the same writer with log number 0, no recycling, no compression
[R db/version_set.cc:6457-6474]; each record is one encoded VersionEdit (§1.14).

**DECISION.** Port reader and writer completely, including recyclable records (a RocksDB
directory with `recycle_log_file_num > 0` must be readable), ZSTD WAL decompression, and all
four recovery modes. The port writes WALs only in fidelity tests; mantle's configuration is
`disableWAL` on every write, and the port rejects `sync && disableWAL` as RocksDB does
[R db/db_impl/db_impl_write.cc:941-943].

### 1.6 Blocks (table/block_based/block_builder.cc, block.cc, block_util.h, data_block_footer.*)

Every block the block-based table writes with `BlockBuilder` — data, index, index partitions,
top-level partition index, filter partition index, meta-index, properties, range-deletion —
has this shape [R table/block_based/block_builder.cc:21-36, :189-220]:

```
entry*                            each: varint32 shared | varint32 non_shared | varint32 value_len
                                        | key_delta[non_shared] | value[value_len]
[values section]                  only if separated KV (bit 28)
restart[num_restarts]             fixed32 offsets of restart entries, first = 0
[data-block hash index]           only if bit 31
[fixed32 values_section_offset]   only if bit 28
fixed32 packed footer             bits 0-27 num_restarts; 28 separated KV; 29 uniform keys;
                                  30 reserved (needs a format bump); 31 hash index present
```

[R block_builder.cc:105, :199-211; data_block_footer.h:20-34, :56-64;
data_block_footer.cc:17-29, :44-100].

- **Restarts.** `shared = 0` at every restart; a restart every `block_restart_interval`
  entries (default 16 for data blocks, 1 for index blocks) [R include/rocksdb/table.h:413,
  :416; block_builder.cc:294-303]. Meta-index and range-deletion blocks use interval 1, the
  properties block `i32::MAX` (one restart) [R table/meta_blocks.cc:35-36, :55-57;
  block_based_table_builder.cc:1107-1113]. `kMaxNumRestarts = 2^28 − 1`; a footer word whose
  remainder after stripping known bits exceeds it is `Corruption("Unrecognized feature in
  block footer (reserved bits set)")` [R data_block_footer.cc:79-82].
- **Index blocks with value delta encoding** (fv ≥ 4, §1.10) omit `value_len`: entries are
  `shared | non_shared | key_delta | value` [R block_builder.cc:316-332; block_util.h:78-121].
- **Separated KV** (`separate_key_value_in_data_block`, default false, data blocks only)
  stores values in a trailing section; the header of an interval's first entry carries a
  fourth varint `value_offset`, later values follow the previous one; the reader needs the
  data restart interval from the property `rocksdb.data.block.restart.interval`
  [R include/rocksdb/table.h:741-748; block_builder.cc:316-352; block.cc:545-603;
  block_based_table_reader.cc:1175-1180].
- **Uniform-keys bit** is set only on index blocks when `uniform_cv_threshold ≥ 0`
  [R include/rocksdb/table.h:750-764; block_builder.cc:406-441].
- **Data-block hash index** (`data_block_index_type = kDataBlockBinaryAndHash`)
  [R table/block_based/data_block_hash_index.h:21-72; data_block_hash_index.cc:16-96]:
  `uint8 bucket[num_buckets] ‖ fixed16 num_buckets`; `num_buckets = max(1, estimate) | 1`
  (odd) with `estimate = keys / util_ratio` (default 0.75); bucket = `GetSliceHash(user_key) %
  num_buckets`; a bucket holds the restart index (≤ 253), `kCollision = 254` or `kNoEntry =
  255`. The index is attached only when the block is ≤ 64 KiB, and not when the comparator
  can equate different bytes [R block_builder.cc:207-208;
  block_based_table_builder.cc:1099-1102]. On lookup, `kNoEntry` means "not in this block"
  and `kCollision` falls back to binary search [R block.cc:246-256].

**DECISION.** Port reader and writer for all four features. Separated KV and the uniform bit
are written only if the corresponding option is set; the port's default writes neither,
matching RocksDB's defaults.

### 1.7 Block handles and the block trailer

- **BlockHandle** = `varint64 offset ‖ varint64 size`, at most 20 bytes
  [R table/format.cc:50-64; format.h:83]. `size` excludes the trailer.
- **Trailer** (`kBlockTrailerSize = 5`) after every block: `u8 compression_type ‖ fixed32
  checksum` [R table/block_based/block_based_table_reader.h:132-133, :441-444]. The checksum
  covers the (possibly compressed) block bytes **and** the compression-type byte, plus the
  context modifier:

```
checksum = ComputeBuiltinChecksumWithLastByte(type, block, size, compression_type)
           + ChecksumModifierForContext(base_context_checksum, block_offset)
```

[R block_based_table_builder.cc:2173-2176, :2254-2274]. The reader recomputes over
`size + 1` bytes and compares after subtracting the modifier [R
table/block_based/reader_common.cc:26-64]. The checksum type is the footer's (§1.9).

**DECISION.** Verification on every block read is unconditional in the port (CLAUDE.md §6):
`ReadOptions::verify_checksums` (default true in RocksDB [R include/rocksdb/options.h:2211])
is not a switch the port offers.
A mismatch is `Corruption { file, offset, kind: BlockChecksum }` and feeds repair.

### 1.8 Compression framing

**Types** [R include/rocksdb/compression_type.h:18-167]: 0x00 none, 0x01 Snappy, 0x02 Zlib,
0x03 BZip2, 0x04 LZ4, 0x05 LZ4HC, 0x06 Xpress, 0x07 ZSTD; 0x08–0x7F reserved; 0x80–0xFE
custom (fv 7 with a CompressionManager); 0xFF "disable" (an option value, never on disk).

**Framing of a compressed block** (fv ≥ 2, the only versions readable) [R include/rocksdb/
table.h:706-709; util/compression.cc:553-570]:

| Type | Bytes after the block handle's offset |
|---|---|
| Snappy | raw Snappy (its own varint length header) [R util/compression.cc:495-547] |
| Xpress | raw Xpress, no prefix [R :1046-1077] |
| Zlib | varint32 uncompressed size ‖ raw deflate, `window_bits = −14` [R :593, :617, :1293] |
| BZip2 | varint32 uncompressed size ‖ bzip2 stream (`BZ2_bzCompressInit(1, 0, 30)`) [R :692, :711] |
| LZ4 / LZ4HC | varint32 uncompressed size ‖ LZ4 block (dictionary via `LZ4_loadDict`) [R :791, :814-822, :1374-1381] |
| ZSTD | varint32 uncompressed size ‖ ZSTD frame (dictionary via `ZSTD_CCtx_refCDict`/`loadDictionary`) [R :1162, :1179-1189] |

The reader takes the prefix as a varint64 (a generalization; writers emit varint32)
[R util/compression.cc:386-404].

**When a block stays uncompressed.** Only data and index blocks are compressed; filter,
properties, range-deletion, dictionary and meta-index blocks are always type 0x00
[R block_based_table_builder.cc:1925-1926, :2119-2123, :2640-2648]. A compressed result is
kept only if it fits in `(max_compressed_bytes_per_kb × size) >> 10` bytes, default 896 per
KiB (clamped ≤ 1023); otherwise the block is stored uncompressed and counted as rejected
[R include/rocksdb/compression_type.h:307-317; block_based_table_builder.cc:1191-1192,
:2055-2068]. Blocks ≥ `i32::MAX` bytes are never compressed [R block_based_table_builder.h:224].

**Compression dictionary** (`max_dict_bytes > 0`): data blocks are buffered, sampled from
index N/2 with stride `545055921143 % N`, and the dictionary is either the raw samples or
ZSTD-trained (`zstd_max_train_bytes > 0`); it is stored uncompressed as meta block
`rocksdb.compression_dict` [R block_based_table_builder.cc:2831-2851, :2932-2969;
util/compression.cc:1243-1258; table/meta_blocks.cc:32].

**Parallel compression** (`parallel_threads > 1`, default 1) produces the same bytes as the
serial path; order is preserved by a ring buffer [R include/rocksdb/compression_type.h:262;
block_based_table_builder.cc:350-363, :2313-2328].

**Property `rocksdb.compression`** (fv ≥ 7): `<compatibility_name>;<sorted hex types>;`, with
the built-in manager named `BuiltinV2` and two uppercase hex digits per type used; fv < 7: the
legacy type name [R block_based_table_builder.cc:1479-1562; util/compression.cc:42-70, :1750;
include/rocksdb/table_properties.h:396-410].

**DECISION.** Read: none, Snappy, LZ4, LZ4HC, ZSTD (with and without dictionary), Zlib.
BZip2 and Xpress, and custom types 0x80–0xFE, are refused per block with
`Unsupported { feature: "compression type N" }` (Xpress is Windows-only in RocksDB; BZip2 is
rarely configured) — a table using them cannot be read, which the corpus check of §3.0 makes
visible. Write: none, LZ4, ZSTD. Codecs and their build consequences are §5 R9. Parallel
compression is not ported (identical bytes, so no fidelity loss).

### 1.9 Footer, magic numbers and format_version

**Footer** is the last 53 bytes (fv ≥ 1) [R table/format.cc:185-225; format.h:296-306]:

```
fv 2..5 (53 bytes):  checksum_type:u8 | metaindex BlockHandle | index BlockHandle
                     | zero pad to 40 bytes (unchecked) | format_version:fixed32 | magic:fixed64
fv 6..7 (53 bytes):  checksum_type:u8 | 3e 00 7a 00 | footer_checksum:fixed32
                     | base_context_checksum:fixed32 | metaindex_size:fixed32
                     | 24 zero bytes (last 8 must be 0) | format_version:fixed32 | magic:fixed64
```

- fv ≥ 6: the meta-index block must end exactly where the footer's trailer-adjusted offset
  begins, so its handle is `(footer_offset − 5 − metaindex_size, metaindex_size)`; the index
  handle moves into the meta-index under `rocksdb.index` [R format.cc:280-314, :440-458;
  format.h:222-224; block_based_table_reader.cc:3155-3158].
- `footer_checksum` = `ComputeBuiltinChecksum(type, the 53 bytes with this field zero) +
  ChecksumModifierForContext(base, footer_offset)` [R format.cc:308-314]; decode rejects a
  base whose modifier at offset 0 would be 0 [R :428-430].
- The reader reads the last `min(53, file_size)` bytes; minimum 48 [R format.cc:520-560].

**Magic numbers** (stored fixed64 LE):

| Magic | Meaning | Source |
|---|---|---|
| `0x88e241b785f4cff7` | block-based table (`echo rocksdb.table.block_based \| sha1sum`) | [R table/block_based/block_based_table_builder.cc:138-146] |
| `0xdb4775248b80fb57` | legacy block-based (fv 0) — refused, NotSupported | [R table/format.cc:344-348] |
| `0x8242229663bf9564`, `0x4f3418eb7a8f13b8` | plain table, legacy plain table | [R table/plain/plain_table_builder.cc:55-56] |
| `0x926789d0c5f17873` | cuckoo table | [R table/cuckoo/cuckoo_table_builder.cc:47] |

**format_version** [R include/rocksdb/table.h:702-731; table/format.h:172-228]. Readable and
writable: 2–7 (`kMinSupportedBbtFormatVersionForRead = ForWrite = 2`,
`kLatestBbtFormatVersion = 7`); default 7 [R table.h:739]; writes below 2 are raised to 2 and
above 7 rejected [R block_based_table_factory.cc:515-525, :673-681].

| fv | What changes | Where the code branches |
|---|---|---|
| 2 | varint32 size prefix for Zlib/BZip2/LZ4/LZ4HC/ZSTD (§1.8) | [R util/compression.cc:553-570] |
| 3 | index keys may be user keys (`index_key_is_user_key`) | [R table/block_based/index_builder.h:257] |
| 4 | index value delta encoding | [R block_based_table_builder.cc:1119-1122] |
| 5 | FastLocalBloom replaces legacy bloom for new filters | [R table/block_based/filter_policy.cc:1486-1490] |
| 6 | context checksums, new footer with checksum, index handle in meta-index | [R table/format.h:218-224] |
| 7 | `rocksdb.compression` property schema; custom compression allowed | [R format.h:226-228; block_based_table_builder.cc:1479-1497] |

**DECISION.** Read fv 2–7; write fv 7 by default and any of 2–7 on request (the corpus of §3.0
exercises each). Plain, cuckoo and legacy magics are refused with a typed error naming the
format.

### 1.10 Index blocks

**Index types** [R include/rocksdb/table.h:298-326]: kBinarySearch 0x00 (default),
kHashSearch 0x01, kTwoLevelIndexSearch 0x02, kBinarySearchWithFirstKey 0x03. The reader learns
the type from the property `rocksdb.block.based.table.index.type` (fixed32) and fails without
it [R block_based_table_builder.cc:178-181; block_based_table_reader.cc:1213-1218].

- **Keys.** Index entry key = a separator ≥ the last key of block i and < the first key of
  block i+1, shortened by default (`index_shortening = kShortenSeparators`); fv ≤ 2 always uses
  internal keys; fv ≥ 3 uses user keys unless two adjacent blocks share a user key, and the
  choice is recorded in `rocksdb.index.key.is.user.key` [R include/rocksdb/table.h:820-831;
  index_builder.h:182-196, :257, :348-354, :437-449; index_builder.cc:78-120;
  block_based_table_builder.cc:2745-2746]. When shortening yields a physically shorter but
  logically larger user key, `fixed64(Pack(kMaxSequenceNumber, kValueTypeForSeek))` is
  appended [R index_builder.cc:78-120].
- **Values** (`IndexValue`) [R table/format.cc:102-145]: full form `BlockHandle`; delta form
  (fv ≥ 4, only for entries with `shared ≠ 0`, not with `block_align` or embedded blobs)
  `varsigned64(size − prev.size)` with offset implied as `prev.offset + prev.size + 5`; with
  first key (kBinarySearchWithFirstKey) a trailing `LP first_internal_key`
  [R block_builder.cc:339-342; block.cc:652-673; block_based_table_builder.cc:1119-1122,
  :2220-2224]. Recorded in `rocksdb.index.value.is.delta.encoded`.
- **Hash index** (kHashSearch, needs a prefix extractor; forces index restart interval 1): the
  binary index plus meta blocks `rocksdb.hashindex.prefixes` (prefixes concatenated) and
  `rocksdb.hashindex.metadata` (per prefix **three varint32**: prefix length, restart index,
  number of blocks — the header comment's "4 bytes" is wrong)
  [R index_builder.h:509-522, :602-638; block_prefix_index.cc:171-191;
  block_based_table_factory.cc:489-494, :1153-1155].
- **Partitioned index** (kTwoLevelIndexSearch): index partitions cut at
  `metadata_block_size` (default 4096) with `block_size_deviation`; a top-level index maps each
  partition's last separator to its handle (delta-encoded sizes); partitions are written in
  order and the top level last, and the footer/meta-index handle points to the top level
  [R index_builder.cc:179-213, :314-373; include/rocksdb/table.h:423;
  block_based_table_builder.cc:2665-2691]. Properties `rocksdb.index.partitions`,
  `rocksdb.top-level.index.size`.
- **User-defined index** (`rocksdb.user_defined_index.<name>` meta block;
  `rocksdb.udi.is.primary.index`) [R include/rocksdb/user_defined_index.h:24-25;
  table/table_properties.cc:328-329]: the experimental trie index registers here
  [R block_based_table_factory.cc:1138].

**DECISION.** Port all four built-in index types. A table whose UDI is primary is refused
(`Unsupported { feature: "user-defined index" }`); a secondary UDI block is ignored, as a
RocksDB without that factory ignores it.

### 1.11 Filters

**Meta-index key** = prefix + `CompatibilityName()`, which is `rocksdb.BuiltinBloomFilter` for
every built-in policy: `fullfilter.rocksdb.BuiltinBloomFilter` or
`partitionedfilter.rocksdb.BuiltinBloomFilter` [R block_based_table_builder.cc:2612-2619,
:3271-3274; filter_policy.cc:1418-1422]. The old per-block filter (`filter.` prefix) is no
longer built; on read it is treated as no filter [R block_based_table_reader.cc:1636-1657].
Property `rocksdb.filter.policy` holds `Name()`: `bloomfilter` / `ribbonfilter`
[R filter_policy.cc:1493-1494, :1884-1886; block_based_table_builder.cc:2709-2712].

**Filter block trailer** (`kMetadataLen = 5`); the reader dispatches on the signed byte at
`len − 5` [R filter_policy.cc:43-59, :1649-1728]:

| Condition | Meaning |
|---|---|
| total length ≤ 5 | always false (an empty filter is 0 bytes) |
| byte = 0 | always true (`"\0\0\0\0\0\0"`) |
| byte = −1 | FastLocalBloom |
| byte = −2 | Standard128Ribbon |
| other negative | always true (reserved) |
| 1..127 | legacy bloom, byte = num_probes |

- **Legacy bloom** (fv < 5): `bits ‖ u8 num_probes ‖ fixed32 num_lines`; hash `BloomHash`
  (legacy `Hash`, seed 0xbc9f1d34); `num_probes = ⌊bits_per_key × 0.69⌋` clamped 1..30; lines
  of `CACHE_LINE_SIZE` bytes, an odd count; probe: line = `h % num_lines`, `delta = rotr(h,
  17)`, bit = `h & (line_bits − 1)`, `h += delta` [R filter_policy.cc:1077, :1149-1158,
  :1216-1260; util/bloom_impl.h:356-362, :404-487]. `CACHE_LINE_SIZE` is 64 by default but
  128 on aarch64 and ppc, 256 on s390 [R port/port_posix.h:190-207]; the reader infers the
  line size from `len / num_lines` [R filter_policy.cc:1703-1727]. **A legacy filter written
  on aarch64 has 128-byte lines**; the port writes with the line size of the build RocksDB
  would use on the same target, and reads any power-of-two line size.
- **FastLocalBloom** (fv ≥ 5): body a multiple of 64 bytes; trailer `[−1][0][probes][0][0]`
  where the top 3 bits of the third byte encode log2(block bytes) − 6 (only 0, 64-byte blocks)
  and the low 5 bits `num_probes` 1..30; hash `GetSliceHash64` (XXPH3, §1.2), adjacent
  duplicates dropped; `h1 = Lower32`, `h2 = Upper32`; cache line =
  `FastRange32(h1, len >> 6) << 6`; per probe `bit = h2 >> 23`, set bit `bit & 7` of byte
  `bit >> 3`, `h2 *= 0x9e3779b9`; probes chosen from `millibits_per_key =
  ⌊bits_per_key × 1000 + 0.500001⌋` by table [R filter_policy.cc:76-86, :440-516, :1440,
  :1768-1818; util/bloom_impl.h:156-214].
- **Standard128Ribbon**: trailer `[−2][seed u8][num_blocks u24 LE]`; `num_blocks < 2` means
  always true; 128-bit coefficient rows, first coefficient always one, no smash, 32-bit result
  rows, interleaved solution; rehash `(hash ^ raw_seed) × 0x6193d459236a3a0d`; hash
  `GetSliceHash64`; falls back to FastLocalBloom above `kMaxRibbonEntries = 950000000` keys or
  if solving fails; `bloom_before_level` picks Bloom for levels below it
  [R filter_policy.cc:637-653, :687-763, :807-818, :1012, :1737-1758, :1840-1882;
  util/ribbon_impl.h:215-435, :819-1110].
- **What is added.** With a prefix extractor and `whole_key_filtering` (default true): key and
  prefix; prefix only if whole-key filtering is off; out-of-domain keys only if whole-key
  filtering is on; always user keys without timestamp [R table/block_based/full_filter_block.cc:
  77-89; include/rocksdb/table.h:662; block_based_table_builder.cc:1664-1668].
- **Partitioned filters**: partitions cut at `metadata_block_size × (100 −
  block_size_deviation) / 100`; with `decouple_partitioned_filters` (default true) a
  partition's key is `prev_user_key ‖ footer(seq 0, kTypeDeletion)`, otherwise the index
  partition key; the next prefix is added before a cut and the previous after; a top-level
  index (BlockHandle values, delta sizes) is written after the partitions and the meta-index
  points to it [R partitioned_filter_block.cc:106-161, :276-345;
  block_based_table_builder.cc:103-108, :2582-2619; include/rocksdb/table.h:539].

**DECISION.** Port all three readers and all three writers; the writer default is
FastLocalBloom at fv ≥ 5, as RocksDB's. Filters are recomputed from the same keys in the
fidelity tests and must be byte-identical (§3 P5).

### 1.12 Meta-index, properties, range deletions, unique id

**Meta-index** [R table/meta_blocks.cc:29-50; util/kv_map.h:17-31]: a block with restart
interval 1, keys sorted bytewise, value = the named block's BlockHandle, never compressed.
Possible keys: `rocksdb.properties`, `rocksdb.index` (fv ≥ 6), `rocksdb.compression_dict`,
`rocksdb.range_del`, `rocksdb.hashindex.prefixes`, `rocksdb.hashindex.metadata`, the filter key
(§1.11), `rocksdb.user_defined_index.<name>`.

**Properties block** [R table/meta_blocks.cc:55-213, :252-284, :371-393]: one block, one
restart, keys sorted bytewise (the reader requires strict order); integer properties are
varint64, string properties raw bytes; user-collected properties are merged into the same
block. Names [R table/table_properties.cc:311-395]:

| Property | Type | Written when |
|---|---|---|
| rocksdb.creating.db.identity | string | non-empty |
| rocksdb.creating.session.identity | string (20 base-36 chars, [R table/unique_id.cc:15-57]) | non-empty |
| rocksdb.creating.host.identity | string | non-empty |
| rocksdb.original.file.number | varint64 | always |
| rocksdb.data.size, rocksdb.index.size, rocksdb.filter.size | varint64 | always |
| rocksdb.index.partitions, rocksdb.top-level.index.size | varint64 | partitioned index |
| rocksdb.index.key.is.user.key, rocksdb.index.value.is.delta.encoded | varint64 | always |
| rocksdb.udi.is.primary.index | varint64 | non-zero |
| rocksdb.raw.key.size, rocksdb.raw.value.size | varint64 | always |
| rocksdb.num.data.blocks, rocksdb.num.entries, rocksdb.num.filter_entries | varint64 | always |
| rocksdb.num.data.blocks.compression.rejected / .bypassed | varint64 | > 0 |
| rocksdb.num.uniform.blocks | varint64 | always |
| rocksdb.deleted.keys, rocksdb.merge.operands, rocksdb.num.range-deletions | varint64 | always |
| rocksdb.format.version, rocksdb.fixed.key.length, rocksdb.column.family.id | varint64 | always |
| rocksdb.column.family.name, rocksdb.comparator, rocksdb.merge.operator, rocksdb.prefix.extractor.name, rocksdb.property.collectors, rocksdb.filter.policy, rocksdb.compression, rocksdb.compression_options | string | non-empty |
| rocksdb.creation.time, rocksdb.oldest.key.time, rocksdb.newest.key.time | varint64 | always |
| rocksdb.file.creation.time, rocksdb.sample_for_compression.slow.data.size / .fast.data.size | varint64 | > 0 |
| rocksdb.seqno.time.map | string | non-empty |
| rocksdb.tail.start.offset | varint64 | always |
| rocksdb.user.defined.timestamps.persisted | varint64 | only when 0 |
| rocksdb.key.largest.seqno, rocksdb.key.smallest.seqno | varint64 | ≠ u64::MAX |
| rocksdb.data.block.restart.interval, rocksdb.index.block.restart.interval, rocksdb.separate.key.value.in.data.block | varint64 | > 0 |

Conditions are [R table/meta_blocks.cc:79-196]. Internal user-collected properties:
`rocksdb.block.based.table.index.type` (fixed32), `…whole.key.filtering`, `…prefix.filtering`,
`…decoupled.partitioned.filters` ("1"/"0") [R block_based_table_factory.cc:1144-1157;
block_based_table_builder.cc:178-191]; `rocksdb.external_sst_file.version` (fixed32, value 2)
and `rocksdb.external_sst_file.global_seqno` (fixed64) from SstFileWriter
[R table/sst_file_writer.cc:27-30, :388; table/sst_file_writer_collectors.h:47-58];
`rocksdb.timestamp_min/max` [R db/table_properties_collector.h:165-166];
`rocksdb.embedded.blob.stats` (two varint64) [R table/embedded_blob_sst.h:35-78].

Ingestion reads `external_sst_file.version`: absent → corruption unless
`allow_db_generated_files`; version 2 requires `global_seqno` and, with `write_global_seqno`,
the property's byte offset is rewritten in place [R db/external_sst_file_ingestion_job.cc:
1015-1064].

**Range-deletion block** `rocksdb.range_del` [R block_based_table_builder.cc:1107-1113,
:1687-1698, :2854-2862; db/dbformat.h:1127-1130]: restart interval 1, never compressed; entry
key = internal key `(start_user_key, seq, kTypeRangeDeletion)`, value = end user key
(exclusive). Flush and compaction write fragmented tombstones [R db/builder.cc:336-342;
db/compaction/compaction_outputs.cc:731]; SstFileWriter writes them unfragmented with seq 0
[R table/sst_file_writer.cc:237-240]; the reader fragments on load in every case
[R block_based_table_reader.cc:1620-1623].

**Unique id** (for the MANIFEST's kUniqueId and for cache keys): from `db_id`,
`db_session_id` (base-36 decode of two u64 halves), `orig_file_number`: `[0] = session_lower`,
`(a, b) = Hash2x64(db_id, seed = session_upper)`, `[1] = a ^ file_number`, `[2] = b`; the
external form is `BijectiveHash2x64` with offsets 17391078804906429400 / 6417269962128484497,
serialized as fixed64 LE pairs [R table/unique_id.cc:15-161]. `Hash2x64` is the released
XXH3-128 [R util/hash.cc:105-128].

**DECISION.** Write the same property set with the same conditions. Time-valued properties
(`creation.time`, `oldest.key.time`, `file.creation.time`), `creating.host.identity` and the
session id come from an injected clock and identity source, so a fidelity test can make them
equal on both sides (§3.0). `seqno.time.map` is parsed and preserved on compaction but not
generated (the option that produces it, `preserve_internal_time_seconds`, is not ported).

### 1.13 Order of a table's blocks

`BlockBasedTableBuilder::Finish` writes [R block_based_table_builder.cc:3091-3126]:

1. data blocks (each with trailer; `block_align` pads each to `min(block_size, 4096)` and is
   incompatible with compression; `super_block_alignment_size` may pad before a block that
   would straddle a super block) [R :1092-1095, :1449-1455, :2198-2234, :2291-2304];
2. `tail_start_offset` is recorded here [R :3100];
3. filter: partitions then top-level index, or the full filter;
4. index: hash-index meta blocks, then the index, or partitions then the top level;
5. compression dictionary;
6. range-deletion block;
7. properties;
8. meta-index;
9. footer.

**DECISION.** Same order; `block_align` and super-block alignment are ported as readable
(nothing to do on read) and not written.

### 1.14 MANIFEST: VersionEdit records (db/version_edit.h, db/version_edit.cc)

A MANIFEST is a log-format file (§1.5) whose every logical record is one `VersionEdit`. A
record is a sequence of `varint32 tag ‖ payload`.

**Tags** [R db/version_edit.h:37-78]. Tags with bit `kTagSafeIgnoreMask = 1 << 13` (8192) are
"forward compatible": an unknown one is skipped by reading a varint32 length and that many
bytes; any other unknown tag is `Corruption("VersionEdit", "unknown tag")`
[R db/version_edit.cc:928-954].

| Tag | Value | Payload | Written by EncodeTo [R version_edit.cc:140-268] |
|---|---|---|---|
| kComparator | 1 | LP name | yes (with kPersistUserDefinedTimestamps) |
| kLogNumber | 2 | varint64 | yes |
| kNextFileNumber | 3 | varint64 | yes |
| kLastSequence | 4 | varint64 | yes |
| kCompactCursor | 5 | varint32 level, LP internal key | yes |
| kDeletedFile | 6 | varint32 level, varint64 file number | yes |
| kNewFile | 7 | level, number, size, LP smallest, LP largest | read only |
| kPrevLogNumber | 9 | varint64 | yes |
| kMinLogNumberToKeep | 10 | varint64 | yes |
| kNewFile2 | 100 | kNewFile + varint64 smallest_seq, largest_seq | read only |
| kNewFile3 | 102 | level, number, **varint32 path_id**, size, keys, seqnos | read only |
| kNewFile4 | 103 | below | yes |
| kColumnFamily | 200 | varint32 CF id (only written when ≠ 0) | yes |
| kColumnFamilyAdd | 201 | LP name | yes |
| kColumnFamilyDrop | 202 | none | yes |
| kMaxColumnFamily | 203 | varint32 | yes |
| kInAtomicGroup | 300 | varint32 remaining entries | yes |
| kBlobFileAddition | 400 | BlobFileAddition body (not length-prefixed) | yes |
| kBlobFileGarbage | 401 | BlobFileGarbage body | yes |
| kDbId | 8193 | LP id | yes |
| kBlobFileAddition_DEPRECATED / kBlobFileGarbage_DEPRECATED | 8194 / 8195 | as 400/401 | read only |
| kWalAddition / kWalDeletion | 8196 / 8197 | WalAddition / WalDeletion, not length-prefixed | read only |
| kFullHistoryTsLow | 8198 | LP (non-empty) | yes |
| kWalAddition2 / kWalDeletion2 | 8199 / 8200 | LP(WalAddition) / LP(WalDeletion) | yes |
| kPersistUserDefinedTimestamps | 8201 | LP of exactly 1 byte | yes |
| kSubcompactionProgress | 8202 | LP(SubcompactionProgress) | yes (resumable compaction) |
| kLastCompactedManifestFileSize | 8203 | LP(varint64) | yes |

Decode case lines are [R db/version_edit.cc:606-917]; `DecodeFrom` starts with `Clear()`, so
the CF id defaults to 0 in every record, and leftover input is "invalid tag" [R :584, :948-949].

**kNewFile4** [R db/version_edit.cc:270-407, :431-566]:

```
varint32 103 | varint32 level | varint64 file_number | varint64 file_size
| LP smallest_internal_key | LP largest_internal_key
| varint64 smallest_seqno | varint64 largest_seqno
| { varint32 custom_tag | LP field }*  | varint32 kTerminate (1)
```

Custom fields, in write order [R db/version_edit.h:96-124; version_edit.cc:307-402]:

| Tag | Value | Field | Written when |
|---|---|---|---|
| kOldestAncesterTime | 5 | varint64 | always |
| kFileCreationTime | 6 | varint64 | always |
| kEpochNumber | 13 | varint64 | always (encode fails if unknown) |
| kFileChecksum, kFileChecksumFuncName | 7, 8 | raw bytes | func name ≠ "Unknown" |
| kPathId | 65 | 1 byte (0..3) | ≠ 0 |
| kTemperature | 9 | 1 byte | ≠ kUnknown |
| kNeedCompaction | 2 | 1 byte = 1 | marked |
| kMinLogNumberToKeepHack | 3 | fixed64 | once per edit, when the edit carries it |
| kOldestBlobFileNumber | 4 | varint64 | ≠ 0 |
| kUniqueId | 12 | 16 bytes (two fixed64) | present |
| kCompensatedRangeDeletionSize | 14 | varint64 | ≠ 0 |
| kTailSize | 15 | varint64 | ≠ 0 |
| kUserDefinedTimestampsPersisted | 16 | 1 byte = 0 | only when false |
| kMinTimestamp, kMaxTimestamp | 10, 11 | raw | non-empty |
| kFileOpenMetadata | 17 | raw | non-empty |

An unknown custom tag with bit `kCustomTagNonSafeIgnoreMask = 1 << 6` (64) fails "new-file4
custom field not supported"; others are skipped [R version_edit.cc:551-555]. `kPathId` (65)
has that bit, so a reader that does not know it fails. `kFileNumberMask =
0x3FFFFFFFFFFFFFFF`; path id is not packed into the on-disk number [R db/version_edit.h:128-135,
:219-225; version_edit.cc:60-63, :273, :334-338, :466]. Temperature bytes ≥ kLastTemperature
are ignored [R :510-519].

**Nested encodings.**
- BlobFileAddition: `varint64 number, varint64 total_blob_count, varint64 total_blob_bytes, LP
  checksum_method, LP checksum_value, {tag, LP}* , varint32 0`; a custom tag with bit 1 << 6
  is corruption [R db/blob/blob_file_addition.cc:21-97]. BlobFileGarbage: `varint64 number,
  varint64 garbage_blob_count, varint64 garbage_blob_bytes, varint32 0` [R
  db/blob/blob_file_garbage.cc:21-86].
- WalAddition: `varint64 log_number, [varint32 2 (kSyncedSize), varint64 size], varint32 1`;
  any unknown tag is corruption. WalDeletion: `varint64 log_number` [R db/wal_edit.h:49-71,
  :117-123; db/wal_edit.cc:14-89].
- SubcompactionProgress and its per-level progress use their own tags with a safe-ignore bit
  `1 << 16` [R db/version_edit.h:80-94; version_edit.cc:1271-1497].

**A new MANIFEST's first records** (`WriteCurrentStateToManifest`) [R db/version_set.cc:
7098-7297]: (1) kDbId if `write_dbid_to_manifest` (default true [R options.h:1613]); (2) all
tracked WAL additions; (3) a WAL deletion before `min_log_number_to_keep`; (4) per live CF an
edit with kColumnFamilyAdd + kColumnFamily (non-default CFs) and kComparator +
kPersistUserDefinedTimestamps; (5) per live CF an edit with kColumnFamily, kNewFile4 for every
file, compact cursors, blob additions/garbage, kLogNumber, kMinLogNumberToKeep (default CF),
kFullHistoryTsLow, kLastSequence; (6) kLastCompactedManifestFileSize. A brand-new DB's
MANIFEST-000001 holds one record: kDbId, LogNumber 0, NextFile 2, LastSequence 0, no
comparator [R db/db_impl/db_impl_open.cc:329-381].

**Writing an edit** (`LogAndApply` → `ProcessManifestWrites`) [R db/version_set.cc:5635-6302]:
a new MANIFEST is started when none is open or the current one is ≥ the tuned limit
`max(max_manifest_file_size, last_compacted_size × (100 + max_manifest_space_amp_pct) /
100)` (defaults 1 GiB and 500) [R :5599-5604, :5877-5889; options.h:1015, :1094]; records are
appended, the MANIFEST is synced, and only then, if the MANIFEST is new, CURRENT is switched
(§1.15) [R version_set.cc:5961-6064; file/filename.cc:522-533]. Atomic groups carry
`kInAtomicGroup` counting down to 0; recovery buffers them and applies all or none
[R version_set.cc:5285-5317; db/version_edit_handler.cc:24-98].

**Recovery** reads CURRENT (must end in `\n` and name a `MANIFEST-` file)
[R db/manifest_ops.cc:13-45], then every edit [R db/version_set.cc:6548-6634]; it requires
log number, next file number and last sequence to have appeared, and sets
`next_file_number = NextFile + 1` [R db/version_edit_handler.cc:373-492]. An edit naming an
unknown CF, a duplicate add or an unknown drop is corruption [R :243-326]. CF records:
kColumnFamily is per edit, not a mode switch; the default CF (id 0, "default") exists
implicitly; `___rocksdb_stats_history___` is opened implicitly if present
[R db/db_impl/db_impl.cc:140-142; version_edit_handler.cc:196-300].

**DECISION.** Decode every tag; write what RocksDB writes except kSubcompactionProgress
(resumable compaction is not ported) and kTemperature/kPathId (single path, no tiering).
Checked conditions RocksDB leaves to debug asserts (atomic-group accounting at
[R version_set.cc:5817-5853], `EncodeTo` returning false on an invalid key) are typed errors.

### 1.15 CURRENT, IDENTITY, OPTIONS, LOCK and file names (file/filename.cc)

**Names** [R file/filename.cc:26-34, :66-274; include/rocksdb/types.h:45-58]: numbered files
use `%06llu` (at least six digits, not truncated): `NNNNNN.log` (WAL), `NNNNNN.sst` (`.ldb`
accepted on read), `NNNNNN.blob`, `NNNNNN.dbtmp` (temp), `archive/NNNNNN.log`;
`MANIFEST-NNNNNN`; `OPTIONS-NNNNNN` (and `.dbtmp`); `CURRENT`; `LOCK`; `IDENTITY`; `LOG`,
`LOG.old.<micros>`; `COMPACTION_PROGRESS-<ts>`; `METADB-<n>`. `ParseFileName` classifies
each [R :290-427].

**CURRENT update** (`SetCurrentFile`) [R file/filename.cc:429-467]: write
`"MANIFEST-%06llu\n"` to `<descriptor_number>.dbtmp`, sync it, rename to `CURRENT`, fsync the
directory; delete the temp file on failure.

**IDENTITY** [R file/filename.cc:469-520; db/db_impl/db_impl.cc:5479-5495; env/env.cc:
861-899]: the DB id — an RFC 4122 v4 UUID string, lowercase, 36 characters — written with no
trailing newline via `000000.dbtmp`, sync, rename, directory fsync; readers strip one trailing
`\n` (older versions wrote one). The same id is kDbId in the MANIFEST; both
`write_dbid_to_manifest` and `write_identity_file` default true and may not both be false
[R include/rocksdb/options.h:1613, :1620; db/db_impl/db_impl_open.cc:322-325].

**OPTIONS file** [R options/options_parser.cc:30-155, :175-661; options/options_parser.h:
21-33]: text, written after open and after option changes as `OPTIONS-<n>.dbtmp`, synced,
renamed to `OPTIONS-<m>` (two kept) [R db/db_impl/db_impl.cc:5756-5930]:

```
# This is a RocksDB option file.
#
# For detailed file format spec, please refer to the example file
# in examples/rocksdb_option_file_example.ini
#

[Version]
  rocksdb_version=11.8.1
  options_file_version=1.1

[DBOptions]
  name=value
  ...

[CFOptions "default"]
  ...

[TableOptions/BlockBasedTable "default"]
  ...
```

Escaping: `\`, `#`, `:`, CR and LF are preceded by `\` (`\n`, `\r` for the controls)
[R util/string_util.cc:185-251]. Parsing: `#` starts a comment unless escaped; exactly one
`[Version]` and one `[DBOptions]`; the first CF section is `"default"`; a TableOptions section
follows its CF; statements are single-line `name=value` split at the first `=`
[R options_parser.cc:175-251, :356-451, :529-576]. Verification levels: none 0x01, loosely
compatible 0x02, exact match 0xFF [R include/rocksdb/convenience.h:45-52].

**LOCK** is a file opened `O_RDWR | O_CREAT` (nothing is written to it) and held with an
advisory `fcntl` lock for the DB's lifetime [R include/rocksdb/file_system.h:722-728;
env/fs_posix.cc:858-910].

**DECISION.** Same names, same procedures and syncs. The port writes `rocksdb_version=11.8.1`
(it claims compatibility with that version's reader) and only the options it implements; on
read it accepts every option name 11.8.1 knows, applies the ones it implements, ignores the
documented no-op and deprecated ones, and refuses any other option whose value differs from
its default. LOCK uses `std::fs::File::try_lock` (stable in the pinned Rust 1.98).

### 1.16 Blob files and blob indexes (db/blob/)

**File** [R db/blob/blob_log_format.h:20-175; blob_log_format.cc:14-141]:

```
header (30 bytes): magic:fixed32 = 2395959 (0x00248f37) | version:fixed32 = 1 | cf_id:fixed32
                   | flags:u8 (bit0 has_ttl) | compression:u8
                   | expiration_first:fixed64 | expiration_last:fixed64        (no CRC)
record:            key_len:fixed64 | value_len:fixed64 | expiration:fixed64
                   | header_crc:fixed32 = Mask(crc32c(first 24 bytes))
                   | blob_crc:fixed32   = Mask(crc32c(key ‖ value))
                   | key | value
footer (32 bytes): magic:fixed32 | blob_count:fixed64 | expiration_first:fixed64
                   | expiration_last:fixed64 | crc:fixed32 = Mask(crc32c(first 28 bytes))
```

The key stored is the user key [R db/compaction/compaction_iterator.cc:1396]; values are
compressed with the blob compression type using the fv-2 framing (varint32 size prefix except
Snappy), always, even if larger [R db/blob/blob_file_builder.cc:113, :243-247;
util/compression.cc:1951-1985]. The reader requires no TTL, a zero expiration range and a
matching CF id, and with `verify_checksums` re-reads the record header and checks both CRCs,
key and sizes [R db/blob/blob_file_reader.cc:139-256, :576-617]. Integrated blob files never
have TTL.

**BlobIndex** (value of a kTypeBlobIndex entry) [R db/blob/blob_index.h:20-201]:

| Type byte | Layout |
|---|---|
| 0 kInlinedTTL | varint64 expiration, raw value |
| 1 kBlob | varint64 file_number, varint64 offset (of the value), varint64 size, u8 compression |
| 2 kBlobTTL | varint64 expiration, then as kBlob |

Decode requires exactly one byte after the varints; type ≥ 3 is corruption [R :103-129].
File number 0 means "this file" (blobs embedded in an SST) [R blob_index.h:64-67;
db/blob/blob_constants.h:17, :31].

**Embedded blobs** (new in 11.x): a block-based table can hold blob records inline, each
`payload ‖ u8 compression ‖ fixed32 checksum` where the checksum is the table's block
checksum over payload and compression byte plus the context modifier at the record offset
(`SimpleGen2Blob`, only kNoCompression); property `rocksdb.embedded.blob.stats`
[R db/blob/blob_gen2_format.h:26-43; blob_gen2_format.cc:73-109; table/embedded_blob_sst.h:
35-78; block_based_table_builder.cc:2397-2434]. Blob direct write uses the same v1 blob
file format and ordinary kTypeBlobIndex batch records; files may be temporarily footer-less
[R db/blob/blob_file_partition_manager.cc:152, :184-313, :764-808].

**DECISION.** P13 ports blob file reading (header, records, footer, both CRCs, BlobIndex,
MANIFEST blob records and garbage accounting) so a RocksDB directory with
`enable_blob_files = true` opens; writing blob files and blob GC follow in P13b. Embedded blobs
and blob direct write are refused on read (`Unsupported`) until a need is shown; both are off
by default [R include/rocksdb/advanced_options.h:1082, :1228].

### 1.17 Wide-column entities (db/wide/wide_column_serialization.cc)

Value of a kTypeWideColumnEntity entry [R db/wide/wide_column_serialization.h:32-129]:

```
V1: varint32 version=1 | varint32 N | { LP name | varint32 value_size }×N | values concatenated
V2: varint32 version=2 | varint32 N | varint32 name_sizes_bytes | varint32 value_sizes_bytes
    | varint32 names_bytes | u8 type×N (0x01 inline, 0x11 blob index)
    | varint32 name_size×N | varint32 value_size×N | names | values
```

Columns strictly ascending by bytewise name; the default column has the empty name; sizes
≤ u32::MAX; trailing bytes are corruption; version > 2 is corruption and **version < 2
(including 0) parses as V1** [R wide_column_serialization.cc:52-270, :337-516;
wide_column_serialization.h:285-332; db/wide/wide_columns.cc:15].

**DECISION.** Port V1 read/write and V2 read with inline columns; a V2 blob column is resolved
through the blob reader of P13.

### 1.18 Conformance: what the port reads and writes

| Artifact | Port reads | Port writes (default) | Refused with a typed error |
|---|---|---|---|
| SST | block-based fv 2–7; all 5 checksum types; none/Snappy/LZ4/LZ4HC/ZSTD/Zlib; all 4 index types; legacy/fast/Ribbon filters, full and partitioned; separated KV; data-block hash index | fv 7, kXXH3, per options | plain, cuckoo, legacy fv 0/1; BZip2, Xpress, custom compression; primary UDI; embedded blobs |
| WAL | all record types, recyclable, ZSTD-compressed, 4 recovery modes | only in fidelity tests | batches with XID markers; non-zero timestamp sizes |
| WriteBatch | all tags | Put, Delete, SingleDelete, DeleteRange, Merge, PutEntity, LogData | XID markers outside WAL recovery |
| MANIFEST | all tags incl. safe-ignore skipping | as RocksDB less subcompaction progress, temperature, path id | unknown non-ignorable tags; unknown comparators; UDT |
| CURRENT, IDENTITY, LOCK | as RocksDB | as RocksDB | — |
| OPTIONS | 11.8.1 files | kept options, `rocksdb_version=11.8.1` | non-default values of unported options |
| Blob files | v1 files, BlobIndex types 0–2 | P13b | TTL blob files; direct-write partial files |
| Info LOG, trace files | not read (nothing in RocksDB parses them either) | tracing output, not a file format | — |

### 1.19 Format constants for docs/design/constants.md

`scripts/check-contracts.py` requires a row in `docs/design/constants.md` for every numeric
constant in production code, with a kind. Every constant of §1 — magic numbers, tag values,
type bytes, header and trailer sizes, `kBlockSize`, `kMaskDelta`, `0x6b9083d9`, the XXPH3
secret, hash seeds (`0xbc9f1d34`, 397), probe multipliers, property-name strings (strings are
not numeric but belong beside them) — is kind **format**, cited to the line given here. Option
defaults the port inherits (`write_buffer_size`, `level0_*_trigger`, `max_bytes_for_level_*`,
`block_size`, `bloom bits_per_key`) are **not** format: under mantle's rule on tuning
constants (CLAUDE.md §4; note 12 §6.6) they are either set per instance from the models of
note 12 §2 with measured inputs, or kind **cited** to RocksDB with the line, never silently
inherited.


---

## 2. Module map

Line counts are `wc -l` of every `.cc`, `.h` and `.c` file at the pinned commit (DERIVED,
§How to read). "Test" counts `*_test.cc`, `*_test.c`, test utilities (`*test_util*`,
`testutil*`, `mock_*`) and benchmarks/stress tools; "source" is the rest. Dependencies are the
modules whose headers a module's source files `#include` (computed over the tree, tests and
tools excluded); the C++ graph is cyclic, and the port imposes the acyclic order of §3.

Verdicts: **Port** (kept, byte- and behaviour-compatible), **Port-reduced** (kept, with the
named parts dropped), **Replace** (the responsibility is kept, the C++ is not: a Rust crate or a
mantle crate does it), **Drop** (not ported, with the reason). A dropped feature whose trace can
appear in a file the port must read is still *parsed*, and refused with a typed
`Unsupported { feature, file, offset }` error when its presence would change the answer; each
such case is named.

### 2.1 Summary table

| Module (files) | Responsibility | Source lines | Test lines | Depends on (principal; full graph in Appendix C) | Verdict |
|---|---|---:|---:|---|---|
| `util/` coding, crc32c*, hash*, xxhash, xxph3, murmurhash, fastrange, math | varint/fixed coding, CRC-32C (incl. arm64/ppc kernels), xxHash, XXPH3, hashing | 12,896 | 1,701 | port | Port coding (§1.1); Replace hashes with `crc-fast` (CRC-32C, already mantle-crc) and `twox-hash` (XXH32, XXH64, XXH3-64, XXH3-128; already a workspace dependency); port `Hash()`/`Lower32of64`/`FastRange` bit-exact |
| `util/` bloom_impl, ribbon_*, dynamic_bloom | filter bit layouts, Ribbon solver, memtable bloom | 3,816 | 2,837 | util | Port (bit-exact, §1.11) |
| `util/` compression*, auto_tune_compressor, simple_mixed_compressor | compression framing and codec calls | 3,617 | 2,834 | port, third-party codecs | Port framing; codecs via crates (§2.4) |
| `util/` threadpool, thread_local, timer, work_queue, channel, core_local, mutexlock, semaphore, atomic, io_dispatcher, coroutines | concurrency primitives, async I/O dispatch | 5,412 | 4,864 | port, monitoring | Replace with std + the designs of §4 |
| `util/` rate_limiter | shared I/O budget | 677 | 673 | port, monitoring | Port (§4.7) |
| `util/` string_util, slice, status, comparator, autovector, heap, random, udt_util, file_checksum_helper, defer, cast_util, aligned_buffer, bit_fields | small utilities | 6,953 (the rest of util/'s 33,371) | 3,841 | — | Port what the ported modules call; `Status` becomes typed errors (§2.3) |
| `db/` formats: dbformat, write_batch*, log_format/reader/writer, version_edit, wal_edit, kv_checksum, lookup_key | internal key, batch, log records, manifest records | 11,318 | 4,641 | util | Port (§1.3–§1.5, §1.14) |
| `memtable/` inlineskiplist, skiplist, skiplistrep, write_buffer_manager, alloc_tracker, other reps | memtable representation, memory accounting | 4,951 (+689 memtablerep_bench) | 1,847 | memory, cache | Port the skiplist rep (redesigned, §4.1) and WriteBufferManager; Drop hash_skiplist_rep, hash_linklist_rep, vectorrep (no on-disk trace; mantle uses the default rep); Drop wbwi_memtable (transactions) |
| `memory/` | arena, concurrent arena, allocators | 1,290 | 552 | port | Replace with the memtable's own bounded node store (§4.1); Drop jemalloc/memkind allocators |
| `db/` memtable, memtable_list, flush_scheduler, trim_history_scheduler | memtable, immutable list | 5,254 | 2,146 | memtable, db formats | Port |
| `db/` write_thread, write_controller, write_stall_stats, callbacks | group commit, stalls | 2,025 | 3,017 | port, monitoring | Port-reduced (§4.2): no pipelined/unordered/two-queue writes |
| `db/` version_set, version_builder, version_edit_handler, version_util, file_indexer, column_family, manifest_ops | Versions, MANIFEST, column families, SuperVersion | 18,273 | 12,048 | db formats, table, cache | Port |
| `db/` db_iter, arena_wrapped_db_iter, forward_iterator, table_cache, snapshot*, multi_cf/coalescing/attribute-group iterators, multi_scan | read path | 7,675 | 15,992 | table, version | Port DBIter, table cache, snapshots; Drop forward (tailing) iterator, multi-CF/attribute-group iterators (not used by mantle; no on-disk trace) |
| `db/` range_del_aggregator, range_tombstone_fragmenter | range tombstones | 1,921 | 5,555 | table | Port |
| `db/` merge_helper, merge_operator, merge_context | merge operands | 1,377 | 3,026 | db formats | Port |
| `db/` flush_job, builder, output_validator, table_properties_collector, event_helpers, job_context | flush and table building | 3,353 | 5,605 | table, version | Port (event listeners reduced to a typed event channel) |
| `db/` db_filesnapshot, external_sst_file_ingestion_job, import_column_family_job | checkpoint support, ingest, import | 3,393 | 10,925 | version, table | Port |
| `db/` wal_manager, transaction_log_impl, logs_with_prep_tracker | WAL archive, GetUpdatesSince, 2PC log tracking | 1,236 | 4,303 | db formats | Port-reduced: WAL recovery kept; archive (`WAL_ttl_seconds`), `GetUpdatesSince` and prepared-log tracking dropped (mantle's Raft log replaces them) |
| `db/` error_handler, internal_stats, periodic_task_scheduler, seqno_to_time_mapping | background errors, stats, periodic tasks, seqno→time | 5,472 | 13,136 | version | Port error handler (no auto-resume, §4.5) and the stats mantle exports; seqno_to_time_mapping parsed (it is a table property, §1.12) but not generated |
| `db/db_impl/` | DB open/recover, write, flush/compaction scheduling, files, read-only, secondary, follower | 30,321 | (in db/ tests) | everything above | Port db_impl, _open, _write, _compaction_flush, _files, _readonly; Drop _secondary (1,631+426), _follower (345+54), compacted_db_impl (484) |
| `db/compaction/` | pickers (level, universal, FIFO), compaction job, iterator, outputs, subcompactions, remote compaction | 17,581 | 19,179 | version, table, db/blob | Port level, universal, FIFO pickers, job, iterator, outputs, subcompactions; Drop compaction_service_job (1,144; remote compaction) and tiered/temperature placement |
| `db/blob/` | integrated BlobDB: blob files, blob index, GC, direct write | 7,135 | 13,893 | table, file, cache | Port reading of blob files and blob indexes (a file RocksDB wrote may hold them); Port writing as a later phase (P13b) |
| `db/wide/` | wide-column entity serialization | 1,862 | 6,777 | db, util | Port (a `kTypeWideColumnEntity` record can appear in any file) |
| `table/` (format, meta_blocks, block_fetcher, table_properties, merging_iterator, get_context, sst_file_writer/reader, two_level_iterator, unique_id) | table-format generic layer | 12,303 | 13,991 | util, file, cache | Port; Drop external_table (760), mock_table (test), persistent_cache_helper |
| `table/block_based/` | the block-based table: blocks, index, filters, builder, reader, factory | 26,627 | 6,328 | table, cache, util | Port; the experimental trie index (`utilities/trie_index`, 5,663) is Dropped and a file that names it is refused (§1.10) |
| `table/plain/`, `table/cuckoo/`, `table/adaptive/` | alternative table formats | 3,379 + 1,390 + 177 | 1,201 | table | Drop: never produced unless configured; a file with their magic numbers is refused by magic (§1.9) |
| `cache/` | LRU, HyperClockCache, secondary/tiered caches, reservation manager | 10,583 | 8,091 | memory, port | Port HyperClockCache (default, §4.8) and CacheReservationManager; Port LRU for fidelity of the `LRUCache` option; Drop compressed/tiered secondary caches |
| `options/` | option structs, string parsing, OPTIONS file | 7,842 | 9,386 | util, table | Port-reduced: typed Rust option structs; OPTIONS file writer and reader for the kept options (§1.15); drop the `Customizable`/`ObjectRegistry` string-configuration framework except what the OPTIONS file needs |
| `file/` | writable/readable file wrappers, prefetch, filename, SstFileManager, DeleteScheduler | 7,929 | 6,074 | env, monitoring | Port filename, WritableFileWriter, RandomAccessFileReader, FilePrefetchBuffer, SstFileManager, DeleteScheduler over mantle-disk (§2.2) |
| `env/` | Env/FileSystem (POSIX), mock env, encryption, chroot, remap, tracing | 11,601 | 6,998 | port | Replace (§2.2); Drop env_encryption (mantle has its own SSE, note 20), chroot, remap, on-demand, tracer, mock (mantle-disk `SimFile` replaces it) |
| `port/` | OS portability: mutex, condvar, atomics, mmap, stack trace, Windows port | 6,903 | 0 | — | Replace with std, `rustix`, `windows-sys` through mantle-disk (§2.2) |
| `monitoring/` | statistics, histograms, perf/iostats context, thread status | 4,105 | 1,046 | port | Port-reduced: counters and histograms mantle exports (mantle-disk already has `Histogram`); Drop perf_context, thread_status, persistent stats history |
| `logging/` | info LOG, auto-roll, event logger | 1,206 | 935 | env | Replace with `tracing`; no file format to keep (nothing reads the LOG) |
| `trace_replay/` | query/IO/block-cache tracing | 2,638 | 775 | db | Drop (diagnostic; no file the engine reads) |
| `utilities/checkpoint` | CreateCheckpoint, ExportColumnFamily | 839 | 1,693 | db, file | Port (snapshots and moves, note 12 §6.3) |
| `utilities/backup` | BackupEngine | 3,409 | 4,930 | checkpoint, env | Drop: mantle's snapshots and moves are checkpoints plus its own transfer (note 12 §6.3, §6.5) |
| `utilities/transactions` | pessimistic/optimistic/write-prepared/unprepared transactions, lock managers | 25,449 | 24,308 | wbwi, db | Drop: a range's Raft log serializes its writes (note 12 §6.2); the 2PC markers in a WriteBatch are still parsed, and a WAL batch carrying them is refused (§1.4) |
| `utilities/write_batch_with_index` | indexed batch for read-your-writes | 2,707 | 4,250 | memtable, db | Drop (used only by transactions) |
| `utilities/ttl` | DBWithTTL: 4-byte timestamp suffix on values, TTL compaction filter | 899 | 934 | db | Drop; the value suffix is a user-level format, not an engine one |
| `utilities/blob_db` | legacy stacked BlobDB | 4,685 | 1,837 | db/blob | Drop (deprecated in favour of integrated blob files, `db/blob/`) |
| `utilities/merge_operators`, `agg_merge`, `cassandra` | sample merge operators | 843 + 436 + 950 | 639 + 135 + 1,190 | db | Drop (mantle defines its own merge operators if it uses merge) |
| `utilities/table_properties_collectors` | compact-on-deletion collector, tiering collector | 563 | 434 | db | Port compact-on-deletion (it bounds tombstone buildup); Drop tiering |
| `utilities/` persistent_cache, simulator_cache, secondary_index, trie_index, option_change_migration, copy_engine, sorted_run_builder, and the top-level files (env_mirror, fault_injection_*, object_registry, cache_dump_load, counted_fs) | assorted | 19,017 | 16,443 | — | Drop; fault injection is replaced by mantle-disk `SimFile` |
| `db/c.cc`, `include/rocksdb/c.h`, `java/` | C API, Java binding (116,064 lines under java/) | 13,671 + C header | 6,892 | db | Drop: the port is a Rust library |
| `tools/` sst_dump, ldb, db_bench | inspection and benchmark tools | 777, 5,654 + 870, 11,117 | 680, 1,259, 381 | db, table | Keep the C++ builds as differential oracles (§3.0); port `db_bench` as the last phase |
| `db_stress_tool/`, `microbench/`, `fuzz/` | stress and fuzz harnesses | — | 23,628 + 1,767 + 518 | — | Replace with mantle's simulator and proptest (§3.0) |
| `include/rocksdb/` | public API (88 headers) | 37,982 (+8,143 utilities) | — | — | Replace with the Rust API of the kept surface |

The C++ include graph between these modules is cyclic (for example `util/` includes `db/`
headers and `db/` includes `util/`; `table/` and `db/` include each other). The port's crate
and module order is the acyclic order of §3: coding/checksums → keys and batches → log →
blocks and tables → filters → version edits → engine → compaction → file operations →
shared budgets.

### 2.2 Env and FileSystem onto mantle-disk

RocksDB reaches the OS through `FileSystem` and its file classes (`FSSequentialFile`,
`FSRandomAccessFile`, `FSWritableFile`, `FSRandomRWFile`, `FSDirectory`)
[R include/rocksdb/file_system.h:404-1568], implemented for POSIX in env/fs_posix.cc and
env/io_posix.cc and for Windows in port/win/. The port replaces the whole layer with a small
trait over mantle-disk, and every call RocksDB makes maps to one of these:

| RocksDB call [R include/rocksdb/file_system.h] | What RocksDB does (POSIX) | Port (DECISION) |
|---|---|---|
| `NewWritableFile`, `ReopenWritableFile` (:502, :515) + `FSWritableFile::Append` (:1217) | buffered `write(2)` appends; optional `O_DIRECT` with aligned buffer | an append writer over `BlockFile::write_all_at` at a tracked end offset, buffering to the file's `Alignment` with mantle-disk's `AlignedBuf`/`Pool` (`crates/disk/src/buf.rs`) |
| `PositionedAppend` (:1254) | `pwrite` at offset (direct I/O path) | `write_all_at` |
| `FSWritableFile::Sync`/`Fsync` (:1313, :1321) | `fdatasync`/`fsync`, or `fcntl(F_FULLFSYNC)` only if built with `HAVE_FULLFSYNC` [R env/io_posix.cc:1826-1851] | `BlockFile::sync_data` = `fdatasync` (Linux), `F_FULLFSYNC` (macOS), `FlushFileBuffers` (Windows), always (`crates/disk/src/file.rs` header; CLAUDE.md §6) |
| `RangeSync` (:1398) with `bytes_per_sync` | `sync_file_range(SYNC_FILE_RANGE_WRITE)` where supported [R env/io_posix.cc:197-223, :1923] | dropped: a write-back hint with no durability meaning; durability comes only from `sync_data` |
| `Close` (:1293) on a direct-I/O file | writes the padded tail, then `ftruncate` to the logical size [R env/io_posix.cc:1769-1825] | needs a `set_len` on `BlockFile` (not present today): mantle-disk gains it, with its `SimFile` semantics (§5 R16) |
| `Allocate`/`SetPreallocationBlockSize` (:1433, :1369) | `fallocate(KEEP_SIZE)` [R env/io_posix.cc:1672-1680] | `DeviceFile::preallocate` (exists) |
| `NewRandomAccessFile` + `Read`/`MultiRead`/`ReadAsync` (:492, :1019, :1040, :1122) | `pread`; io_uring for MultiRead/async where built | `read_exact_at` / `read_at`; MultiRead as a loop first, then batched through mantle-disk's worker queue (`crates/disk/src/workers.rs`) if measured worthwhile |
| `NewSequentialFile` + `Read` (:480, :887) | `read(2)` | `read_at` at a tracked offset |
| `Prefetch`/`Hint`/`InvalidateCache` (:1026, :1073, :1086) | `posix_fadvise`, readahead | dropped; readahead is the port's `FilePrefetchBuffer` in user space, which RocksDB also has (file/file_prefetch_buffer.cc) |
| `NewDirectory` + `FSDirectory::Fsync`/`FsyncWithDirOptions` (:558, :1554-1559) | `open(O_DIRECTORY)` + `fsync`, or `F_FULLFSYNC` with `HAVE_FULLFSYNC`; skipped on btrfs for some reasons [R env/io_posix.cc:2090-2160] | `mantle_disk::file::sync_dir` (exists); no btrfs special case (mantle does not assume a file system, CLAUDE.md §5) |
| `RenameFile`, `LinkFile`, `DeleteFile`, `CreateDir[IfMissing]`, `DeleteDir`, `GetChildren`, `FileExists`, `GetFileSize`, `GetFileModificationTime`, `NumFileLinks`, `AreFilesSame` (:568-700) | libc | `std::fs` (portable on all three OSes), each followed by `sync_dir` where RocksDB syncs the directory |
| `LockFile`/`UnlockFile` (:722-728) | `fcntl(F_SETLK)` | `std::fs::File::try_lock` / drop |
| `GetFreeSpace` (:805) | `statvfs` | mantle-disk's device capacity measurement (CLAUDE.md §5), not a per-call syscall |
| `NewLogger` (:741) | info LOG file | `tracing` |
| `NewMemoryMappedFileBuffer`, mmap reads/writes (`allow_mmap_reads/writes`) | `mmap` | dropped: needs `unsafe`; the options are refused if set |
| `Env::Schedule` / thread pools [R include/rocksdb/env.h:441; env/env_posix.cc:421-442] | per-priority `ThreadPoolImpl` | process-wide bounded workers (§4.5) |
| `Env::NowMicros`/`NowNanos`/`SleepForMicroseconds` | clock, `usleep` | an injected clock (real or simulated); no sleeping (clippy.toml forbids `std::thread::sleep`) — waits are condvar waits with deadlines |
| `GenerateUniqueId` [R env/env.cc:861-899] | `/proc/sys/kernel/random/uuid` or random v4 UUID | `getrandom` (workspace dependency) → v4 UUID, same text form |

**Where the file layer sits.** The port's file trait has two implementations: the real one on
`mantle_disk::DeviceFile` and `std::fs`, and a simulated one on `mantle_disk::sim::SimFile`
plus a simulated directory (§5 R16), so the crash matrices of §3 run in deterministic
simulation (CLAUDE.md §8) as well as on real disks. Direct I/O is decided per file system by
mantle-disk (`CachingRequest::PreferDirect`, falling back to buffered), not by RocksDB's
`use_direct_reads`/`use_direct_io_for_flush_and_compaction` flags, which the port accepts and
records but does not obey.

**What replaces `port/`.** `port::Mutex`/`CondVar`/`RWMutex` → `std::sync` (a poisoned lock is
a typed error, CLAUDE.md §1); atomics → `std::sync::atomic` with the orderings of §4;
`port::GenerateRfcUuid` → `getrandom`; `CACHE_LINE_SIZE` → a per-target constant that matches
[R port/port_posix.h:190-207] (it shapes legacy filter bytes, §1.11); stack traces, jemalloc
hooks, `malloc_usable_size` → dropped. The Windows port (port/win/, 5,129 lines) is not
translated: the Rust standard library and mantle-disk already give the portable path, with
`windows-sys` only where mantle-disk already uses it.

### 2.3 Status → typed errors

RocksDB returns `Status`/`IOStatus` with a code (`kCorruption`, `kNotSupported`,
`kInvalidArgument`, `kIOError`, `kBusy`, `kIncomplete`, `kTryAgain`, `kNoSpace` via subcode,
`kAborted`, `kTimedOut`, `kShutdownInProgress`, `kMemoryLimit`, …) and a free-text message
(include/rocksdb/status.h). The port's error is an enum per crate, with the variants the
callers must tell apart: `Corruption { file, offset, what }` (feeds repair, CLAUDE.md §6),
`Unsupported { feature, file }` (a format the port refuses, §1.18), `InvalidArgument`,
`Io { op, path, source }` (a failed flush fences the instance: `BlockFile::sync_data`
documents that durability is then unknown), `Busy`/`Stalled` (a bound was reached, §4),
`NoSpace`, `Shutdown`. `TryAgain` from a duplicate memtable key is an internal variant that
never leaves the write path. Every C++ `assert` on a data path becomes one of these (§5 R6).

### 2.4 Dependencies the port adds

| Need | Crate (pure Rust unless noted) | Status in mantle |
|---|---|---|
| CRC-32C | `crc-fast` via mantle-crc | present |
| XXH32, XXH64, XXH3-64, XXH3-128 | `twox-hash` (features `xxhash64`, `xxhash3_64`, `xxhash3_128` enabled; `xxhash32` to add) | present |
| XXPH3 (preview) | none exists; ported from util/xxph3.h (§5 R1) | new code |
| Snappy | `snap` (raw format) | to add |
| LZ4 block (+ dictionary) | `lz4_flex` (block format; dictionary support to verify, UNVERIFIED) | to add |
| ZSTD (+ dictionary, training, streaming) | `zstd` / `zstd-sys` (C library, BSD) — or decode-only `ruzstd` if writes use LZ4/none | owner's decision (§5 R9) |
| Zlib raw deflate | `miniz_oxide` | to add |
| UUID | `getrandom` | present |
| Randomized testing | `proptest` | present |

Each is called behind an unwind boundary where it can panic (CLAUDE.md §1), and each passes
`cargo deny` (the gates).

---

## 3. Port order

Each phase ends with a tree that passes mantle's gates (CLAUDE.md "Gates") and is committed on
its own. A phase is done when (a) its RocksDB tests, listed by file with their count of test
definitions (`TEST`, `TEST_F`, `TEST_P` macros; a `TEST_P` counts once however many parameter
sets instantiate it), are ported and pass, and (b) its differential checks against the C++
build pass. Counts are DERIVED by `grep -cE '^\s*(TEST|TEST_F|TEST_P|TYPED_TEST|TYPED_TEST_P)\('`
over each file at the pinned commit. A test that exercises a dropped feature (§2) is not ported;
the test file's row says which of its tests those are when it is not the whole file.

### 3.0 The oracle, the corpus and the harness (before P1)

- **The oracle** is the C++ RocksDB 11.8.1 build that is being set up separately, with
  `sst_dump`, `ldb` and `db_bench` (tools/sst_dump_tool.cc 777 lines, tools/ldb_cmd.cc 5,654,
  tools/db_bench_tool.cc 11,117). The port never links it; differential checks run the tools as
  processes on files each side wrote, which is CLAUDE.md §8's "real processes, real disks".
- **Two directions for every format.** *Port writes, C++ reads*: the C++ tool opens the port's
  output and must accept it and print the same content. *C++ writes, port reads*: a corpus of
  files written by the C++ build is read by the port's tests. The corpus is generated by a
  script into a directory outside the repository (the development machine's volume is
  nearly full), keyed by the SHA-256 of each file, and regenerated rather than committed.
- **Corpus axes** (each a RocksDB option that changes bytes on disk; §1 cites each):
  `format_version` 2–7 × `checksum` {kNoChecksum, kCRC32c, kxxHash, kxxHash64, kXXH3} ×
  compression {none, snappy, zlib, lz4, lz4hc, zstd, zstd+dictionary} × `index_type` {binary, hash,
  two-level, binary-with-first-key} × filter {none, legacy bloom (fv<5), fast local bloom,
  Ribbon, each whole-file and partitioned} × `block_restart_interval` {1, 16} ×
  `data_block_index_type` {binary, binary-and-hash} × {range deletions present or not} ×
  {wide-column entities present or not} × {blob files present or not} ×
  `separate_key_value_in_data_block` {off, on} × host {x86_64, aarch64} (the legacy bloom
  line size differs, §1.11). Not every product is
  generated; every value of every axis appears at least once with the defaults for the rest,
  and every pair that a format check couples (for example fv≥6 × each checksum, fv≥4 × index
  value delta encoding) appears.
- **Byte identity.** Where both sides are given the same input and the same options, the port
  must write the same bytes, with three sources of per-file randomness or time excepted:
  (1) properties that carry a wall-clock time, host name, DB id or session id (§1.12);
  (2) for fv ≥ 6, the random `base_context_checksum`, which changes every block checksum and the
  footer [R table/block_based/block_based_table_builder.cc:1438-1447]; (3) anything derived from
  those (block handles shift when a property's length changes). The check reads (1) and (2)
  from the oracle's file and gives them to the port's builder as inputs (the port takes clock,
  identities and the context-checksum base from its caller), then requires equal bytes; where
  that is not possible it compares block by block after removing the context modifier and
  masking exactly those properties.
- **The harness replaces three C++ test facilities:** `SyncPoint` (used by 104 of the 225 test
  files) becomes named hook points compiled in only under `#[cfg(test)]` or a test-only cargo
  feature; `FaultInjectionTestFS`/`FaultInjectionTestEnv` (29 test files) become mantle-disk's
  `SimFile` with its power-loss semantics (`crates/disk/src/sim.rs`); `DBTestBase`
  (db/db_test_util.h, 1,491 lines; 78 test files) becomes a Rust fixture with the same helper
  names (`Put`, `Get`, `Flush`, `FilesPerLevel`, `NumTableFilesAtLevel`, `dbfull()->TEST_*`),
  so ported tests read line for line against the C++.

### 3.1 Phases

| Phase | Scope | RocksDB tests to port (file: test definitions) | Differential checks against the C++ build |
|---|---|---|---|
| **P1 Coding and checksums** | util/coding (§1.1); CRC-32C with Mask/Unmask and Crc32cCombine (mantle-crc); XXH32, XXH64, XXH3-64 (twox-hash); `Hash`, `Hash64` (XXPH3 preview, ported, §5 R1–R2), `Lower32of64`, FastRange; the file checksum `FileChecksumCrc32c` | util/coding_test.cc: 19; util/crc32c_test.cc: 8; util/hash_test.cc: 16 (the `Hash128`/`BijectiveHash` tests only if those are kept); util/slice_test.cc: 13 | Golden vectors printed by a small C++ program linked to the oracle's `librocksdb` (every function over lengths 0–4,096 and random seeds), compared with the port's output. hash_test.cc already pins many outputs as literals. |
| **P2 Internal keys, WriteBatch, memtable** | dbformat (§1.3); WriteBatch encode/decode with every tag (§1.4); wide-column serialization V1/V2 (§1.17); blob index decode (§1.16); the memtable skiplist, single-writer, with its bounded node store (§4.1); MemTable Add/Get with range-del table; MemTableList | db/dbformat_test.cc: 12; db/write_batch_test.cc: 31; db/wide/wide_column_serialization_test.cc: 29; db/wide/wide_columns_helper_test.cc: 2; memtable/inlineskiplist_test.cc: 26 (concurrent-insert cases dropped with the feature); memtable/skiplist_test.cc: 8; memory/arena_test.cc: 6 (as tests of the node store's accounting); db/memtable_list_test.cc: 8 | Batches serialized by both sides byte-compared; the port parses batches the oracle wrote (through P3's WAL, `ldb dump_wal --print_value`). |
| **P3 WAL / log format** | log writer and reader incl. recyclable records, fragments, ZSTD streaming WAL compression, timestamp-size and predecessor-WAL records, all four recovery modes (§1.5) | db/log_test.cc: 47 | Port writes WALs from a batch script → `ldb dump_wal --walfile=F --header --print_value` output equals the oracle's for the same script; the oracle's WALs (`db_bench --benchmarks=fillrandom --disable_wal=0`, killed mid-write for torn tails; `recycle_log_file_num>0`; `wal_compression=zstd`) are read by the port with the same records and the same corruption reports in each recovery mode. |
| **P4 Blocks and the block-based table** | block builder/reader with restart points, data-block hash index, index blocks (binary, hash, two-level, first-key), BlockHandle/trailer/checksums incl. context checksums, footers for every version, compression framing, meta-index, properties, range-del block, SstFileWriter/SstFileReader (§1.6–§1.13) | table/block_based/block_test.cc: 23; table/block_based/data_block_hash_index_test.cc: 12; table/block_fetcher_test.cc: 4; table/block_based/block_based_table_reader_test.cc: 19; table/table_test.cc: 106 (less plain/cuckoo harness cases); table/sst_file_reader_test.cc: 31; table/merger_test.cc: 6; table/cleanable_test.cc: 5; util/compression_test.cc: 22 | `sst_dump --file=F --command=verify --verify_checksum`, `--command=scan --output_hex`, `--command=raw`, `--show_properties`, `--list_meta_blocks` on every port-written file: accepted, identical scan output, identical property values; the port reads every corpus file and yields the same `scan`; byte identity for identical input (§3.0). |
| **P5 Filters** | legacy bloom, FastLocalBloom, Standard128Ribbon, whole-file and partitioned filters, prefix extractor, `whole_key_filtering` (§1.11) | util/bloom_test.cc: 10; util/ribbon_test.cc: 7; util/dynamic_bloom_test.cc: 6; table/block_based/full_filter_block_test.cc: 5; table/block_based/partitioned_filter_block_test.cc: 7 | Filter block bytes identical to the oracle's for the same key set and bits-per-key (construction is deterministic); `sst_dump --command=raw` shows identical filter blocks; the port answers `KeyMayMatch` identically on the oracle's filters for 10⁶ probe keys. |
| **P6 VersionEdit, MANIFEST, CURRENT, OPTIONS** | VersionEdit encode/decode with every tag and custom field (§1.14); VersionBuilder, VersionSet recover and LogAndApply; atomic groups; column families; file names (§1.15); IDENTITY; OPTIONS writer/reader for the kept options | db/version_edit_test.cc: 28; db/wal_edit_test.cc: 11; db/version_builder_test.cc: 29; db/version_set_test.cc: 94; db/filename_test.cc: 4; db/file_indexer_test.cc: 5; db/options_file_test.cc: 5; options/options_test.cc: 75 (the parsing and OPTIONS-file tests of kept options; the `Customizable` framework tests of options/customizable_test.cc 49 and configurable_test.cc 25 are dropped with it) | `ldb manifest_dump --path=MANIFEST-N --verbose --json` equal for the same edit sequence; the oracle opens (`ldb --db=D list_column_families`, `ldb --db=D checkconsistency`) a directory whose MANIFEST/CURRENT/OPTIONS the port wrote; the port recovers the oracle's MANIFESTs incl. ones with atomic groups and rolled-over manifests. |
| **P7 DB open, write, flush, recovery** | DBImpl open/recover (with and without WAL), write path (§4.2), SwitchMemtable, FlushJob, BuildTable, obsolete-file purge, error handler without auto-resume, repair | db/db_basic_test.cc: 126; db/db_flush_test.cc: 59; db/flush_job_test.cc: 9; db/db_wal_test.cc: 62; db/db_write_test.cc: 18; db/db_memtable_test.cc: 7; db/corruption_test.cc: 31; db/fault_injection_test.cc: 9; db/db_io_failure_test.cc: 21; db/error_handler_fs_test.cc: 48 (auto-recovery tests dropped); db/obsolete_files_test.cc: 6; db/repair_test.cc: 14; db/column_family_test.cc: 69; db/db_kv_checksum_test.cc: 14 | A DB written by the oracle (`db_bench --benchmarks=fillrandom,overwrite,deleterandom`) opened by the port, and one written by the port opened by `ldb --db=D scan` and `ldb checkconsistency`: identical key-value sets (compared by a streamed digest). Crash matrix: power loss injected by `SimFile` at every sync point of a flush; after reopen, the persisted applied index (note 12 §6.2) names exactly the data present, and a `kPersistedTier` read returns exactly that state. |
| **P8 Reads: iterators, Get, MultiGet, snapshots** | DBIter, MergingIterator, table cache, GetContext, snapshots and SnapshotList, `ReadOptions` (`read_tier` incl. `kPersistedTier`, bounds, `total_order_seek`, `pin_data`), properties | db/db_iter_test.cc: 38; db/db_iterator_test.cc: 132; db/db_iter_stress_test.cc: 1; db/db_test.cc: 135; db/db_etc2_test.cc: 117; db/db_etc3_test.cc: 2; db/comparator_db_test.cc: 9; db/prefix_test.cc: 7; db/db_properties_test.cc: 32; db/db_table_properties_test.cc: 13; db/db_readonly_with_timestamp_test.cc: dropped with UDT (§1.3) | `ldb --db=D scan --from --to`, `get`, `multi_get` on the same directory from both sides, and a randomized operation log (puts, deletes, range deletes, merges, snapshots, flushes, reads) replayed through `ldb` on the oracle and through the API on the port, every read's result compared; the same log also runs against an in-memory model (an ordered map with explicit tombstones and snapshots), as `db_stress` checks RocksDB against its expected state. |
| **P9 Block cache** | HyperClockCache (default), LRUCache, sharding, priority pools, CacheReservationManager, cache keys (§4.8) | cache/cache_test.cc: 27; cache/lru_cache_test.cc: 31; cache/cache_reservation_manager_test.cc: 10; db/db_block_cache_test.cc: 20 | Behavioural, not bytes: hit/miss/eviction counters under a fixed trace equal the oracle's for LRU (deterministic); HyperClock is compared by its documented invariants (capacity never exceeded under `strict_capacity_limit`). `db_bench --benchmarks=readrandom --cache_size=` throughput recorded for both (CLAUDE.md §8). |
| **P10 Leveled compaction** | picker (score, `kMinOverlappingRatio`, dynamic level bytes, trivial move, L0→L0, intra-L0, bottommost, periodic/TTL), CompactionJob, CompactionIterator, outputs, subcompactions, manual CompactRange/CompactFiles | db/compaction/compaction_picker_test.cc: 147 (universal and FIFO cases move to P11); db/compaction/compaction_job_test.cc: 37; db/compaction/compaction_iterator_test.cc: 73; db/compaction/clipping_iterator_test.cc: 1; db/compaction/compaction_job_stats_test.cc: 3; db/db_compaction_test.cc: 172; db/db_dynamic_level_test.cc: 5; db/manual_compaction_test.cc: 3; db/compact_files_test.cc: 11; db/db_compaction_abort_test.cc: 15; db/db_sst_test.cc: 26 | The same op sequence on both engines with one background thread and `disable_auto_compactions` + explicit `CompactRange`: `ldb manifest_dump` lists the same files per level with the same key ranges and sizes equal up to the variable-length time properties of §3.0 (the picker is deterministic given sizes, so a size tie-break that flips on those bytes is the one tolerated difference, and is reported); `ldb checkconsistency` passes on the port's output. |
| **P11 Universal and FIFO compaction** | universal picker (size ratio, space amplification, sorted runs), FIFO (size, TTL, `allow_compaction`) | db/db_universal_compaction_test.cc: 34; the universal and FIFO cases of compaction_picker_test.cc and db_compaction_test.cc; db/db_test.cc FIFO cases | As P10, with `compaction_style=1` and `2`. |
| **P12 Range deletions, merge, compaction filters** | fragmenter, aggregator, range-del in memtable/table/iterators/compaction, `DeleteRange`; MergeHelper, merge in Get/iterators/compaction; CompactionFilter all decisions; table property collectors incl. compact-on-deletion | db/range_tombstone_fragmenter_test.cc: 17; db/range_del_aggregator_test.cc: 16; db/db_range_del_test.cc: 80; db/merge_helper_test.cc: 12; db/merge_test.cc: 7; db/db_merge_operator_test.cc: 14; db/db_merge_operand_test.cc: 10; db/db_compaction_filter_test.cc: 15; db/table_properties_collector_test.cc: 2; utilities/table_properties_collectors/compact_on_deletion_collector_test.cc: 3 | `sst_dump --command=scan` shows identical range-del blocks; `ldb list_file_range_deletes`; `ldb deleterange` on the oracle then port reads, and vice versa; merge with `stringappend` on both sides (the operator is re-implemented in the port's tests only). |
| **P13 File operations for snapshots and moves** | Checkpoint (hard links, flush-before when WAL off), ExportColumnFamily, CreateColumnFamilyWithImport, IngestExternalFile (global seqno, `write_global_seqno`, `allow_db_generated_files`), SstFileWriter, `DeleteFilesInRanges`, `GetLiveFilesStorageInfo`, `DisableFileDeletions`; blob files read and written (P13b) | utilities/checkpoint/checkpoint_test.cc: 32; db/external_sst_file_basic_test.cc: 35; db/external_sst_file_test.cc: 69; db/import_column_family_test.cc: 12; db/deletefile_test.cc: 8; db/db_clip_test.cc: 1; P13b: db/blob/blob_file_addition_test.cc: 5, blob_file_garbage_test.cc: 5, blob_file_builder_test.cc: 7, blob_file_reader_test.cc: 16, blob_file_cache_test.cc: 5, blob_source_test.cc: 9, blob_garbage_meter_test.cc: 7, blob_counting_iterator_test.cc: 2, db_blob_basic_test.cc: 39, db_blob_compaction_test.cc: 16, db_blob_corruption_test.cc: 1, db_blob_index_test.cc: 35 (db_blob_direct_write_test.cc 32 and db/wide/db_wide_blob_direct_write_test.cc 22 only if blob direct write is ported) | `ldb checkpoint` on the oracle → port opens; port checkpoint → `ldb --db=CP scan`; `ldb write_extern_sst` output ingested by the port and port SstFileWriter output ingested by `ldb ingest_extern_sst`; oracle DBs with `enable_blob_files=true` opened by the port with identical `scan`. |
| **P14 Shared budgets across instances** | WriteBufferManager (with cache charging and stall), RateLimiter, SstFileManager + DeleteScheduler, WriteController stalls, shared thread pools, shared block cache — all shared by many DB instances in one process (note 12 §6.1) | memtable/write_buffer_manager_test.cc: 4; db/db_write_buffer_manager_test.cc: 12; util/rate_limiter_test.cc: 14; db/db_rate_limiter_test.cc: 10; file/delete_scheduler_test.cc: 17; db/write_controller_test.cc: 4; new mantle tests: N instances in one process under one budget, each bound reached and answered with a typed refusal (CLAUDE.md §2) | `db_bench --num_multi_db=N --db_write_buffer_size=B --rate_limiter_bytes_per_sec=R` (tools/db_bench_tool.cc:415, :495, :1730) on both: the shared write-buffer budget and the rate limit hold on both engines (stall counts; bytes written per refill period). db_bench has no SstFileManager flag, so the deletion budget is checked by the ported delete_scheduler_test.cc and mantle's multi-instance tests only. |
| **P15 db_bench port** | the benchmarks mantle's hot paths need: `fillseq`, `fillrandom`, `overwrite`, `fillsync`, `readrandom`, `readseq`, `seekrandom`, `multireadrandom`, `readwhilewriting`, `deleterandom`, `compact`, `waitforcompaction`, with the flags of tools/db_bench_tool.cc (`--num` :297, `--value_size` :359, `--key_size` :410, `--format_version` :755, `--use_existing_db` :870, `--sync` :937, `--disable_wal` :941) | tools/db_bench_tool_test.cc: 7 | Same flags on both, same machine, same device: throughput and latency percentiles recorded as the baseline mantle's benchmark rule requires (CLAUDE.md §8), and the resulting DBs compared by `ldb scan` digest. A performance claim names the run. |

Tests of dropped features are not ported: transactions (utilities/transactions/*: 300 test
definitions), BlobDB legacy (blob_db_test.cc: 30), backup (backup_engine_test.cc: 65),
secondary/follower instances (db_secondary_test.cc: 43, db_follower_test.cc: 7), plain and
cuckoo tables (plain_table_db_test.cc: 18, cuckoo_table_db_test.cc: 6,
table/cuckoo/*: 18), tailing iterator (db_tailing_iter_test.cc: 12), user-defined
timestamps (db_with_timestamp_basic_test.cc: 69, db_with_timestamp_compaction_test.cc: 11,
db_readonly_with_timestamp_test.cc: 19, udt_util_test.cc: 14), remote and tiered compaction
(compaction_service_test.cc: 46, tiered_compaction_test.cc: 26), trie index (230),
the Customizable framework (74), statistics history (stats_history_test.cc: 10), trace
(block_cache_tracer_test.cc: 7, io_tracer_test.cc: 5).

---

## 4. Concurrency and memory model

Each item gives what RocksDB does (SOURCE), then what the port does (DECISION) and why, under
mantle's rules. "Preserve" means the observable behaviour is kept; "change" means it is
deliberately different and the difference is part of the contract.

mantle's use shapes several choices: one engine instance per range replica, written by that
range's Raft apply thread alone, with the WAL off and each batch carrying its applied index
(note 12 §6.1–§6.2); many instances per process sharing memory, cache, I/O and deletion
budgets (note 12 §6.1).

### 4.1 Memtable: skiplist, arena, concurrent insert

**RocksDB.**
- `InlineSkipList` stores each key inline after its node; the `next_` pointers for levels ≥ 1
  sit *before* the node struct and level n is `&next_[0] - n`; no field holds the height,
  which is stashed in `next_[0]` between `AllocateKey` and `Insert`
  [R memtable/inlineskiplist.h:352-374, :359-372, :861-869].
- Defaults `max_height = 12`, `branching_factor = 4`, `kMaxPossibleHeight = 32`; height grows
  with probability 1/4 per level from a thread-local RNG [R inlineskiplist.h:70, :76-78,
  :559-573].
- Ordering: `Next` acquire, `SetNext` release, `CASNext` strong acq_rel, `NoBarrier_*` relaxed,
  `max_height_` relaxed with a weak-CAS raise [R inlineskiplist.h:379-407, :1035-1044;
  util/atomic.h:57-120].
- `InsertConcurrently` links level by level with `NoBarrier_SetNext` on the new node then a CAS
  on the predecessor, re-finding the splice on CAS failure; a duplicate (key, seq) returns
  false, which `MemTable::Add` turns into `TryAgain` [R inlineskiplist.h:913-920, :1134-1172;
  db/memtable.cc:1167-1209].
- Concurrent insert is on by default (`allow_concurrent_memtable_write = true`) and supported
  by SkipListRep and VectorRep only [R include/rocksdb/options.h:1434;
  include/rocksdb/memtablerep.h:385, :401-423, :442-458]; the range-deletion table is always a
  skiplist [R db/memtable.cc:207-210].
- Memory: `ConcurrentArena` over an `Arena` (inline block 2,048 bytes, blocks 4 KiB–2 GiB,
  per-core shards of `min(128 KiB, block_size/8)`) [R memory/arena.h:31-35;
  memory/concurrent_arena.cc:26-31]. `ShouldFlushNow` compares arena-allocated bytes with
  `write_buffer_size` using `kAllowOverAllocationRatio = 0.6` [R db/memtable.cc:295-367].
- Entry encoding inside the memtable: `varint32 ikey_len | user_key | fixed64(seq<<8|type) |
  varint32 val_len | value | [checksum]` [R db/memtable.cc:1123-1154]. This is not an on-disk
  format; the port may lay entries out differently.

**Port (DECISION).**
- **Change: single writer, many readers.** A range's batches arrive from one apply thread, so
  concurrent insert buys nothing and costs `unsafe`. The port's memtable accepts inserts from
  one writer at a time (enforced by `&mut`-style ownership of the insert handle, not a
  runtime lock) and reads from any thread. `allow_concurrent_memtable_write` is accepted and
  ignored, and the OPTIONS file records the value the port actually uses (`false`).
- **Preserve:** the skiplist's ordering (internal-key order, §1.3), duplicate detection at
  (user key, seq) returning a typed `Duplicate` that the write path handles as RocksDB's
  `TryAgain`, the flush trigger arithmetic of `ShouldFlushNow` with its cited constants, and
  the range-deletion table.
- **Structure without `unsafe`:** nodes live in an append-only, chunked node store (chunks
  allocated once and never moved; each slot a `std::sync::OnceLock` written once before its
  index is published, so readers never see a partial node), and
  links are `AtomicU32` node indices with release stores on publish and acquire loads on
  traversal — the same orderings as RocksDB's. Key and value bytes are held per node. The
  store's capacity is the memtable's byte budget; reaching it is the flush trigger, and an
  insert past a hard ceiling is a typed refusal (CLAUDE.md §2).
- **Measured, not assumed:** the per-entry memory and insert/lookup time of this design are
  benchmarked against the C++ skiplist (`db_bench --benchmarks=fillrandom,readrandom
  --disable_wal=1` on the same device) before P7 ends; if it loses by more than the owner
  accepts, the fallback options are in §5 R5.

### 4.2 Write path: group commit, pipelined and unordered writes

**RocksDB.**
- Writers push onto a lock-free LIFO (`newest_writer_`, CAS); the first becomes leader, the
  others wait in `AwaitState`: 200 spin iterations (about 1 µs by the source's comment), then adaptive `yield` up to
  `write_thread_max_yield_usec = 100` µs, then a mutex/condvar block
  [R db/write_thread.cc:36-62, :64-210, :226-260; options.h:1442, :1458, :1468].
- The leader groups compatible writers (same `sync`, `no_slowdown`, `disableWAL`,
  protection bytes, rate-limiter priority) up to `max_write_batch_group_size_bytes = 1 MiB`,
  or leader size + 128 KiB when the leader is small [R write_thread.cc:440-569;
  options.h:1444-1449].
- Groups of ≥ 20 writers fan out parallel memtable inserts with a √n stride; merges disable
  parallel insert [R write_thread.cc:680-715; db_impl_write.cc:1269-1284].
- Sequence numbers: the group takes `[LastSequence()+1, +count]`; reads see a write only after
  `SetLastSequence` (release store), done after all memtable inserts by the thread that exits
  the group [R db_impl_write.cc:1385-1388, :1535-1556; db/version_set.h:1429-1448].
- With `disableWAL`, the WAL write and sync are skipped, `has_unpersisted_data_` is set, and
  everything else — stall checks, flush scheduling, grouping, memtable insert, sequence
  publication — runs [R db_impl_write.cc:1356-1383]. `sync` with `disableWAL` is rejected
  [R :941-943].
- Pipelined write (separate WAL and memtable queues), unordered write and two write queues
  are options, all off by default [R options.h:1398, :1424, :1558].

**Port (DECISION).**
- **Preserve** the sequence-number contract exactly: allocation per record (not per batch;
  `seq_per_batch` is a transactions feature), publication after the memtable insert, and
  `kPersistedTier` reading only flushed data when the WAL is off
  [R include/rocksdb/options.h:1934-1942].
- **Change: no group commit machinery for mantle's path.** One writer per instance means the
  write queue has one member; the port keeps a leader/follower queue only because the
  port's WAL-on mode (kept for fidelity, §1.5) needs it, and bounds it: at most N queued
  writers (N a stated constant with its derivation), a full queue answers `Busy`. No spinning
  or yielding: a waiting writer blocks on a condvar with a deadline (`std::thread::sleep` is a
  disallowed method, clippy.toml).
- **Drop** pipelined, unordered and two-queue writes (they exist for concurrent writers).
- **Write stalls preserve their triggers** — stop at `max_write_buffer_number` unflushed
  memtables, `level0_stop_writes_trigger` (36), `hard_pending_compaction_bytes_limit`
  (256 GiB); delay at `level0_slowdown_writes_trigger` (20),
  `soft_pending_compaction_bytes_limit` (64 GiB) [R db/column_family.cc:1010-1051;
  advanced_options.h:547, :554, :709, :717] — but **change their effect**: RocksDB's delay
  sleeps in 1,001 µs slices and its stop waits unboundedly on `bg_cv_`
  [R db_impl_write.cc:2841-2884]. The port answers a stopped write with a typed `Stalled`
  refusal at once, and a delayed write waits at most the delay the WriteController computed
  (a deadline, not a loop), so the Raft apply loop decides what to do (CLAUDE.md §2; note 12
  §6.1 "no_slowdown → Busy").

### 4.3 SuperVersion, Version and file lifetime

**RocksDB.**
- A `SuperVersion` bundles `mem`, `imm` (a `MemTableListVersion`), `current` (a `Version`) and
  the mutable CF options, with an atomic refcount; readers take it through a thread-local
  cache (`local_sv_` holding `kSVInUse`/`kSVObsolete` markers), and `InstallSuperVersion`
  (under the DB mutex) scrapes every thread's cached pointer
  [R db/column_family.h:207-276; column_family.cc:512-594, :1372-1504].
- `Version::refs_` is a plain `int` under the DB mutex; a Version's destructor drops its
  files' refs and queues files at zero as obsolete [R db/version_set.h:1185;
  version_set.cc:1013-1038, :4465-4475].
- Obsolete-file deletion keeps any file number ≥ the smallest pending output of a running
  job [R db/db_impl/db_impl_files.cc:122-128, :213-265, :676-679].
- Snapshots are a doubly linked list ordered by sequence number under the DB mutex; flush and
  compaction take the list (`InitSnapshotContext`) to decide which versions to keep
  [R db/snapshot_impl.h:54-150; db_impl_compaction_flush.cc:5358-5398].

**Port (DECISION).**
- **Preserve** the lifetime rules: a reader pins mem + imm + Version together; a table file is
  deleted only when no Version references it and its number is below every running job's
  pending output; compaction keeps every version visible to some snapshot.
- **Change the mechanism:** `SuperVersion` is an immutable `Arc<SuperVersion>` swapped
  atomically (an `ArcSwap`-style cell built on `std` primitives — a `Mutex<Arc<_>>` read by
  clone, measured first; RocksDB's thread-local cache exists to avoid that lock, and the port
  adds a per-thread cache only if the measurement shows contention). `Version` and file
  metadata are `Arc`s; "refcount reaches zero" becomes `Drop`, which sends the file number to
  a bounded obsolete-file channel instead of deleting in the destructor (no I/O in `Drop`).
- Snapshots are an ordered set keyed by sequence number with a bounded count; taking one past
  the bound is a typed refusal.

### 4.4 Locks

**RocksDB.** One DB mutex (`mutex_`) guards most column-family state, scheduling counters, the
snapshot list, the WriteController and the slow paths of `PreprocessWrite`;
`options_mutex_` is taken before `mutex_`; `wal_write_mutex_` after `mutex_`
[R db/db_impl/db_impl.h:1440-1460, :3158-3176; db_impl_write.cc:2101-2213].

**Port (DECISION).** Preserve the single state mutex and the lock order (options → state →
WAL), because the correctness arguments in RocksDB's comments assume them. A poisoned lock is
a typed error that fences the instance (CLAUDE.md §1). No I/O is done while holding the state
mutex except what RocksDB itself does under it (MANIFEST writes are done with the mutex
released, as in `LogAndApply`).

### 4.5 Background jobs

**RocksDB.**
- Thread pools per priority `{BOTTOM, LOW, HIGH, USER}` with an unbounded `std::deque` queue
  each [R include/rocksdb/env.h:441; util/threadpool_imp.cc:126-174, :401-425].
- `max_background_jobs = 2` split as flushes `max(1, jobs/4)` and compactions
  `max(1, jobs − flushes)`; compactions are throttled to 1 unless the WriteController asks for
  speed-up [R options.h:900; db_impl_compaction_flush.cc:3453-3478]. Flushes run in HIGH (LOW
  if HIGH is empty), compactions in LOW, bottommost compactions may be forwarded to BOTTOM
  [R :3357-3451, :4784-4816].
- The error handler grades errors by severity; retryable I/O errors start an auto-recovery
  thread retrying up to `max_bgerror_resume_count = INT_MAX` times
  [R db/error_handler.cc:413-532, :688-752; options.h:1707, :1714].

**Port (DECISION).**
- **Change: one set of workers per process, shared by all instances** (note 12 §6.1), with
  a bounded job queue per priority; an instance whose job cannot be queued records "work
  pending" and is re-offered when a worker frees (the queue holds at most one entry per
  instance and priority, so its bound is the instance count).
- **Preserve** the flush/compaction split and the throttle rule as the per-instance job limit,
  and flush priority over compaction.
- **Change: no auto-resume.** A background error at hard or fatal severity stops writes on
  that instance and surfaces a typed error; mantle fences the replica (note 12 §6.2). Retry
  counts and intervals are therefore not ported.

### 4.6 Iterators and reads

**RocksDB.** `DBIter` over a `MergingIterator` (binary heap; range tombstones inserted as
heap items of type `DELETE_RANGE_START`/`END`, with a set of active levels) over memtable and
table iterators; `PinnedIteratorsManager` pins blocks for `pin_data`; the implicit snapshot
is read *after* referencing the SuperVersion [R table/merging_iterator.cc:15-52, :155-193,
:497, :573-664; db/pinned_iterators_manager.h:19-90; db/db_impl/db_impl.cc:4182-4202].

**Port (DECISION).** Preserve the algorithm (it is where range-deletion correctness lives,
§5 R11) and the snapshot-after-pin order. Iterators own `Arc`s to what they read (blocks,
tables, the SuperVersion), so pinning is ownership; the number of live iterators per instance
is bounded.

### 4.7 Shared budgets: WriteBufferManager, RateLimiter, SstFileManager

**RocksDB.**
- `WriteBufferManager(buffer_size, cache, allow_stall)`: flush when mutable usage exceeds 7/8
  of the limit or total usage exceeds the limit with half of it mutable; stall (if allowed)
  at ≥ the limit; optional charging of memtable memory to the block cache in 256 KiB dummy
  entries [R memtable/write_buffer_manager.cc:25-38; include/rocksdb/write_buffer_manager.h:
  101-142; cache/cache_reservation_manager.h:206].
- `GenericRateLimiter(rate, refill_period_us = 100,000, fairness = 10)`: token bucket per
  period, per-priority FIFO queues, `IO_USER` first, probabilistic fairness among the rest;
  optional auto-tuning every 100 periods within [max/20, max]
  [R include/rocksdb/rate_limiter.h:166-170; util/rate_limiter.cc:105-132, :195-481].
- `SstFileManager`: `max_allowed_space` (0 = unlimited), compaction space reservation, and a
  `DeleteScheduler` that renames files to `*.trash` and deletes at `rate_bytes_per_sec`,
  truncating files larger than `bytes_max_delete_chunk` (64 MiB) in chunks, unless trash
  exceeds `max_trash_db_ratio` (0.25) of live data [R include/rocksdb/sst_file_manager.h:
  120-129; file/sst_file_manager_impl.cc:32, :142-200; file/delete_scheduler.cc:60-398].

**Port (DECISION).** Preserve all three as process-wide objects shared across instances, with
their arithmetic. Change three things to meet CLAUDE.md §2: the rate limiter's per-priority
queues are bounded (a full queue is `Busy`); the WriteBufferManager's stall is a typed
refusal to the caller rather than a blocked thread; `max_allowed_space` and the trash ratio
have no "unlimited" default — mantle sets them from the device's measured capacity (CLAUDE.md
§5). The per-device rate limiter's rate is the device's measured bandwidth share (note 12
§6.6), not a constant.

### 4.8 Block cache

**RocksDB.** The default block cache is `AutoHyperClockCache` of 32 MiB
[R table/block_based/block_based_table_factory.cc:471-478; cache/clock_cache.cc:3615-3618].
HyperClock keeps one 64-bit atomic meta word per slot (acquire counter 30 bits, release
counter 30 bits, hit, occupied, shareable, visible flags) and evicts by CLOCK countdown
(high 3, low 2, bottom 1) [R cache/clock_cache.h:312-417]. LRUCache is sharded (≤ 6 shard bits
by default), with high/low/bottom priority pools and a mutex per shard
[R cache/sharded_cache.cc:129-139; cache/lru_cache.h:382-437].

**Port (DECISION).** The cache is a process-wide bound shared by all instances. HyperClock's
slot protocol is a state machine on one `AtomicU64` per slot and ports to `std` atomics with
the same orderings. The value beside the meta word is the part C++ reads without a lock
through a raw pointer; in safe Rust the baseline is a per-slot lock held only to clone an
`Arc` of the block, which keeps the eviction order and the capacity accounting exact but adds
a lock to every hit. Whether that baseline keeps HyperClock's throughput under many reader
threads is measured (P9, `db_bench --benchmarks=readrandom` with the oracle alongside); if it
does not, the choice among a vetted dependency, a sharded LRU (which RocksDB also offers), or
widening CLAUDE.md §7 is the owner's (§5 R5 has the same shape). Preserve the meta-word state
machine and eviction order; test with the ported lru_cache_test.cc `ClockCacheTest` cases. LRU
is ported second, for option fidelity. Cache keys are the port's own (not an on-disk format),
derived as RocksDB does from the table's unique id (§1.12) so that the same file keeps the
same key across reopen.

---

## 5. Known hard parts and risks

Size is the C++ that carries the risk (source lines, DERIVED from §2's counts) and a class:
**S** (a module a phase absorbs), **M** (a phase of its own), **L** (several phases or a
design decision that blocks one), **XL** (can decide whether the port is viable). Each risk
names what retires it.

| # | Risk | Evidence | Size | Retired by |
|---|---|---|---|---|
| R1 | **Hash64 is not XXH3.** `Hash64` is `XXPH3_64bits`, the xxHash 0.7.2 *preview* of XXH3 vendored as `util/xxph3.h`, not the released algorithm [R util/hash.cc:81-88; util/xxph3.h:39-43, :133-135]. It keys the FastLocalBloom and Ribbon filter probes [R table/block_based/filter_policy.cc:77, :600, :1041] (§1.11), so no published crate reproduces those filter bits. The released XXH3 (`twox-hash`) is correct only for the kXXH3 *block checksum* and the WAL/record XXH3 [R table/format.cc:624-634; db/log_reader.cc:115]. | 1,760 lines (xxph3.h), of which the 64-bit path is a fraction | S in code, but a wrong answer if missed: probing RocksDB's filters with a different hash gives false negatives, so `Get` reports existing keys as absent, and RocksDB reading the port's filters does the same. Tests that only write and read with the port pass anyway, so the P1 vectors and P5 byte identity are what catch it | P1 golden vectors for `Hash64` over lengths 0–4,096 and seeds; P5 filter byte identity |
| R2 | **Legacy `Hash()` sign-extends tail bytes** through `int8_t` [R util/hash.cc:45-59]; the legacy bloom filter (`BloomHash`, seed 0xbc9f1d34) [R table/block_based/filter_policy.cc:1149, :1330] and the data-block hash index (`GetSliceHash`, seed 397) [R table/block_based/data_block_hash_index.cc:24, :100] depend on it. A Rust port with `u8` tails gives different bits for bytes ≥ 0x80; in a data block with a hash index that sends a point lookup to the wrong bucket, where a bucket marked empty answers "not in this block" for a key that is there (§1.6). | 41 lines | S, but a wrong answer, not a slow one | P1 vectors with high-bit bytes |
| R3 | **Byte-identical block-based tables across format_version 2–7**, context checksums, index value delta encoding, partitioned index/filter, compression framing (§1.6–§1.13). | table/ 12,303 + table/block_based/ 26,627 | L | P4 byte identity and `sst_dump` over the whole corpus of §3.0 |
| R4 | **Ribbon filter** construction (banding, back-substitution, the configuration tables) must be bit-exact to produce the same filter bytes; reading needs only the query side. | util/ribbon_alg.h 1,225, ribbon_impl.h 1,137, ribbon_config.cc 498 | M | P5; ribbon_test.cc; byte identity |
| R5 | **Lock-free memtable without `unsafe`.** The C++ skiplist stores nodes in an arena, links by raw pointer, and inserts concurrently by CAS (§4.1). mantle forbids `unsafe` outside OS-interface files (CLAUDE.md §7; scripts/check-contracts.py `UNSAFE_ALLOWED`). A safe Rust design exists for mantle's case — one writer per range (the Raft apply thread), many readers — but its memory cost and speed must be measured against the C++ (§4.1). If it loses, the choice is a vetted dependency (which then sits behind an unwind boundary, CLAUDE.md §1) or widening the rule, which is the owner's decision. | inlineskiplist.h 1,422; arena 316; concurrent_arena 260 | L (design) | P2 benchmark against `db_bench --benchmarks=fillrandom --disable_wal` on the same device, recorded |
| R6 | **5,634 `assert()` calls in the engine directories** (DERIVED, grep over non-test files of db/, table/, memtable/, cache/, util/, options/, file/, env/, monitoring/, memory/, logging/, utilities/; how many fall in the kept files is counted per module as it is ported). In release C++ they vanish; in the port each is either a type invariant or a typed error (CLAUDE.md §1; `debug_assert!` is a disallowed macro, clippy.toml). Many guard decode paths, where a C++ release build reads out of bounds on a corrupt file and the port must return `Corruption`. | spread over the kept ≈210,000 lines | L | Review per module; a fuzz target per decoder (block, index, filter, footer, VersionEdit, WriteBatch, WAL record, blob record, wide-column entity, OPTIONS) fed the corpus and mutations of it |
| R7 | **Unbounded resources in the C++ design** that CLAUDE.md §2 forbids: `Env::Schedule` queues are unbounded (§4.5); `max_open_files = -1` (the default) keeps every table reader open; `max_manifest_file_size` bounds a MANIFEST only by rollover; the WriteThread queue is bounded only by the number of calling threads; `max_write_batch_group_size_bytes` bounds a group, not the queue; the info LOG and the WAL archive grow with time. Each needs a stated bound and a typed refusal or eviction rule in the port. | db_impl_compaction_flush.cc 5,476; table_cache 1,162; threadpool 670 | M | A bound table in the port's design record, one row per resource, each with a test that reaches it |
| R8 | **Test infrastructure.** 104 of 225 test files drive `SyncPoint`; 708 `TEST_SYNC_POINT*` sites sit in production source; 78 test files derive from `DBTestBase`; 29 use fault-injection file systems. Without equivalents the ported tests are not the same tests. | test_util 2,878 + db_test_util 3,483 + fault injection 3,764 | M | §3.0 harness before P7 |
| R9 | **Compression codecs.** Snappy (raw format), LZ4 block (with its format_version-2 varint32 size prefix), ZSTD (with dictionaries and, for the WAL, streaming frames with checksums), zlib raw deflate, BZip2 (§1.8). Pure-Rust codecs exist for Snappy (`snap`), LZ4 block (`lz4_flex`) and deflate (`miniz_oxide`); a complete ZSTD encoder with dictionary training does not exist in pure Rust, so ZSTD brings a C build (`zstd-sys`) with the cross-target lint consequence note 12 §5.8 records. A codec that can panic is called behind an unwind boundary (CLAUDE.md §1). | util/compression.cc 1,987 + .h 759 | M | P4 over the compression axis of the corpus; the owner's decision on the C dependency (or refuse ZSTD-compressed files and write none) |
| R10 | **Compaction behaviour is not a format but is a contract.** Differential checks of P10–P11 depend on the picker being deterministic given the same file set; RocksDB's picker uses file sizes (equal up to the variable-length properties of §3.0), `compaction_pri`, and time-based triggers (`periodic_compaction_seconds`, `ttl`) that must be driven by an injected clock. | db/compaction/ 17,581; version_set.cc score code | L | P10 with a mock clock; `ldb manifest_dump` equality |
| R11 | **Range tombstones in the read path.** MergingIterator's range-tombstone handling (skipping covered keys across levels), fragmentation, and truncation at file boundaries have a long bug history in RocksDB itself (MyRocks validation, note 12 §1.13). | table/merging_iterator.cc 1,769; db/range_* 1,921 | L | P12 tests, plus a randomized differential against a model (an ordered map with explicit tombstones) and against the oracle |
| R12 | **Durability ordering with the WAL off.** mantle runs `disableWAL` with the Raft log as the only WAL (note 12 §6.2). The port must keep RocksDB's flush ordering exactly — table synced, then MANIFEST record synced, then (on rollover) CURRENT via temp file, rename and directory sync (§1.14–§1.15) — and must use the platform's full flush for every one of them (CLAUDE.md §6), which RocksDB's own macOS build does not unless `HAVE_FULLFSYNC` is defined [R env/io_posix.cc:1826-1838; note 12 §6.2 item 4]. The port's `kPersistedTier` read must report exactly what that ordering made durable. | db/flush_job.cc 1,319; version_set.cc LogAndApply; file/filename.cc SetCurrentFile | M | P7 crash matrix on `SimFile` at every sync; `ldb` accepts every post-crash directory |
| R13 | **Options surface.** RocksDB 11.8.1 has several hundred options (include/rocksdb/options.h, advanced_options.h, table.h), many of them deprecated no-ops kept for OPTIONS-file compatibility. The port keeps the options that change behaviour it keeps, parses and ignores the documented no-ops, and refuses the rest when a file or OPTIONS entry would need them. Getting this list exact is tedious and error-prone. | options/ 7,842 | M | P6: the oracle's OPTIONS files from every corpus DB parse; the port's OPTIONS files verify under `VerifyRocksDBOptionsFromFile` at `kSanityLevelLooselyCompatible` |
| R14 | **Error semantics.** RocksDB's background error handler classifies errors by severity and may auto-resume [R db/error_handler.cc:413-532, :688-752]; mantle fences a replica on a fatal engine error and never retries automatically (note 12 §6.2 "Failure"; `BlockFile::sync_data` doc: a failed flush leaves durability unknown). The port keeps the classification (which errors stop writes) and drops auto-resume. | db/error_handler.cc 870 + .h 165 | S | error_handler_fs_test.cc less its recovery tests |
| R15 | **Scale of the port.** About 210,000 lines of C++ source are in the kept scope (§2, DERIVED), and the phase table names 118 test files (about 219,600 lines, about 3,360 test definitions). For comparison, `lsm-tree` 3.1.10 plus `fjall` is about 42,700 lines of Rust (note 12 §5.6). | — | XL | Phasing (§3) so that each phase is useful on its own: P1–P5 alone give a reader and writer of RocksDB tables (ingest, export, inspection), P1–P8 an engine without compaction, P10 the engine note 12 §6 needs |
| R16 | **mantle-disk simulates files, not directories.** `SimFile` models power loss for one file's sectors (`crates/disk/src/sim.rs`), and `BlockFile` has no truncate. RocksDB's durability depends on directory operations too: create + directory fsync for new tables, rename for CURRENT/IDENTITY/OPTIONS, hard links for checkpoints, unlink for obsolete files [R file/filename.cc:429-520; utilities/checkpoint/checkpoint_impl.cc:321-480]. A crash between a rename and its directory fsync, or a lost unsynced create, must be simulated or the P7/P13 crash matrices test less than they claim. | new: a simulated directory (names → files, with rename/link/unlink durable only after `sync_dir`) and `set_len` on `BlockFile` | M | The simulated directory with its own tests (the Pillai et al. OSDI 2014 cases mantle-disk's header already cites) before P7 |
| R17 | **Background work under simulation.** RocksDB's flush and compaction run on OS threads scheduled by the kernel; mantle's simulator is deterministic and seed-replayable (docs/design/metadata.md §5; note 06 §C.d). The port must be able to run its background jobs as steps the simulator schedules (a job is a state machine driven by the caller), or its crash and fault tests on the engine are not replayable from a seed. | db_impl_compaction_flush.cc 5,476 | M | A scheduler trait with a threaded and a stepped implementation, from P7 |

---

## Appendix A: counts at the pinned commit (DERIVED)

| What | Count |
|---|---|
| Test files (`*_test.cc`, outside java/ and third-party/) | 225 files, 321,245 lines, 4,838 test definitions |
| Test files that drive `SyncPoint` | 104 |
| Test files deriving from `DBTestBase` | 78 |
| Test files using `FaultInjectionTestFS`/`Env` | 29 |
| `TEST_SYNC_POINT*` sites in non-test source of the engine directories | 708 |
| `assert(` calls in non-test source of the engine directories | 5,634 |
| Public headers `include/rocksdb/` | 88 files, 37,982 lines (+8,143 under `utilities/`) |
| Kept C++ source (§2 verdicts) | ≈210,400 lines |
| Test files named in §3's phase table | 118 files, ≈219,600 lines, ≈3,360 test definitions (some ported in part) |

"Engine directories" are db/, table/, memtable/, cache/, util/, options/, file/, env/,
monitoring/, memory/, logging/ and utilities/, non-test files only.

## Appendix B: mantle files this note relies on

- `CLAUDE.md` (rules 1–9 and the gates).
- `clippy.toml` (disallowed macros incl. `debug_assert!`, disallowed `std::thread::sleep`),
  `Cargo.toml` (workspace lints; `twox-hash`, `crc-fast`, `getrandom`, `rustix`,
  `windows-sys`, `proptest` dependencies), `scripts/check-contracts.py` (`UNSAFE_ALLOWED`,
  the constants inventory `docs/design/constants.md`).
- `crates/disk/src/block.rs` (`BlockFile`), `file.rs` (`DeviceFile`, `sync_dir`, the
  per-platform full flush), `buf.rs` (`Alignment`, `AlignedBuf`, `Pool`), `sim.rs`
  (`SimFile`, faults, crash modes), `workers.rs`.
- `crates/crc/src/lib.rs` (CRC-32C and CRC combination).
- `docs/design/metadata.md` §4–§5 (the engine interface, model engine, simulation) and
  `docs/design/raft-log.md` (the per-device log that replaces the WAL).
- Note 12 (§5.6, §5.8, §6.1–§6.3, §6.6) for why and how the engine is used.

## Appendix C: directory-level include graph (DERIVED)

Computed from `#include "…"` lines of non-test source files; an edge A → B means some file in A
includes a header in B (`include/rocksdb/` and self-edges omitted; `test_util` edges come from
`test_util/sync_point.h`, which production code includes for `TEST_SYNC_POINT`).

| Module | Includes headers of |
|---|---|
| cache | db/db_impl, env, logging, memory, monitoring, port, table, table/block_based, test_util, util |
| db | cache, db/blob, db/compaction, db/db_impl, db/wide, env, file, logging, memory, monitoring, options, port, table, table/block_based, table/plain, test_util, trace_replay, util, utilities |
| db/blob | cache, db, db/wide, file, logging, memory, monitoring, options, port, table, table/block_based, test_util, trace_replay, util |
| db/compaction | db, db/blob, db/db_impl, db/wide, file, logging, memory, monitoring, options, port, table, test_util, util |
| db/db_impl | db, db/blob, db/compaction, db/wide, env, file, logging, memtable, monitoring, options, port, table, table/block_based, test_util, trace_replay, util, utilities/trace |
| db/wide | db, db/blob, util |
| env | file, logging, memory, monitoring, options, port, test_util, trace_replay, util, utilities |
| file | db, db/compaction, db/db_impl, env, logging, monitoring, options, port, table, test_util, trace_replay, util, utilities |
| logging | file, memory, monitoring, port, test_util, util |
| memory | logging, port, test_util, util, utilities |
| memtable | cache, db, db/db_impl, memory, monitoring, port, test_util, util |
| monitoring | db, db/db_impl, port, test_util, util |
| options | db, file, logging, monitoring, port, table/block_based, test_util, util |
| port | env, logging, monitoring, test_util, util |
| table | cache, db, db/blob, db/db_impl, db/wide, env, file, logging, memory, monitoring, options, port, table/block_based, table/cuckoo, table/plain, test_util, trace_replay, util |
| table/block_based | cache, db, db/blob, db/compaction, db/wide, file, logging, memory, monitoring, options, port, table, test_util, trace_replay, util, utilities/trie_index |
| util | db, db/db_impl, db/wide, file, logging, memory, monitoring, options, port, table, table/block_based, table/plain, test_util |
| utilities/checkpoint | db, file, logging, port, test_util, util, utilities/copy_engine |
| utilities/transactions | db, db/db_impl, file, logging, monitoring, port, table, test_util, util, utilities, utilities/merge_operators, utilities/secondary_index, utilities/write_batch_with_index |

