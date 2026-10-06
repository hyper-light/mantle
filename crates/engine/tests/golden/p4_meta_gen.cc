// Golden meta blocks for mantle-engine P4: RocksDB 11.8.1's own PropertyBlockBuilder,
// ParsePropertiesBlock, MetaIndexBuilder and FindOptionalMetaBlock (~/Projects/rocksdb at
// abeebd963: table/meta_blocks.cc).
//
// 128 property blocks from a SplitMix64-driven script. Each sets every number of TableProperties
// to 0, a small number or a large one, each optional number set or not, each string empty or
// random bytes (not always UTF-8), and up to 8 user-collected properties, sometimes the external
// file's global sequence number among them. In one in eight, one of the table's own numbers is
// replaced by a varint that does not end. The block is built,
// then parsed at a random file offset.
// Then 64 meta-index blocks of up to 12 names and handles, each looked up by every name it holds
// and by names it does not.
//
// Output, one line per block:
//   P offset numbers(36, comma-separated) strings(12, hex, comma-separated, - for empty)
//     user(name_hex=value_hex;...) malformed(name_hex or -) block_hex parsed_numbers
//     parsed_strings parsed_user global_seqno_offset
//   M names_and_handles(name_hex=offset,size;...) block_hex lookups(name_hex=offset,size|-;...)
// The numbers are in the order of `kNumbers` below; the strings in that of `kStrings`.
//
// Built outside the repository against an unmodified checkout (R=~/Projects/rocksdb) and the
// static library of ~/Projects/rocksdb-golden/lib (L), on aarch64 macOS:
//   clang++ -std=c++20 -O2 -DNDEBUG -march=armv8-a+crc+crypto -I$R -I$R/include \
//     -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_MACOSX p4_meta_gen.cc \
//     $L/librocksdb.a -o p4_meta_gen
//   ./p4_meta_gen > p4_meta.txt
// crates/engine/tests/meta_blocks_golden.rs builds and parses the same blocks with the port.
#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "options/cf_options.h"
#include "rocksdb/options.h"
#include "rocksdb/table_properties.h"
#include "table/block_based/block.h"
#include "table/block_based/block_builder.h"
#include "table/meta_blocks.h"
#include "table/sst_file_writer_collectors.h"
#include "util/coding.h"

using namespace ROCKSDB_NAMESPACE;

namespace ROCKSDB_NAMESPACE {
Status ParsePropertiesBlock(const ImmutableOptions& ioptions, uint64_t offset,
                            Block& properties_block,
                            std::unique_ptr<TableProperties>& new_table_properties);
}

namespace {
struct SplitMix64 {
  uint64_t s;
  uint64_t next() {
    uint64_t z = (s += 0x9e3779b97f4a7c15ull);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ull;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebull;
    return z ^ (z >> 31);
  }
  uint64_t below(uint64_t n) { return next() % n; }
  std::string bytes(size_t n) {
    std::string s(n, '\0');
    for (auto& c : s) c = static_cast<char>(next());
    return s;
  }
};

std::string hex(const Slice& s) { return s.empty() ? "-" : s.ToString(true); }

// The numbers, in the order the output lists them, with the names RocksDB stores them under.
#define NUMBERS(X)                                                         \
  X(orig_file_number, kOriginalFileNumber)                                 \
  X(data_size, kDataSize)                                                  \
  X(index_size, kIndexSize)                                                \
  X(index_partitions, kIndexPartitions)                                    \
  X(top_level_index_size, kTopLevelIndexSize)                              \
  X(index_key_is_user_key, kIndexKeyIsUserKey)                             \
  X(index_value_is_delta_encoded, kIndexValueIsDeltaEncoded)               \
  X(udi_is_primary_index, kUDIIsPrimaryIndex)                              \
  X(filter_size, kFilterSize)                                              \
  X(raw_key_size, kRawKeySize)                                             \
  X(raw_value_size, kRawValueSize)                                         \
  X(num_data_blocks, kNumDataBlocks)                                       \
  X(num_data_blocks_compression_rejected, kNumDataBlocksCompressionRejected) \
  X(num_data_blocks_compression_bypassed, kNumDataBlocksCompressionBypassed) \
  X(num_uniform_blocks, kNumUniformBlocks)                                 \
  X(num_entries, kNumEntries)                                              \
  X(num_filter_entries, kNumFilterEntries)                                 \
  X(num_deletions, kDeletedKeys)                                           \
  X(num_merge_operands, kMergeOperands)                                    \
  X(num_range_deletions, kNumRangeDeletions)                               \
  X(format_version, kFormatVersion)                                        \
  X(fixed_key_len, kFixedKeyLen)                                           \
  X(column_family_id, kColumnFamilyId)                                     \
  X(creation_time, kCreationTime)                                          \
  X(oldest_key_time, kOldestKeyTime)                                       \
  X(newest_key_time, kNewestKeyTime)                                       \
  X(file_creation_time, kFileCreationTime)                                 \
  X(slow_compression_estimated_data_size, kSlowCompressionEstimatedDataSize) \
  X(fast_compression_estimated_data_size, kFastCompressionEstimatedDataSize) \
  X(tail_start_offset, kTailStartOffset)                                   \
  X(user_defined_timestamps_persisted, kUserDefinedTimestampsPersisted)    \
  X(key_largest_seqno, kKeyLargestSeqno)                                   \
  X(key_smallest_seqno, kKeySmallestSeqno)                                 \
  X(data_block_restart_interval, kDataBlockRestartInterval)                \
  X(index_block_restart_interval, kIndexBlockRestartInterval)              \
  X(separate_key_value_in_data_block, kSeparateKeyValueInDataBlock)

#define STRINGS(X)                                       \
  X(db_id)                                               \
  X(db_session_id)                                       \
  X(db_host_id)                                          \
  X(column_family_name)                                  \
  X(filter_policy_name)                                  \
  X(comparator_name)                                     \
  X(merge_operator_name)                                 \
  X(prefix_extractor_name)                               \
  X(property_collectors_names)                           \
  X(compression_name)                                    \
  X(compression_options)                                 \
  X(seqno_to_time_mapping)

std::string Numbers(const TableProperties& p) {
  std::string out;
#define PRINT(field, name) out += std::to_string(p.field) + ",";
  NUMBERS(PRINT)
#undef PRINT
  out.pop_back();
  return out;
}

std::string Strings(const TableProperties& p) {
  std::string out;
#define PRINT(field) out += hex(p.field) + ",";
  STRINGS(PRINT)
#undef PRINT
  out.pop_back();
  return out;
}

std::string User(const UserCollectedProperties& u) {
  std::string out;
  for (auto& [k, v] : u) out += hex(k) + "=" + hex(v) + ";";
  return out.empty() ? "-" : out;
}

uint64_t Number(SplitMix64& rng) {
  switch (rng.below(3)) {
    case 0:
      return 0;
    case 1:
      return rng.below(1000);
    default:
      return rng.next();
  }
}
}  // namespace

int main() {
  SplitMix64 rng{19};
  Options options;
  ImmutableOptions ioptions(options);
  for (int n = 0; n < 128; ++n) {
    TableProperties p;
#define SET(field, name) p.field = Number(rng);
    NUMBERS(SET)
#undef SET
    if (rng.below(2) == 0) p.key_largest_seqno = UINT64_MAX;
    if (rng.below(2) == 0) p.key_smallest_seqno = UINT64_MAX;
    if (rng.below(2) == 0) p.user_defined_timestamps_persisted = 1;
#define SET(field) p.field = rng.below(2) == 0 ? "" : rng.bytes(1 + rng.below(12));
    STRINGS(SET)
#undef SET
    UserCollectedProperties user;
    for (uint64_t i = rng.below(9); i > 0; --i) {
      user["user." + rng.bytes(1 + rng.below(6))] = rng.bytes(rng.below(10));
    }
    if (rng.below(4) == 0) {
      std::string seqno;
      PutFixed64(&seqno, rng.next());
      user[ExternalSstFilePropertyNames::kGlobalSeqno] = seqno;
      user[ExternalSstFilePropertyNames::kVersion] = std::string("\x02\x00\x00\x00", 4);
    }
    PropertyBlockBuilder b;
    b.AddTableProperty(p);
    b.Add(user);
    std::string bytes = b.Finish().ToString();
    std::string malformed;
    if (n % 8 == 3) {
      // One of the table's numbers replaced by a varint that does not end: the block rebuilt
      // from its entries, in their order, with that value changed.
      static const std::string kNames[] = {TablePropertiesNames::kRawKeySize,
                                           TablePropertiesNames::kFormatVersion,
                                           TablePropertiesNames::kNumEntries};
      malformed = kNames[rng.below(3)];
      Block built(BlockContents(Slice(bytes)), 0, nullptr, 1);
      std::unique_ptr<MetaBlockIter> it(built.NewMetaIterator());
      BlockBuilder rebuilt(std::numeric_limits<int32_t>::max());
      for (it->SeekToFirst(); it->Valid(); it->Next()) {
        rebuilt.Add(it->key(), it->key() == malformed ? std::string(11, '\xff')
                                                      : it->value().ToString());
      }
      bytes = rebuilt.Finish().ToString();
    }
    uint64_t offset = rng.below(1 << 30);
    Block block(BlockContents(Slice(bytes)), 0, nullptr, 1);
    auto parsed = std::make_unique<TableProperties>();
    Status s = ParsePropertiesBlock(ioptions, offset, block, parsed);
    if (!s.ok()) return 1;
    printf("P %llu %s %s %s %s %s %s %s %s %llu\n", static_cast<unsigned long long>(offset),
           Numbers(p).c_str(), Strings(p).c_str(), User(user).c_str(), hex(malformed).c_str(),
           hex(bytes).c_str(), Numbers(*parsed).c_str(), Strings(*parsed).c_str(),
           User(parsed->user_collected_properties).c_str(),
           static_cast<unsigned long long>(parsed->external_sst_file_global_seqno_offset));
  }
  for (int n = 0; n < 64; ++n) {
    MetaIndexBuilder b;
    std::map<std::string, BlockHandle> entries;
    for (uint64_t i = rng.below(13); i > 0; --i) {
      std::string name = "rocksdb." + rng.bytes(1 + rng.below(8));
      if (entries.count(name)) continue;
      BlockHandle h(rng.below(1ull << 40), rng.below(1 << 20));
      entries[name] = h;
      b.Add(name, h);
    }
    std::string bytes = b.Finish().ToString();
    printf("M ");
    if (entries.empty()) printf("-");
    for (auto& [name, h] : entries) {
      printf("%s=%llu,%llu;", hex(name).c_str(), static_cast<unsigned long long>(h.offset()),
             static_cast<unsigned long long>(h.size()));
    }
    printf(" %s ", hex(bytes).c_str());
    Block block(BlockContents(Slice(bytes)), 0, nullptr, 1);
    std::unique_ptr<MetaBlockIter> it(block.NewMetaIterator());
    std::vector<std::string> names;
    for (auto& [name, h] : entries) names.push_back(name);
    for (int i = 0; i < 4; ++i) names.push_back("rocksdb." + rng.bytes(1 + rng.below(8)));
    for (auto& name : names) {
      BlockHandle h;
      Status s = FindOptionalMetaBlock(it.get(), name, &h);
      if (!s.ok()) return 1;
      printf("%s=", hex(name).c_str());
      if (h.IsNull()) {
        printf("-;");
      } else {
        printf("%llu,%llu;", static_cast<unsigned long long>(h.offset()),
               static_cast<unsigned long long>(h.size()));
      }
    }
    printf("\n");
  }
  return 0;
}
