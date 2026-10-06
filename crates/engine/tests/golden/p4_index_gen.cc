// Golden indexes for mantle-engine P4, built by RocksDB 11.8.1's own index builders
// (~/Projects/rocksdb at abeebd963: table/block_based/index_builder.{h,cc}).
//
// 240 tables from a SplitMix64-driven script, a quarter each of the four index types: binary
// search, binary search with first keys, hash search (fixed, capped or no-op prefixes) and
// two-level. Each draws a comparator (bytewise or reverse), a format_version (2, 3, 4, 5 or 7;
// value delta encoding from 4), a shortening mode, an index restart interval (1, 2 or 16; one for
// hash search), a uniformity threshold (none or 0.5), and for two-level a partition size (64, 256
// or 4096) and deviation (0, 10 or 50). Its keys are internal keys in comparator order: in half
// the tables after a common prefix, so adjacent separators share bytes, and in half with some user
// keys repeated at falling sequence numbers, so adjacent blocks can share one. They are cut into
// blocks of 1 to 6 keys. Block handles follow each other with the 5-byte trailer between; now and
// then a block starts after a gap and is added with skip_delta_encoding.
//
// Output, one line per table:
//   type comparator format_version shortening interval threshold prefix partition deviation
// then each block as keys_hex(comma-separated)/offset/size/skip, then after each AddIndexEntry
// the separator returned and CurrentIndexSizeEstimate (and for two-level ShouldCutFilterBlock),
// then each Finish's index block with the handle it was written at, the meta blocks, IndexSize,
// NumUniformIndexBlocks and separator_is_key_plus_seq.
//
// Built outside the repository against an unmodified checkout (R=~/Projects/rocksdb) and the
// static library of ~/Projects/rocksdb-golden/lib (L), on aarch64 macOS:
//   clang++ -std=c++20 -O2 -DNDEBUG -march=armv8-a+crc+crypto -I$R -I$R/include \
//     -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_MACOSX p4_index_gen.cc \
//     $L/librocksdb.a -o p4_index_gen
//   ./p4_index_gen > p4_index.txt
// crates/engine/tests/index_builder_golden.rs builds the same indexes with the port.
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "table/block_based/index_builder.h"
#include "table/format.h"

using namespace ROCKSDB_NAMESPACE;

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
    for (auto& c : s) c = static_cast<char>("abcdefgh"[next() % 8]);
    return s;
  }
};

std::string hex(const Slice& s) { return s.ToString(true); }

const BlockBasedTableOptions::IndexType kTypes[] = {
    BlockBasedTableOptions::kBinarySearch,
    BlockBasedTableOptions::kBinarySearchWithFirstKey,
    BlockBasedTableOptions::kHashSearch,
    BlockBasedTableOptions::kTwoLevelIndexSearch};
const uint32_t kVersions[] = {2, 3, 4, 5, 7};
const int kIntervals[] = {1, 2, 16};
const uint64_t kPartitions[] = {64, 256, 4096};
const int kDeviations[] = {0, 10, 50};
}  // namespace

int main() {
  SplitMix64 rng{13};
  for (int n = 0; n < 240; ++n) {
    auto type = kTypes[n % 4];
    const Comparator* ucmp =
        rng.below(4) == 0 ? ReverseBytewiseComparator() : BytewiseComparator();
    InternalKeyComparator icmp(ucmp);
    BlockBasedTableOptions opts;
    opts.index_type = type;
    opts.format_version = kVersions[rng.below(5)];
    opts.index_shortening =
        static_cast<BlockBasedTableOptions::IndexShorteningMode>(rng.below(3));
    opts.index_block_restart_interval =
        type == BlockBasedTableOptions::kHashSearch ? 1 : kIntervals[rng.below(3)];
    opts.uniform_cv_threshold = rng.below(2) == 0 ? -1.0 : 0.5;
    opts.metadata_block_size = kPartitions[rng.below(3)];
    opts.block_size_deviation = kDeviations[rng.below(3)];
    int prefix_kind = static_cast<int>(rng.below(3));
    size_t prefix_len = 1 + rng.below(3);
    std::unique_ptr<const SliceTransform> prefix(
        prefix_kind == 0   ? NewFixedPrefixTransform(prefix_len)
        : prefix_kind == 1 ? NewCappedPrefixTransform(prefix_len)
                           : NewNoopTransform());
    InternalKeySliceTransform ikst(prefix.get());
    bool value_delta = opts.format_version >= 4;
    std::unique_ptr<IndexBuilder> b(IndexBuilder::CreateIndexBuilder(
        type, &icmp, &ikst, value_delta, opts, 0, true));

    // User keys of at least the fixed prefix's length, sorted, some repeated.
    std::vector<std::string> users;
    size_t count = 1 + rng.below(80);
    std::string common = rng.below(2) == 0 ? rng.bytes(1 + rng.below(6)) : "";
    for (size_t i = 0; i < count; ++i) {
      users.push_back(common + rng.bytes(3 + rng.below(6)));
    }
    std::sort(users.begin(), users.end(),
              [&](const std::string& a, const std::string& b) { return ucmp->Compare(a, b) < 0; });
    users.erase(std::unique(users.begin(), users.end()), users.end());
    std::vector<std::string> keys;
    bool repeats = rng.below(2) == 0;
    for (auto& u : users) {
      size_t copies = repeats && rng.below(4) == 0 ? 2 + rng.below(3) : 1;
      uint64_t seq = 1000 + rng.below(1000);
      for (size_t c = 0; c < copies; ++c) {
        std::string k = u;
        PutFixed64(&k, PackSequenceAndType(seq - c, kTypeValue));
        keys.push_back(k);
      }
    }
    printf("%d %c %u %d %d %.1f %d.%zu %llu %d", static_cast<int>(type),
           ucmp == BytewiseComparator() ? 'b' : 'r', opts.format_version,
           static_cast<int>(opts.index_shortening), opts.index_block_restart_interval,
           opts.uniform_cv_threshold, prefix_kind, prefix_len,
           static_cast<unsigned long long>(opts.metadata_block_size), opts.block_size_deviation);
    uint64_t offset = rng.below(1000);
    size_t at = 0;
    std::string scratch;
    while (at < keys.size()) {
      size_t take = std::min<size_t>(1 + rng.below(6), keys.size() - at);
      bool skip = rng.below(8) == 0;
      if (skip) offset += 1 + rng.below(100);
      BlockHandle h(offset, 50 + rng.below(4000));
      offset = h.offset() + h.size() + 5;
      printf(" B");
      for (size_t i = at; i < at + take; ++i) {
        b->OnKeyAdded(keys[i], std::optional<Slice>{});
        printf("%s%s", i == at ? "" : ",", hex(keys[i]).c_str());
      }
      Slice next;
      bool last = at + take == keys.size();
      if (!last) next = keys[at + take];
      Slice sep = b->AddIndexEntry(keys[at + take - 1], last ? nullptr : &next, h, &scratch, skip);
      printf("/%llu/%llu/%d=%s/%llu", static_cast<unsigned long long>(h.offset()),
             static_cast<unsigned long long>(h.size()), skip, hex(sep).c_str(),
             static_cast<unsigned long long>(b->CurrentIndexSizeEstimate()));
      if (type == BlockBasedTableOptions::kTwoLevelIndexSearch) {
        printf("/%d", static_cast<PartitionedIndexBuilder*>(b.get())->ShouldCutFilterBlock());
      }
      at += take;
    }
    IndexBuilder::IndexBlocks blocks;
    BlockHandle last_partition;
    Status s;
    do {
      s = b->Finish(&blocks, last_partition);
      last_partition = BlockHandle(offset, blocks.index_block_contents.size());
      offset += blocks.index_block_contents.size() + 5;
      printf(" F%s:%s", s.IsIncomplete() ? "p" : "d", hex(blocks.index_block_contents).c_str());
    } while (s.IsIncomplete());
    std::vector<std::pair<std::string, std::string>> metas;
    for (auto& [name, block] : blocks.meta_blocks) metas.emplace_back(name, block.second.ToString());
    std::sort(metas.begin(), metas.end());
    for (auto& [name, block] : metas) printf(" M%s:%s", name.c_str(), hex(block).c_str());
    printf(" S%zu/%llu/%d\n", b->IndexSize(),
           static_cast<unsigned long long>(b->NumUniformIndexBlocks()),
           b->separator_is_key_plus_seq());
  }
  return 0;
}
