// Golden hash-index seeks for mantle-engine P4: RocksDB 11.8.1's own HashIndexBuilder writes an
// index, BlockPrefixIndex::Create reads its prefix blocks, and an IndexBlockIter seeks through
// them (~/Projects/rocksdb at abeebd963: table/block_based/{index_builder,block_prefix_index,
// block}.cc).
//
// 160 indexes from a SplitMix64-driven script, each with a comparator (bytewise or reverse), a
// format_version (3, 4 or 7; value delta encoding from 4), a shortening mode, and a fixed, capped
// or no-op prefix extractor of 1 to 3 bytes. Keys are internal keys of 3 to 8 bytes of a small
// alphabet, so prefixes repeat and runs of blocks share them, cut into blocks of 1 to 6 keys.
// Each index then takes 48 seeks, each followed by up to two steps forward: to a key of the table,
// its user key at another sequence number, a key of a prefix in the table, or a key whose prefix
// may be absent. Every target is in the extractor's domain (RocksDB's Transform is undefined
// outside it).
//
// Output, one line per index:
//   comparator format_version prefix value_is_full key_includes_seq index_hex prefixes_hex
//   metadata_hex move...
// where a move is S:target_hex=result or N=result, and a result is I (not valid), A (the prefix
// is absent: status NotFound), E (another status) or V followed by key_hex/offset,size.
//
// Built outside the repository against an unmodified checkout (R=~/Projects/rocksdb) and the
// static library of ~/Projects/rocksdb-golden/lib (L), on aarch64 macOS:
//   clang++ -std=c++20 -O2 -DNDEBUG -march=armv8-a+crc+crypto -I$R -I$R/include \
//     -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_MACOSX p4_prefix_gen.cc \
//     $L/librocksdb.a -o p4_prefix_gen
//   ./p4_prefix_gen > p4_prefix.txt
// crates/engine/tests/block_prefix_golden.rs replays every seek on the port.
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
#include "table/block_based/block.h"
#include "table/block_based/block_prefix_index.h"
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
    for (auto& c : s) c = static_cast<char>("abcd"[next() % 4]);
    return s;
  }
};

std::string hex(const Slice& s) { return s.ToString(true); }

std::string Internal(const std::string& user, uint64_t seq) {
  std::string k = user;
  PutFixed64(&k, PackSequenceAndType(seq, kTypeValue));
  return k;
}

std::string Result(IndexBlockIter* it) {
  if (it->status().IsNotFound()) return "A";
  if (!it->status().ok()) return "E";
  if (!it->Valid()) return "I";
  IndexValue v = it->value();
  return "V" + hex(it->key()) + "/" + std::to_string(v.handle.offset()) + "," +
         std::to_string(v.handle.size());
}

const uint32_t kVersions[] = {3, 4, 7};
}  // namespace

int main() {
  SplitMix64 rng{17};
  for (int n = 0; n < 160; ++n) {
    const Comparator* ucmp =
        rng.below(4) == 0 ? ReverseBytewiseComparator() : BytewiseComparator();
    InternalKeyComparator icmp(ucmp);
    BlockBasedTableOptions opts;
    opts.index_type = BlockBasedTableOptions::kHashSearch;
    opts.format_version = kVersions[rng.below(3)];
    opts.index_shortening =
        static_cast<BlockBasedTableOptions::IndexShorteningMode>(rng.below(3));
    int prefix_kind = static_cast<int>(rng.below(3));
    size_t prefix_len = 1 + rng.below(3);
    std::unique_ptr<const SliceTransform> prefix(
        prefix_kind == 0   ? NewFixedPrefixTransform(prefix_len)
        : prefix_kind == 1 ? NewCappedPrefixTransform(prefix_len)
                           : NewNoopTransform());
    InternalKeySliceTransform ikst(prefix.get());
    bool value_delta = opts.format_version >= 4;
    std::unique_ptr<IndexBuilder> b(IndexBuilder::CreateIndexBuilder(
        opts.index_type, &icmp, &ikst, value_delta, opts, 0, true));

    std::vector<std::string> users;
    size_t count = 1 + rng.below(60);
    for (size_t i = 0; i < count; ++i) users.push_back(rng.bytes(3 + rng.below(6)));
    std::sort(users.begin(), users.end(),
              [&](const std::string& a, const std::string& b) { return ucmp->Compare(a, b) < 0; });
    users.erase(std::unique(users.begin(), users.end()), users.end());
    std::vector<std::string> keys;
    for (auto& u : users) keys.push_back(Internal(u, 1000 + rng.below(1000)));

    uint64_t offset = 0;
    size_t at = 0;
    std::string scratch;
    while (at < keys.size()) {
      size_t take = std::min<size_t>(1 + rng.below(6), keys.size() - at);
      BlockHandle h(offset, 50 + rng.below(4000));
      offset = h.offset() + h.size() + 5;
      for (size_t i = at; i < at + take; ++i) b->OnKeyAdded(keys[i], std::optional<Slice>{});
      Slice next;
      bool last = at + take == keys.size();
      if (!last) next = keys[at + take];
      b->AddIndexEntry(keys[at + take - 1], last ? nullptr : &next, h, &scratch, false);
      at += take;
    }
    IndexBuilder::IndexBlocks blocks;
    if (!b->Finish(&blocks).ok()) return 1;
    std::string index = blocks.index_block_contents.ToString();
    std::string prefixes = blocks.meta_blocks[kHashIndexPrefixesBlock].second.ToString();
    std::string metadata = blocks.meta_blocks[kHashIndexPrefixesMetadataBlock].second.ToString();
    bool key_includes_seq = b->separator_is_key_plus_seq();

    BlockPrefixIndex* raw = nullptr;
    if (!BlockPrefixIndex::Create(prefix.get(), prefixes, metadata, &raw).ok()) return 1;
    std::unique_ptr<BlockPrefixIndex> pi(raw);
    Block block(BlockContents(Slice(index)), 0, nullptr, 1);
    std::unique_ptr<IndexBlockIter> it(block.NewIndexIterator(
        ucmp, kDisableGlobalSequenceNumber, nullptr, nullptr, /*total_order_seek=*/false,
        /*have_first_key=*/false, key_includes_seq, /*value_is_full=*/!value_delta,
        /*block_contents_pinned=*/false, true, pi.get(), BlockBasedTableOptions::kBinary));

    printf("%c %u %d.%zu %d %d %s %s %s", ucmp == BytewiseComparator() ? 'b' : 'r',
           opts.format_version, prefix_kind, prefix_len, !value_delta, key_includes_seq,
           hex(index).c_str(), hex(prefixes).c_str(), hex(metadata).c_str());
    for (int m = 0; m < 48; ++m) {
      std::string user;
      switch (rng.below(4)) {
        case 0:
          user = users[rng.below(users.size())];
          break;
        case 1: {
          const std::string& u = users[rng.below(users.size())];
          user = u.substr(0, std::min(u.size(), prefix_len)) + rng.bytes(rng.below(5));
          break;
        }
        default:
          user = rng.bytes(3 + rng.below(6));
      }
      std::string target = Internal(user, rng.below(3) == 0 ? kMaxSequenceNumber : rng.below(3000));
      it->Seek(target);
      printf(" S:%s=%s", hex(target).c_str(), Result(it.get()).c_str());
      for (uint64_t k = rng.below(3); k > 0 && it->Valid(); --k) {
        it->Next();
        printf(" N=%s", Result(it.get()).c_str());
      }
    }
    printf("\n");
  }
  return 0;
}
