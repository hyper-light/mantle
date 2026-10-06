// Golden block reads for mantle-engine P4: RocksDB 11.8.1's own Block, DataBlockIter,
// IndexBlockIter and MetaBlockIter (~/Projects/rocksdb at abeebd963: table/block_based/block.cc)
// replaying scripted moves over blocks its BlockBuilder wrote.
//
// 192 blocks from a SplitMix64-driven script, a third each of data, index and meta blocks.
//   data:  internal keys under the bytewise or the reverse bytewise comparator; restart interval
//          1, 2, 3, 16 or 32; delta encoding on or off; binary search or a hash index;
//          separated keys and values or not; per-entry checksums of 0, 1, 2, 4 or 8 bytes; and
//          a global sequence number or none, keys then carrying sequence 0.
//   index: handles of consecutive blocks, with or without each block's first internal key,
//          values in full or delta encoded, keys internal or user keys, read by binary search,
//          interpolation (bytewise only) or the uniform-flag choice; separated or not; a global
//          sequence number where first keys are carried; per-entry checksums as for data.
//   meta:  user keys, restart interval 1, bytewise, per-entry checksums as for data.
// Keys are spread evenly (a fixed prefix and a big-endian counter) or at random.
//
// Each block then takes 64 moves: SeekToFirst, SeekToLast, Next and Prev (only while valid),
// Seek, SeekForPrev (not on index blocks, where RocksDB forbids it) and SeekForGet (data blocks
// without separated keys and values, where RocksDB's end-of-block test is wrong: see
// docs/design/engine.md §9). Seek targets are a key of the block, its user key with another
// trailer, a prefix of it, or random bytes.
//
// Output, one line per block:
//   kind comparator restart_interval protection global_seqno search have_first_key value_is_full
//   key_includes_seq block_hex kv_checksum_hex(or -) move...
// kind is d, i or m; comparator b or r; global_seqno - for none; search b, i or a. Each move is
// op:target_hex=result, where op is F (first), L (last), N, P, S (seek), R (seek for prev) or
// G (seek for get), target_hex is - for moves without one, and result is I (not valid, status
// OK), E (status not OK) or V followed by key_hex/value_hex; an index value is written as
// offset,size,first_key_hex. A seek for get's result starts with its return, T or F.
//
// Built outside the repository against an unmodified checkout (R=~/Projects/rocksdb) and the
// static library of ~/Projects/rocksdb-golden/lib (L), on aarch64 macOS:
//   clang++ -std=c++20 -O2 -DNDEBUG -march=armv8-a+crc+crypto -I$R -I$R/include \
//     -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_MACOSX p4_iter_gen.cc \
//     $L/librocksdb.a -o p4_iter_gen
//   ./p4_iter_gen > p4_iter.txt
// crates/engine/tests/block_iter_golden.rs replays every move on the port's reader.
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "rocksdb/table.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_builder.h"
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
    for (auto& c : s) c = static_cast<char>(next());
    return s;
  }
};

std::string hex(const Slice& s) { return s.ToString(true); }

std::string be64(uint64_t v) {
  std::string s(8, '\0');
  for (int i = 0; i < 8; ++i) s[i] = static_cast<char>(v >> (56 - 8 * i));
  return s;
}

std::string internal(const std::string& user, SequenceNumber seq, ValueType t) {
  std::string k = user;
  PutFixed64(&k, PackSequenceAndType(seq, t));
  return k;
}

const uint8_t kWidths[] = {0, 1, 2, 4, 8};
const int kIntervals[] = {1, 2, 3, 16, 32};
const ValueType kTypes[] = {kTypeValue, kTypeMerge, kTypeDeletion};

// Sorted, distinct user keys under `cmp`.
std::vector<std::string> UserKeys(SplitMix64& rng, size_t count, const Comparator* cmp) {
  bool even = rng.below(2) == 0;
  std::string prefix = rng.bytes(rng.below(8));
  uint64_t start = rng.next() >> 8, step = 1 + rng.below(1000);
  std::vector<std::string> keys;
  for (size_t i = 0; i < count; ++i) {
    keys.push_back(even ? prefix + be64(start + i * step)
                        : prefix + rng.bytes(1 + rng.below(12)));
  }
  std::sort(keys.begin(), keys.end(),
            [&](const std::string& a, const std::string& b) { return cmp->Compare(a, b) < 0; });
  keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
  return keys;
}

// A seek target near the block's keys: `internal_target` gives it an internal key's trailer.
std::string Target(SplitMix64& rng, const std::vector<std::string>& users, bool internal_target) {
  std::string user;
  switch (users.empty() ? 3 : rng.below(4)) {
    case 0:
    case 1:
      user = users[rng.below(users.size())];
      break;
    case 2: {
      const std::string& k = users[rng.below(users.size())];
      user = k.substr(0, rng.below(k.size() + 1));
      break;
    }
    default:
      user = rng.bytes(rng.below(14));
  }
  if (!internal_target) return user;
  return internal(user, rng.below(4) == 0 ? 0 : rng.next() >> 8,
                  rng.below(4) == 0 ? kTypeMerge : kTypeValue);
}

template <class It>
std::string Result(It* it, const std::string& (*value)(It*, std::string*)) {
  if (!it->status().ok()) return "E";
  if (!it->Valid()) return "I";
  std::string v;
  return "V" + hex(it->key()) + "/" + value(it, &v);
}

const std::string& PlainValue(DataBlockIter* it, std::string* out) {
  *out = hex(it->value());
  return *out;
}
const std::string& MetaValue(MetaBlockIter* it, std::string* out) {
  *out = hex(it->value());
  return *out;
}
const std::string& IndexEntry(IndexBlockIter* it, std::string* out) {
  IndexValue v = it->value();
  *out = std::to_string(v.handle.offset()) + "," + std::to_string(v.handle.size()) + "," +
         hex(v.first_internal_key);
  return *out;
}

template <class It>
void Replay(SplitMix64& rng, It* it, const std::vector<std::string>& users, bool internal_target,
            bool seek_for_prev, bool seek_for_get,
            const std::string& (*value)(It*, std::string*)) {
  for (int m = 0; m < 64; ++m) {
    int op = static_cast<int>(rng.below(7));
    if ((op == 2 || op == 3) && !it->Valid()) op = static_cast<int>(rng.below(2));
    if (op == 5 && !seek_for_prev) op = 4;
    if (op == 6 && !seek_for_get) op = 4;
    std::string target = op >= 4 ? Target(rng, users, internal_target) : "";
    std::string prefix;
    switch (op) {
      case 0: it->SeekToFirst(); printf(" F:-="); break;
      case 1: it->SeekToLast(); printf(" L:-="); break;
      case 2: it->Next(); printf(" N:-="); break;
      case 3: it->Prev(); printf(" P:-="); break;
      case 4: it->Seek(target); printf(" S:%s=", hex(target).c_str()); break;
      case 5: it->SeekForPrev(target); printf(" R:%s=", hex(target).c_str()); break;
      default: {
        if constexpr (std::is_same_v<It, DataBlockIter>) {
          prefix = it->SeekForGet(target) ? "T" : "F";
        }
        printf(" G:%s=", hex(target).c_str());
      }
    }
    printf("%s%s", prefix.c_str(), Result(it, value).c_str());
  }
}

std::string Checksums(const Block& block, size_t keys, uint8_t width) {
  if (width == 0 || block.TEST_GetKVChecksum() == nullptr) return "-";
  return hex(Slice(block.TEST_GetKVChecksum(), keys * width));
}
}  // namespace

int main() {
  SplitMix64 rng{11};
  for (int n = 0; n < 192; ++n) {
    char kind = "dim"[n % 3];
    const Comparator* cmp =
        kind != 'm' && rng.below(4) == 0 ? ReverseBytewiseComparator() : BytewiseComparator();
    int interval = kind == 'm' ? 1 : kIntervals[rng.below(5)];
    bool delta = kind != 'd' || rng.below(4) != 0;
    bool separated = kind != 'm' && rng.below(3) == 0;
    uint8_t width = kWidths[rng.below(5)];
    bool hash = kind == 'd' && rng.below(2) == 0;
    bool value_is_full = kind != 'i' || rng.below(2) == 0;
    bool have_first_key = kind == 'i' && rng.below(2) == 0;
    bool key_includes_seq = kind != 'i' || rng.below(3) != 0;
    char search = "bia"[rng.below(3)];
    // Interpolation reads keys as integers, so bytewise order only.
    if (kind != 'i' || (search == 'i' && cmp != BytewiseComparator())) search = 'b';
    bool with_seqno = (kind == 'd' && rng.below(4) == 0) || (have_first_key && rng.below(3) == 0);
    SequenceNumber global_seqno = with_seqno ? 1 + rng.below(1u << 20) : kDisableGlobalSequenceNumber;
    bool user_keys = kind == 'm' || !key_includes_seq;

    BlockBuilder b(interval, delta, !value_is_full,
                   hash ? BlockBasedTableOptions::kDataBlockBinaryAndHash
                        : BlockBasedTableOptions::kDataBlockBinarySearch,
                   0.75, 0, true, user_keys, separated, nullptr, 0.5);
    std::vector<std::string> users = UserKeys(rng, rng.below(80), cmp);
    uint64_t offset = rng.below(1u << 20);
    BlockHandle last_handle;
    std::vector<std::string> firsts;
    for (size_t i = 0; i < users.size(); ++i) {
      std::string key = user_keys ? users[i]
                                  : internal(users[i], kind == 'd' && with_seqno ? 0 : rng.next() >> 8,
                                             kTypes[rng.below(3)]);
      if (kind == 'i') {
        BlockHandle h(offset, rng.below(5000));
        offset = h.offset() + h.size() + BlockBasedTable::kBlockTrailerSize;
        firsts.push_back(internal(rng.bytes(rng.below(10)), with_seqno ? 0 : rng.next() >> 8,
                                  kTypes[rng.below(3)]));
        IndexValue v(h, firsts.back());
        std::string full, dv;
        v.EncodeTo(&full, have_first_key, nullptr);
        if (i > 0) v.EncodeTo(&dv, have_first_key, &last_handle);
        Slice dvs(dv);
        b.Add(key, full, value_is_full ? nullptr : &dvs);
        last_handle = h;
      } else {
        b.Add(key, rng.bytes(rng.below(40)));
      }
    }
    Slice raw = b.Finish();
    std::string bytes = raw.ToString();
    Block block(BlockContents(Slice(bytes)), 0, nullptr, static_cast<uint32_t>(interval));
    switch (kind) {
      case 'd': block.InitializeDataBlockProtectionInfo(width, cmp); break;
      case 'i':
        block.InitializeIndexBlockProtectionInfo(width, cmp, value_is_full, have_first_key);
        break;
      default: block.InitializeMetaIndexBlockProtectionInfo(width);
    }
    printf("%c %c %d %u %s %c %d %d %d %s %s", kind, cmp == BytewiseComparator() ? 'b' : 'r',
           interval, width,
           with_seqno ? std::to_string(global_seqno).c_str() : "-", search, have_first_key,
           value_is_full, key_includes_seq, hex(raw).c_str(),
           Checksums(block, users.size(), width).c_str());
    if (kind == 'd') {
      std::unique_ptr<DataBlockIter> it(block.NewDataIterator(cmp, global_seqno));
      Replay<DataBlockIter>(rng, it.get(), users, true, true, !separated, PlainValue);
    } else if (kind == 'i') {
      auto type = search == 'b' ? BlockBasedTableOptions::kBinary
                  : search == 'i' ? BlockBasedTableOptions::kInterpolation
                                  : BlockBasedTableOptions::kAuto;
      std::unique_ptr<IndexBlockIter> it(block.NewIndexIterator(
          cmp, global_seqno, nullptr, nullptr, true, have_first_key, key_includes_seq,
          value_is_full, false, true, nullptr, type));
      Replay<IndexBlockIter>(rng, it.get(), users, true, false, false, IndexEntry);
    } else {
      std::unique_ptr<MetaBlockIter> it(block.NewMetaIterator());
      Replay<MetaBlockIter>(rng, it.get(), users, false, true, false, MetaValue);
    }
    printf("\n");
  }
  return 0;
}
