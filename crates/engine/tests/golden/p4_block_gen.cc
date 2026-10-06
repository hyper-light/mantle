// Golden blocks for mantle-engine P4, built by RocksDB 11.8.1's own BlockBuilder
// (~/Projects/rocksdb at abeebd963: table/block_based/block_builder.cc, data_block_hash_index.cc,
// data_block_footer.cc).
//
// 256 blocks from a SplitMix64-driven script. Each block draws a configuration: a restart
// interval of 1, 2, 3, 16 or 32; delta encoding on or off; value delta encoding (as index blocks
// of format_version 4 use, with a delta value given for every entry) on or off; a binary search
// or a binary-and-hash index at a utilisation ratio of 0.75, 0.5, 1.0 or 0.3; separated key and
// value storage on or off; a uniformity threshold of none, 0.1, 0.5 or 2.0; and keys that are
// internal keys (the hash index needs them) or user keys. It then adds 0 to 60 entries in
// ascending order: keys either spread evenly (a fixed prefix and a big-endian counter stepped by a
// constant) or randomly, with shared prefixes of random length, and random values of 0 to 48
// bytes. Every 32nd block instead has a restart interval of one, a hash index and 260 entries of
// one-byte values, past the 253 restart intervals a hash index holds. Some blocks are built with
// AddWithLastKey instead of Add.
//
// Output, one line per block: the configuration
//   restart_interval delta value_delta hash ratio separated threshold user_keys with_last_key
// then the number of entries, each entry as key_hex:value_hex:delta_hex, then the
// EstimateSizeAfterKV of a probe entry before Finish, CurrentSizeEstimate before Finish,
// IsUniform after Finish, and the finished block in hex.
//
// Built outside the repository against an unmodified checkout (R=~/Projects/rocksdb) and the
// static library of ~/Projects/rocksdb-golden/lib (L), on aarch64 macOS:
//   clang++ -std=c++20 -O2 -DNDEBUG -march=armv8-a+crc+crypto -I$R -I$R/include \
//     -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_MACOSX p4_block_gen.cc \
//     $L/librocksdb.a -o p4_block_gen
//   ./p4_block_gen > p4_block.txt
// crates/engine/tests/block_builder_golden.rs builds the same blocks with the port and compares
// every field.
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

#include "rocksdb/table.h"
#include "table/block_based/block_builder.h"

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

std::string hex(const std::string& s) { return Slice(s).ToString(true); }

std::string be64(uint64_t v) {
  std::string s(8, '\0');
  for (int i = 0; i < 8; ++i) s[i] = static_cast<char>(v >> (56 - 8 * i));
  return s;
}
}  // namespace

int main() {
  SplitMix64 rng{7};
  const int kIntervals[] = {1, 2, 3, 16, 32};
  const double kRatios[] = {0.75, 0.5, 1.0, 0.3};
  const double kThresholds[] = {-1.0, 0.1, 0.5, 2.0};
  for (int n = 0; n < 256; ++n) {
    bool many = n % 32 == 0;
    int interval = many ? 1 : kIntervals[rng.below(5)];
    bool delta = rng.below(4) != 0;
    bool value_delta = !many && rng.below(4) == 0;
    bool user_keys = !many && rng.below(4) == 0;
    bool hash = many || (!user_keys && !value_delta && rng.below(2) == 0);
    double ratio = kRatios[rng.below(4)];
    bool separated = rng.below(3) == 0;
    double threshold = kThresholds[rng.below(4)];
    bool with_last_key = rng.below(3) == 0;
    BlockBuilder b(interval, delta, value_delta,
                   hash ? BlockBasedTableOptions::kDataBlockBinaryAndHash
                        : BlockBasedTableOptions::kDataBlockBinarySearch,
                   ratio, 0, true, user_keys, separated, nullptr, threshold);
    size_t count = many ? 260 : rng.below(61);
    bool even = rng.below(2) == 0;
    std::string prefix = rng.bytes(rng.below(12));
    uint64_t start = rng.next() >> 8, step = 1 + rng.below(1000);
    std::vector<std::string> keys;
    for (size_t i = 0; i < count; ++i) {
      std::string user;
      if (even) {
        user = prefix + be64(start + i * step);
      } else {
        user = prefix + rng.bytes(1 + rng.below(16));
      }
      keys.push_back(user);
    }
    std::sort(keys.begin(), keys.end());
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
    printf("%d %d %d %d %.2f %d %.1f %d %d %zu", interval, delta, value_delta, hash,
           ratio, separated, threshold, user_keys, with_last_key, keys.size());
    std::string last;
    for (size_t i = 0; i < keys.size(); ++i) {
      std::string key = keys[i];
      if (!user_keys) {
        // An internal key: the user key and its 8-byte sequence and type, descending sequence
        // so internal keys ascend with user keys.
        key += be64(rng.next());
      }
      std::string value = rng.bytes(many ? 1 : rng.below(49));
      std::string dv = rng.bytes(rng.below(12));
      Slice dvs(dv);
      if (with_last_key) {
        b.AddWithLastKey(key, value, last, value_delta ? &dvs : nullptr);
      } else {
        b.Add(key, value, value_delta ? &dvs : nullptr);
      }
      last = key;
      printf(" %s:%s:%s", hex(key).c_str(), hex(value).c_str(), hex(dv).c_str());
    }
    std::string probe_key = rng.bytes(rng.below(40)), probe_value = rng.bytes(rng.below(48));
    size_t after = b.EstimateSizeAfterKV(probe_key, probe_value);
    size_t current = b.CurrentSizeEstimate();
    Slice block = b.Finish();
    printf(" %s:%s %zu %zu %d %s\n", hex(probe_key).c_str(), hex(probe_value).c_str(), after,
           current, b.IsUniform(), block.ToString(true).c_str());
  }
  return 0;
}
