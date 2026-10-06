// Golden table footers for mantle-engine P4, built and decoded by RocksDB 11.8.1's own
// FooterBuilder and Footer (~/Projects/rocksdb at abeebd963: table/format.{h,cc}).
//
// 512 footers from a SplitMix64-driven script: every block-based format_version from 2 to 7 and
// every checksum type, a data size from one byte to 2^40, an index and a metaindex of random
// sizes laid out as a table lays them (data, index, metaindex, each with its 5-byte trailer, then
// the footer), and from format_version 6 a nonzero base context checksum. Each footer is then
// decoded at its offset, and again with one bit flipped and at an offset one off.
//
// Output, one line per footer: format_version, checksum type, footer offset, metaindex offset and
// size, index offset and size, base context checksum, the footer's bytes in hex, then three
// decodes "status" for the footer as written, with bit B flipped ("B:status") and at the offset
// minus one: O ok, C corruption, N not supported, X other.
//
// Built outside the repository against an unmodified checkout (R=~/Projects/rocksdb) and the
// static library of ~/Projects/rocksdb-golden/lib (L), on aarch64 macOS:
//   clang++ -std=c++20 -O2 -DNDEBUG -march=armv8-a+crc+crypto -I$R -I$R/include \
//     -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_MACOSX p4_footer_gen.cc \
//     $L/librocksdb.a -o p4_footer_gen
//   ./p4_footer_gen > p4_footer.txt
// crates/engine/tests/table_format_golden.rs builds the same footers with the port and compares
// bytes and every decode.
#include <cstdint>
#include <cstdio>
#include <string>

#include "rocksdb/table.h"
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
};

char status(const Status& s) {
  if (s.ok()) return 'O';
  if (s.IsCorruption()) return 'C';
  if (s.IsNotSupported()) return 'N';
  return 'X';
}

const ChecksumType kChecksums[] = {kNoChecksum, kCRC32c, kxxHash, kxxHash64, kXXH3};
}  // namespace

int main() {
  SplitMix64 rng{4};
  for (int n = 0; n < 512; ++n) {
    uint32_t fv = 2 + static_cast<uint32_t>(n % 6);
    ChecksumType t = kChecksums[(n / 6) % 5];
    uint64_t data_size = (uint64_t{1} << rng.below(41)) + rng.below(100);
    uint64_t index_size = rng.below(1000000000);
    uint64_t metaindex_size = rng.below(1000000);
    BlockHandle index(data_size + 5, index_size);
    BlockHandle meta(data_size + index_size + 2 * 5, metaindex_size);
    uint64_t footer_offset = data_size + index_size + metaindex_size + 3 * 5;
    uint32_t bcc = 0;
    if (FormatVersionUsesContextChecksum(fv)) {
      do {
        bcc = static_cast<uint32_t>(rng.next());
      } while (ChecksumModifierForContext(bcc, 0) == 0);
    }
    FooterBuilder b;
    Status s = b.Build(kBlockBasedTableMagicNumber, fv, footer_offset, t, meta,
                       index, bcc);
    if (!s.ok()) {
      fprintf(stderr, "build %d: %s\n", n, s.ToString().c_str());
      return 1;
    }
    std::string bytes = b.GetSlice().ToString();
    Footer as_written;
    char ok = status(as_written.DecodeFrom(Slice(bytes), footer_offset));
    uint64_t bit = rng.below(bytes.size() * 8);
    std::string flipped = bytes;
    flipped[bit / 8] ^= static_cast<char>(1 << (bit % 8));
    Footer f2;
    char flip = status(f2.DecodeFrom(Slice(flipped), footer_offset));
    Footer f3;
    char off = status(f3.DecodeFrom(Slice(bytes), footer_offset - 1));
    printf("%u %u %llu %llu %llu %llu %llu %u %s %c %llu:%c %c\n", fv,
           static_cast<unsigned>(t),
           static_cast<unsigned long long>(footer_offset),
           static_cast<unsigned long long>(meta.offset()),
           static_cast<unsigned long long>(meta.size()),
           static_cast<unsigned long long>(index.offset()),
           static_cast<unsigned long long>(index.size()), bcc,
           Slice(bytes).ToString(true).c_str(), ok,
           static_cast<unsigned long long>(bit), flip, off);
  }
  return 0;
}
