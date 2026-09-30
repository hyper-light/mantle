// Golden vectors for mantle-engine P1, computed by RocksDB 11.8.1's own code
// (~/Projects/rocksdb at abeebd963), compiled from its sources:
//   util/hash.cc util/crc32c.cc util/crc32c_arm64.cc util/xxhash.cc util/coding.cc
// plus header-only util/coding.h, util/prefix_varint.h, util/fastrange.h, util/math.h,
// table/format.h (ChecksumModifierForContext), util/file_checksum_helper.h.
// ComputeBuiltinChecksum{,WithLastByte} live in table/format.cc, which links most of RocksDB;
// their bodies are copied verbatim below from table/format.cc:602-682.
//
// Output: one line per record, "name count d0 d1 ...", where each d is the FNV-1a 64 digest (hex)
// of the outputs of a window of 32 consecutive items (lengths or random draws).
//
// Built outside the repository against an unmodified checkout (R=~/Projects/rocksdb), on
// aarch64 macOS:
//   clang++ -std=c++20 -O2 -march=armv8-a+crc+crypto -I$R -I$R/include \
//     -DROCKSDB_PLATFORM_POSIX -DOS_MACOSX p1_gen.cc $R/util/hash.cc $R/util/crc32c.cc \
//     $R/util/crc32c_arm64.cc $R/util/xxhash.cc $R/util/coding.cc -o p1_gen
//   ./p1_gen > p1.txt
// crates/engine/tests/golden.rs recomputes every record with the port and compares.
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/fastrange.h"
#include "util/file_checksum_helper.h"
#include "util/hash.h"
#include "util/hash128.h"
#include "util/math.h"
#include "util/prefix_varint.h"
#include "util/xxhash.h"

using namespace ROCKSDB_NAMESPACE;

// ---- copied from table/format.cc:602-682 ----
namespace {
inline uint32_t ModifyChecksumForLastByte(uint32_t checksum, char last_byte) {
  const uint32_t kRandomPrime = 0x6b9083d9;
  return checksum ^ lossless_cast<uint8_t>(last_byte) * kRandomPrime;
}
}  // namespace
uint32_t GoldenComputeBuiltinChecksum(ChecksumType type, const char* data,
                                      size_t data_size) {
  switch (type) {
    case kCRC32c:
      return crc32c::Mask(crc32c::Value(data, data_size));
    case kxxHash:
      return XXH32(data, data_size, /*seed*/ 0);
    case kxxHash64:
      return Lower32of64(XXH64(data, data_size, /*seed*/ 0));
    case kXXH3: {
      if (data_size == 0) {
        return 0;
      } else {
        uint32_t v = Lower32of64(XXH3_64bits(data, data_size - 1));
        return ModifyChecksumForLastByte(v, data[data_size - 1]);
      }
    }
    default:
      return 0;
  }
}
uint32_t GoldenComputeBuiltinChecksumWithLastByte(ChecksumType type,
                                                  const char* data,
                                                  size_t data_size,
                                                  char last_byte) {
  switch (type) {
    case kCRC32c: {
      uint32_t crc = crc32c::Value(data, data_size);
      crc = crc32c::Extend(crc, &last_byte, 1);
      return crc32c::Mask(crc);
    }
    case kxxHash: {
      XXH32_state_t* const state = XXH32_createState();
      XXH32_reset(state, 0);
      XXH32_update(state, data, data_size);
      XXH32_update(state, &last_byte, 1);
      uint32_t v = XXH32_digest(state);
      XXH32_freeState(state);
      return v;
    }
    case kxxHash64: {
      XXH64_state_t* const state = XXH64_createState();
      XXH64_reset(state, 0);
      XXH64_update(state, data, data_size);
      XXH64_update(state, &last_byte, 1);
      uint32_t v = Lower32of64(XXH64_digest(state));
      XXH64_freeState(state);
      return v;
    }
    case kXXH3: {
      uint32_t v = Lower32of64(XXH3_64bits(data, data_size));
      return ModifyChecksumForLastByte(v, last_byte);
    }
    default:
      return 0;
  }
}
// ---- end copy ----

struct SplitMix64 {
  uint64_t s;
  uint64_t next() {
    s += 0x9e3779b97f4a7c15ULL;
    uint64_t z = s;
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
  }
};

constexpr size_t kMaxLen = 4096;
constexpr size_t kDraws = 4096;
constexpr size_t kWindow = 32;

struct Record {
  std::string name;
  size_t count = 0;
  uint64_t d = 0xcbf29ce484222325ULL;
  std::vector<uint64_t> digests;
  explicit Record(std::string n) : name(std::move(n)) {}
  void bytes(const void* p, size_t n) {
    const unsigned char* b = static_cast<const unsigned char*>(p);
    for (size_t i = 0; i < n; ++i) {
      d ^= b[i];
      d *= 0x100000001b3ULL;
    }
  }
  void u8(uint8_t v) { bytes(&v, 1); }
  void u32(uint32_t v) {
    char b[4];
    EncodeFixed32(b, v);
    bytes(b, 4);
  }
  void u64(uint64_t v) {
    char b[8];
    EncodeFixed64(b, v);
    bytes(b, 8);
  }
  void end_item() {
    ++count;
    if (count % kWindow == 0) flush();
  }
  void flush() {
    digests.push_back(d);
    d = 0xcbf29ce484222325ULL;
  }
  void finish() {
    if (count % kWindow != 0) flush();
    printf("%s %zu", name.c_str(), count);
    for (uint64_t x : digests) printf(" %016llx", (unsigned long long)x);
    printf("\n");
  }
};

int main() {
  // The input: SplitMix64 from "ROCKSDB1", 8 bytes little-endian per draw.
  std::vector<char> buf(kMaxLen + 64);
  {
    SplitMix64 r{0x524F434B53444231ULL};
    for (size_t i = 0; i < buf.size(); i += 8) EncodeFixed64(&buf[i], r.next());
  }
  std::vector<char> high(buf);
  for (auto& c : high) c = static_cast<char>(static_cast<unsigned char>(c) | 0x80);

  // ---- by length, 0..=kMaxLen ----
  const uint32_t h32_seeds[] = {0, 0xbc9f1d34u, 397, 0xdeadbeefu, 0x80000000u};
  for (uint32_t seed : h32_seeds) {
    char name[64];
    snprintf(name, sizeof name, "hash32_%08x", seed);
    Record rec(name);
    for (size_t n = 0; n <= kMaxLen; ++n) {
      rec.u32(Hash(buf.data(), n, seed));
      rec.end_item();
    }
    rec.finish();
  }
  {
    Record rec("hash32_high_bloom");
    for (size_t n = 0; n <= kMaxLen; ++n) {
      rec.u32(BloomHash(Slice(high.data(), n)));
      rec.end_item();
    }
    rec.finish();
  }
  {
    Record rec("hash64_unseeded");
    for (size_t n = 0; n <= kMaxLen; ++n) {
      rec.u64(Hash64(buf.data(), n));
      rec.end_item();
    }
    rec.finish();
  }
  const uint64_t h64_seeds[] = {1, 0x9e3779b97f4a7c15ULL, 0xffffffffffffffffULL,
                                0x0123456789abcdefULL};
  for (uint64_t seed : h64_seeds) {
    char name[64];
    snprintf(name, sizeof name, "hash64_%016llx", (unsigned long long)seed);
    Record rec(name);
    for (size_t n = 0; n <= kMaxLen; ++n) {
      rec.u64(Hash64(buf.data(), n, seed));
      rec.end_item();
    }
    rec.finish();
  }
  {
    Record rec("hash64_high_unseeded");
    for (size_t n = 0; n <= kMaxLen; ++n) {
      rec.u64(Hash64(high.data(), n));
      rec.end_item();
    }
    rec.finish();
  }
  {
    Record rec("hash2x64_unseeded");
    for (size_t n = 0; n <= kMaxLen; ++n) {
      uint64_t hi, lo;
      Hash2x64(buf.data(), n, &hi, &lo);
      rec.u64(hi);
      rec.u64(lo);
      rec.end_item();
    }
    rec.finish();
  }
  const uint64_t h128_seeds[] = {1, 0xfedcba9876543210ULL};
  for (uint64_t seed : h128_seeds) {
    char name[64];
    snprintf(name, sizeof name, "hash2x64_%016llx", (unsigned long long)seed);
    Record rec(name);
    for (size_t n = 0; n <= kMaxLen; ++n) {
      uint64_t hi, lo;
      Hash2x64(buf.data(), n, seed, &hi, &lo);
      rec.u64(hi);
      rec.u64(lo);
      rec.end_item();
    }
    rec.finish();
  }
  const uint32_t x32_seeds[] = {0, 0x9747b28cu};
  for (uint32_t seed : x32_seeds) {
    char name[64];
    snprintf(name, sizeof name, "xxh32_%08x", seed);
    Record rec(name);
    for (size_t n = 0; n <= kMaxLen; ++n) {
      rec.u32(XXH32(buf.data(), n, seed));
      rec.end_item();
    }
    rec.finish();
  }
  const uint64_t x64_seeds[] = {0, 0x9747b28c9747b28cULL};
  for (uint64_t seed : x64_seeds) {
    char name[64];
    snprintf(name, sizeof name, "xxh64_%016llx", (unsigned long long)seed);
    Record rec(name);
    for (size_t n = 0; n <= kMaxLen; ++n) {
      rec.u64(XXH64(buf.data(), n, seed));
      rec.end_item();
    }
    rec.finish();
  }
  {
    Record rec("xxh3_64_unseeded");
    for (size_t n = 0; n <= kMaxLen; ++n) {
      rec.u64(XXH3_64bits(buf.data(), n));
      rec.end_item();
    }
    rec.finish();
  }
  {
    Record rec("xxh3_64_0123456789abcdef");
    for (size_t n = 0; n <= kMaxLen; ++n) {
      rec.u64(XXH3_64bits_withSeed(buf.data(), n, 0x0123456789abcdefULL));
      rec.end_item();
    }
    rec.finish();
  }
  {
    Record value("crc32c_value"), mask("crc32c_mask"), ext("crc32c_extend"),
        comb("crc32c_combine");
    for (size_t n = 0; n <= kMaxLen; ++n) {
      uint32_t c = crc32c::Value(buf.data(), n);
      value.u32(c);
      value.end_item();
      mask.u32(crc32c::Mask(c));
      mask.end_item();
      size_t a = n / 3;
      uint32_t ca = crc32c::Value(buf.data(), a);
      ext.u32(crc32c::Extend(ca, buf.data() + a, n - a));
      ext.end_item();
      comb.u32(crc32c::Crc32cCombine(ca, crc32c::Value(buf.data() + a, n - a), n - a));
      comb.end_item();
    }
    value.finish();
    mask.finish();
    ext.finish();
    comb.finish();
  }
  for (int t = 0; t <= 4; ++t) {
    char name[64];
    snprintf(name, sizeof name, "builtin_checksum_%d", t);
    Record rec(name);
    snprintf(name, sizeof name, "builtin_checksum_last_byte_%d", t);
    Record last(name);
    for (size_t n = 0; n <= kMaxLen; ++n) {
      rec.u32(GoldenComputeBuiltinChecksum(static_cast<ChecksumType>(t), buf.data(), n));
      rec.end_item();
      last.u32(GoldenComputeBuiltinChecksumWithLastByte(static_cast<ChecksumType>(t),
                                                        buf.data(), n, buf[n]));
      last.end_item();
    }
    rec.finish();
    last.finish();
  }
  {
    Record rec("file_checksum_crc32c");
    for (size_t n = 0; n <= kMaxLen; ++n) {
      FileChecksumGenContext ctx;
      FileChecksumGenCrc32c gen(ctx);
      size_t a = n / 2;
      gen.Update(buf.data(), a);
      gen.Update(buf.data() + a, n - a);
      gen.Finalize();
      std::string s = gen.GetChecksum();
      rec.bytes(s.data(), s.size());
      rec.end_item();
    }
    rec.finish();
  }

  // ---- by random draw, kDraws each, SplitMix64 seeded per record ----
  {
    Record rec("fastrange32");
    SplitMix64 r{1};
    for (size_t i = 0; i < kDraws; ++i) {
      uint32_t h = static_cast<uint32_t>(r.next());
      uint32_t range = static_cast<uint32_t>(r.next());
      if (i % 4 == 0) range &= 0xff;
      rec.u32(FastRange32(h, range));
      rec.end_item();
    }
    rec.finish();
  }
  {
    Record rec("fastrange64");
    SplitMix64 r{2};
    for (size_t i = 0; i < kDraws; ++i) {
      uint64_t h = r.next();
      uint64_t range = r.next();
      if (i % 4 == 0) range >>= 40;
      rec.u64(FastRange64(h, static_cast<size_t>(range)));
      rec.end_item();
    }
    rec.finish();
  }
  {
    Record rec("bijective_hash2x64");
    SplitMix64 r{3};
    for (size_t i = 0; i < kDraws; ++i) {
      uint64_t in_hi = r.next(), in_lo = r.next(), seed = r.next();
      if (i % 2 == 0) seed = 0;
      uint64_t hi, lo, uhi, ulo;
      BijectiveHash2x64(in_hi, in_lo, seed, &hi, &lo);
      BijectiveUnhash2x64(in_hi, in_lo, seed, &uhi, &ulo);
      rec.u64(hi);
      rec.u64(lo);
      rec.u64(uhi);
      rec.u64(ulo);
      rec.end_item();
    }
    rec.finish();
  }
  {
    Record rec("context_modifier");
    SplitMix64 r{4};
    for (size_t i = 0; i < kDraws; ++i) {
      uint32_t base = static_cast<uint32_t>(r.next());
      uint64_t offset = r.next();
      if (i % 8 == 0) base = 0;
      if (i % 3 == 0) offset >>= 20;
      rec.u32(ChecksumModifierForContext(base, offset));
      rec.end_item();
    }
    rec.finish();
  }
  {
    Record rec("crc32c_mask_unmask");
    SplitMix64 r{5};
    for (size_t i = 0; i < kDraws; ++i) {
      uint32_t v = static_cast<uint32_t>(r.next());
      rec.u32(crc32c::Mask(v));
      rec.u32(crc32c::Unmask(v));
      rec.end_item();
    }
    rec.finish();
  }
  {
    Record v64("varint_encode"), sv64("varsignedint_encode"), p32("prefix_varint32_encode"),
        p64("prefix_varint64_encode");
    SplitMix64 r{6};
    for (size_t i = 0; i < kDraws; ++i) {
      uint64_t v = r.next();
      v >>= r.next() % 64;
      std::string s;
      PutVarint64(&s, v);
      PutVarint32(&s, static_cast<uint32_t>(v));
      v64.u8(static_cast<uint8_t>(VarintLength(v)));
      v64.bytes(s.data(), s.size());
      v64.end_item();
      s.clear();
      PutVarsignedint64(&s, static_cast<int64_t>(v));
      PutVarsignedint64(&s, -static_cast<int64_t>(v >> 1));
      sv64.bytes(s.data(), s.size());
      sv64.end_item();
      s.clear();
      PutPrefixVarint32(&s, static_cast<uint32_t>(v));
      p32.bytes(s.data(), s.size());
      p32.end_item();
      s.clear();
      PutPrefixVarint64(&s, v);
      p64.bytes(s.data(), s.size());
      p64.end_item();
    }
    v64.finish();
    sv64.finish();
    p32.finish();
    p64.finish();
  }
  {
    // Random byte strings of 1..=11 bytes. Status: 0 refused, 1 accepted (value, length), 2
    // accepted by RocksDB with bits past the type's width, which the port refuses by design.
    Record d32("varint32_decode"), d64("varint64_decode"), q32("prefix_varint32_decode"),
        q64("prefix_varint64_decode");
    SplitMix64 r{7};
    for (size_t i = 0; i < kDraws; ++i) {
      size_t k = 1 + r.next() % 11;
      char in[16];
      EncodeFixed64(in, r.next());
      EncodeFixed64(in + 8, r.next());
      {
        uint32_t v = 0;
        const char* q = GetVarint32Ptr(in, in + k, &v);
        if (q == nullptr) {
          d32.u8(0);
        } else if (q - in == 5 && static_cast<unsigned char>(in[4]) > 0x0f) {
          d32.u8(2);
        } else {
          d32.u8(1);
          d32.u32(v);
          d32.u8(static_cast<uint8_t>(q - in));
        }
        d32.end_item();
      }
      {
        uint64_t v = 0;
        const char* q = GetVarint64Ptr(in, in + k, &v);
        if (q == nullptr) {
          d64.u8(0);
        } else if (q - in == 10 && static_cast<unsigned char>(in[9]) > 0x01) {
          d64.u8(2);
        } else {
          d64.u8(1);
          d64.u64(v);
          d64.u8(static_cast<uint8_t>(q - in));
        }
        d64.end_item();
      }
      {
        uint32_t v = 0;
        const char* q = GetPrefixVarint32Ptr(in, in + k, &v);
        if (q == nullptr) {
          q32.u8(0);
        } else {
          q32.u8(1);
          q32.u32(v);
          q32.u8(static_cast<uint8_t>(q - in));
        }
        q32.end_item();
      }
      {
        uint64_t v = 0;
        const char* q = GetPrefixVarint64Ptr(in, in + k, &v);
        if (q == nullptr) {
          q64.u8(0);
        } else {
          q64.u8(1);
          q64.u64(v);
          q64.u8(static_cast<uint8_t>(q - in));
        }
        q64.end_item();
      }
    }
    d32.finish();
    d64.finish();
    q32.finish();
    q64.finish();
  }
  {
    Record rec("downward_involution");
    SplitMix64 r{8};
    for (size_t i = 0; i < kDraws; ++i) {
      uint64_t v = r.next();
      rec.u64(DownwardInvolution(v));
      rec.u32(DownwardInvolution(static_cast<uint32_t>(v)));
      rec.end_item();
    }
    rec.finish();
  }
  return 0;
}
