// Golden vectors for mantle-engine P2's wide-column entities and blob indexes, computed by
// RocksDB 11.8.1's own code (~/Projects/rocksdb at abeebd963): db/wide/
// wide_column_serialization.cc (Serialize = version 1, SerializeV2 with blob columns,
// GetValueOfDefaultColumn, HasBlobColumns, Deserialize) and db/blob/blob_index.h (EncodeBlob,
// EncodeBlobTTL, EncodeInlinedTTL, DecodeFrom).
//
// Output: "name count d0 d1 ..." records as p1_gen.cc (FNV-1a 64 of each window of 32 draws),
// then "hex v1 <bytes>" and "hex v2 <bytes>" lines for the first 16 draws' entities in full,
// which the port parses and re-serializes.
//
// Each draw: up to 8 columns with names over a small alphabet (so the default column and shared
// prefixes are common) and values of 0-299 random bytes (so value sizes take 1- and 2-byte
// varints); a random subset become blob columns, each a blob, blob-with-TTL or inlined-TTL
// index. Every random argument is drawn into a local first, in the order written, because C++
// leaves the order of evaluating a call's arguments unspecified.
//
// Built outside the repository against an unmodified checkout (R=~/Projects/rocksdb) and the
// static library L=~/Projects/rocksdb-golden/lib (src.mk LIB_SOURCES, -O2 -DNDEBUG):
//   clang++ -std=c++20 -O2 -DNDEBUG -march=armv8-a+crc+crypto -I$R -I$R/include \
//     -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_MACOSX p2_wide_gen.cc \
//     $L/librocksdb.a -o p2_wide_gen
//   ./p2_wide_gen > p2_wide.txt
// crates/engine/tests/wide_column_golden.rs recomputes every record and parses the hex lines.
#include <cstdio>
#include <map>
#include <string>
#include <vector>

#include "db/blob/blob_index.h"
#include "db/wide/wide_column_serialization.h"
#include "util/coding.h"

using namespace ROCKSDB_NAMESPACE;

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

constexpr size_t kDraws = 4096;
constexpr size_t kHexDraws = 16;
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
  void u64(uint64_t v) {
    char b[8];
    EncodeFixed64(b, v);
    bytes(b, 8);
  }
  void str(const Slice& s) {
    u64(s.size());
    bytes(s.data(), s.size());
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

const unsigned char kAlphabet[4] = {'a', 'b', 0x00, 0xff};

std::string Hex(const std::string& s) {
  std::string out;
  char buf[3];
  for (unsigned char c : s) {
    snprintf(buf, sizeof buf, "%02x", c);
    out += buf;
  }
  return out;
}

int main() {
  Record v1("serialize_v1"), v2("serialize_v2"), dflt("default_column"),
      blobs("blob_index_encode"), parsed("deserialize_v2");
  std::vector<std::string> hex_lines;
  SplitMix64 r{0x5749444543304C53ULL};
  for (size_t draw = 0; draw < kDraws; ++draw) {
    std::map<std::string, std::string> entity;
    const uint64_t n = r.next() % 9;
    for (uint64_t k = 0; k < n; ++k) {
      const uint64_t name_len = r.next() % 6;
      std::string name;
      for (uint64_t j = 0; j < name_len; ++j) name.push_back(kAlphabet[r.next() % 4]);
      const uint64_t value_len = r.next() % 300;
      std::string value;
      for (uint64_t j = 0; j < value_len; ++j) value.push_back(static_cast<char>(r.next() & 0xff));
      entity[name] = value;
    }
    WideColumns columns;
    for (const auto& [name, value] : entity) columns.emplace_back(name, value);

    std::string s1;
    if (!WideColumnSerialization::Serialize(columns, s1).ok()) return 1;
    v1.str(s1);

    // Blob columns: encodings kept alive for the inlined ones' value slices.
    const uint64_t mask = r.next();
    std::vector<std::string> encodings;
    encodings.reserve(columns.size());
    std::vector<std::pair<size_t, BlobIndex>> blob_columns;
    for (size_t i = 0; i < columns.size(); ++i) {
      if (((mask >> i) & 1) == 0) continue;
      const uint64_t kind = r.next() % 3;
      encodings.emplace_back();
      std::string& enc = encodings.back();
      if (kind == 0) {
        const uint64_t file = r.next() % 1000000;
        const uint64_t offset = r.next();
        const uint64_t size = r.next() % 1000000;
        const uint64_t comp = r.next() % 8;
        BlobIndex::EncodeBlob(&enc, file, offset, size, static_cast<CompressionType>(comp));
      } else if (kind == 1) {
        const uint64_t exp = r.next();
        const uint64_t file = r.next() % 1000000;
        const uint64_t offset = r.next();
        const uint64_t size = r.next() % 1000000;
        const uint64_t comp = r.next() % 8;
        BlobIndex::EncodeBlobTTL(&enc, exp, file, offset, size,
                                 static_cast<CompressionType>(comp));
      } else {
        const uint64_t exp = r.next();
        const uint64_t len = r.next() % 20;
        BlobIndex::EncodeInlinedTTL(&enc, exp, std::string(len, 'i'));
      }
      blobs.str(enc);
      BlobIndex bi;
      if (!bi.DecodeFrom(enc).ok()) return 2;
      blob_columns.emplace_back(i, bi);
    }
    blobs.end_item();

    std::string s2;
    if (!WideColumnSerialization::SerializeV2(columns, blob_columns, s2).ok()) return 3;
    v2.str(s2);

    for (const std::string* s : {&s1, &s2}) {
      Slice value;
      bool is_blob = false;
      Status st = WideColumnSerialization::GetValueOfDefaultColumn(*s, value, is_blob);
      dflt.u8(st.ok() ? 1 : 0);
      dflt.str(value);
      dflt.u8(is_blob ? 1 : 0);
      bool has = false;
      st = WideColumnSerialization::HasBlobColumns(*s, has);
      dflt.u8(st.ok() ? 1 : 0);
      dflt.u8(has ? 1 : 0);
    }

    WideColumns back;
    std::vector<std::pair<size_t, BlobIndex>> back_blobs;
    if (!WideColumnSerialization::Deserialize(s2, back, &back_blobs).ok()) return 4;
    for (const auto& c : back) {
      parsed.str(c.name());
      parsed.str(c.value());
    }
    for (const auto& [i, bi] : back_blobs) {
      parsed.u64(i);
      std::string enc;
      bi.EncodeTo(&enc);
      parsed.str(enc);
    }

    v1.end_item();
    v2.end_item();
    dflt.end_item();
    parsed.end_item();
    if (draw < kHexDraws) {
      hex_lines.push_back("hex v1 " + Hex(s1));
      hex_lines.push_back("hex v2 " + Hex(s2));
    }
  }
  v1.finish();
  v2.finish();
  dflt.finish();
  blobs.finish();
  parsed.finish();
  for (const auto& l : hex_lines) printf("%s\n", l.c_str());
  return 0;
}
