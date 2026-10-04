// Golden vectors for mantle-engine P2's internal keys and user comparators, computed by
// RocksDB 11.8.1's own code (~/Projects/rocksdb at abeebd963): util/comparator.cc
// (BytewiseComparator, ReverseBytewiseComparator: FindShortestSeparator, FindShortSuccessor,
// IsSameLengthImmediateSuccessor), db/dbformat.{h,cc} (InternalKeyComparator Compare and
// CompareKeySeq, ParseInternalKey, LookupKey) and table/block_based/index_builder.cc
// (ShortenedIndexBuilder::FindShortestInternalKeySeparator, FindShortInternalKeySuccessor),
// whose outputs become index keys on disk.
//
// Output: as p1_gen.cc, one line per record, "name count d0 d1 ...", each d the FNV-1a 64
// digest of a window of 32 draws. Inputs are SplitMix64 draws over a small alphabet, so common
// prefixes, 0x00 and 0xFF runs and equal user keys are frequent.
//
// Built outside the repository against an unmodified checkout (R=~/Projects/rocksdb) and a
// static library built from its src.mk LIB_SOURCES with -O2 -DNDEBUG (L), on aarch64 macOS:
//   clang++ -std=c++20 -O2 -DNDEBUG -march=armv8-a+crc+crypto -I$R -I$R/include \
//     -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_MACOSX p2_dbformat_gen.cc \
//     $L/librocksdb.a -o p2_dbformat_gen
//   ./p2_dbformat_gen > p2_dbformat.txt
// crates/engine/tests/dbformat_golden.rs recomputes every record with the port and compares.
#include <cstdio>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/lookup_key.h"
#include "rocksdb/comparator.h"
#include "table/block_based/index_builder.h"
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

constexpr size_t kDraws = 8192;
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
  // A byte string, length first so adjacent strings cannot alias.
  void str(const Slice& s) {
    u64(s.size());
    bytes(s.data(), s.size());
  }
  void sign(int c) { u8(c < 0 ? 0 : (c == 0 ? 1 : 2)); }
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

const unsigned char kAlphabet[8] = {0x00, 0x01, 'A', 'B', 0x7f, 0x80, 0xfe, 0xff};
const ValueType kTypes[8] = {kTypeDeletion,      kTypeValue,     kTypeMerge,
                             kTypeSingleDeletion, kTypeRangeDeletion,
                             kTypeBlobIndex,     kTypeWideColumnEntity,
                             kTypeValuePreferredSeqno};

std::string RandKey(SplitMix64& r, size_t max_len) {
  size_t n = r.next() % (max_len + 1);
  std::string s;
  for (size_t i = 0; i < n; ++i) s.push_back(static_cast<char>(kAlphabet[r.next() % 8]));
  return s;
}

// A key pair sharing a prefix of random length, each with its own random tail.
std::pair<std::string, std::string> RandPair(SplitMix64& r) {
  std::string a = RandKey(r, 12);
  size_t p = r.next() % (a.size() + 1);
  std::string b = a.substr(0, p) + RandKey(r, 6);
  return {a, b};
}

std::string IKey(const std::string& u, uint64_t seq, ValueType t) {
  std::string s;
  AppendInternalKey(&s, ParsedInternalKey(u, seq, t));
  return s;
}

void Separators(const char* name, const Comparator* c, uint64_t seed) {
  Record rec(name);
  SplitMix64 r{seed};
  for (size_t i = 0; i < kDraws; ++i) {
    auto [a, b] = RandPair(r);
    std::string s = a;
    c->FindShortestSeparator(&s, b);
    rec.str(s);
    rec.end_item();
  }
  rec.finish();
}

void Successors(const char* name, const Comparator* c, uint64_t seed) {
  Record rec(name);
  SplitMix64 r{seed};
  for (size_t i = 0; i < kDraws; ++i) {
    std::string s = RandKey(r, 12);
    c->FindShortSuccessor(&s);
    rec.str(s);
    rec.end_item();
  }
  rec.finish();
}

void SameLengthSuccessor(const char* name, const Comparator* c, uint64_t seed) {
  Record rec(name);
  SplitMix64 r{seed};
  for (size_t i = 0; i < kDraws; ++i) {
    std::string a, b;
    if (r.next() % 2 == 0) {
      // A constructed immediate successor, sometimes broken at one byte.
      std::string head = RandKey(r, 6);
      unsigned char x = static_cast<unsigned char>(r.next() % 0xff);
      size_t k = r.next() % 4;
      a = head + static_cast<char>(x) + std::string(k, '\xff');
      b = head + static_cast<char>(x + 1) + std::string(k, '\0');
      if (k > 0 && r.next() % 3 == 0) b[b.size() - 1] = static_cast<char>(r.next() % 256);
    } else {
      size_t n = r.next() % 6;
      for (size_t j = 0; j < n; ++j) {
        a.push_back(static_cast<char>(kAlphabet[r.next() % 8]));
        b.push_back(static_cast<char>(kAlphabet[r.next() % 8]));
      }
    }
    rec.u8(c->IsSameLengthImmediateSuccessor(a, b) ? 1 : 0);
    rec.end_item();
  }
  rec.finish();
}

// Two internal keys: equal user keys half the time, sequence numbers from a small range so
// ties happen, types from the stored set.
std::pair<std::string, std::string> RandIKeys(SplitMix64& r) {
  auto [a, b] = RandPair(r);
  if (r.next() % 2 == 0) b = a;
  uint64_t sa = r.next() % 4, sb = r.next() % 4;
  ValueType ta = kTypes[r.next() % 8], tb = kTypes[r.next() % 8];
  return {IKey(a, sa, ta), IKey(b, sb, tb)};
}

void Icmp(const char* name, const Comparator* c, uint64_t seed) {
  Record rec(name);
  SplitMix64 r{seed};
  InternalKeyComparator icmp(c);
  for (size_t i = 0; i < kDraws; ++i) {
    auto [a, b] = RandIKeys(r);
    rec.sign(icmp.Compare(a, b));
    rec.sign(icmp.CompareKeySeq(a, b));
    rec.end_item();
  }
  rec.finish();
}

void InternalSeparators(const char* name, const Comparator* c, uint64_t seed) {
  Record rec(name);
  SplitMix64 r{seed};
  for (size_t i = 0; i < kDraws; ++i) {
    auto [a, b] = RandIKeys(r);
    std::string scratch;
    rec.str(ShortenedIndexBuilder::FindShortestInternalKeySeparator(*c, a, b, &scratch));
    std::string scratch2;
    rec.str(ShortenedIndexBuilder::FindShortInternalKeySuccessor(*c, a, &scratch2));
    rec.end_item();
  }
  rec.finish();
}

int main() {
  const Comparator* bw = BytewiseComparator();
  const Comparator* rev = ReverseBytewiseComparator();
  Separators("bytewise_separator", bw, 1);
  Separators("reverse_separator", rev, 2);
  Successors("bytewise_successor", bw, 3);
  Successors("reverse_successor", rev, 4);
  SameLengthSuccessor("bytewise_same_length_successor", bw, 5);
  SameLengthSuccessor("reverse_same_length_successor", rev, 6);
  Icmp("icmp_bytewise", bw, 7);
  Icmp("icmp_reverse", rev, 8);
  InternalSeparators("internal_separator_bytewise", bw, 9);
  InternalSeparators("internal_separator_reverse", rev, 10);
  {
    Record rec("lookup_key");
    SplitMix64 r{11};
    for (size_t i = 0; i < kDraws; ++i) {
      std::string u = RandKey(r, 300);
      uint64_t seq = r.next() & kMaxSequenceNumber;
      LookupKey lk(u, seq);
      rec.str(lk.memtable_key());
      rec.str(lk.internal_key());
      rec.str(lk.user_key());
      rec.end_item();
    }
    rec.finish();
  }
  {
    Record rec("parse_internal_key");
    SplitMix64 r{12};
    for (size_t i = 0; i < kDraws; ++i) {
      size_t n = r.next() % 17;
      std::string s;
      for (size_t j = 0; j < n; ++j) s.push_back(static_cast<char>(r.next() & 0xff));
      // Half the draws get a type byte below 0x20 so both outcomes are frequent.
      if (n >= 8 && r.next() % 2 == 0) s[n - 8] = static_cast<char>(r.next() % 0x20);
      ParsedInternalKey p;
      Status st = ParseInternalKey(s, &p, false);
      rec.u8(st.ok() ? 1 : 0);
      if (st.ok()) {
        rec.str(p.user_key);
        rec.u64(p.sequence);
        rec.u8(p.type);
      }
      rec.end_item();
    }
    rec.finish();
  }
  return 0;
}
