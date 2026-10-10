// Memtable micro benchmark for mantle-engine P2, RocksDB 11.8.1's side: MemTable::Add and
// MemTable::Get (db/memtable.cc) over a SkipListRep, as the DB path builds it: bytewise
// comparator, arena_block_size 1 MiB (what SanitizeOptions derives for a large write buffer,
// db/column_family.cc:239-249), no memtable Bloom filter, no prefix extractor, no per-key
// protection, single-threaded non-concurrent Add. The Rust side is
// crates/engine/benches/memtable.rs (this file is copied beside it); both draw the same keys
// from SplitMix64.
//
// Workloads, each over N entries of 16-byte keys and 100-byte values:
//   fill_random: Add at keys drawn from SplitMix64 (two draws, little-endian, per key)
//   fill_seq:    Add at keys 0..N as 8 zero bytes then the big-endian counter
//   get_random:  Get of every fill_random key, in a second SplitMix64 order (a permutation
//                by index draws), after the fill
// Output: "workload N run ns_per_op bytes_per_entry" per run.
//
// Built outside the repository (R=~/Projects/rocksdb, L=~/Projects/rocksdb-golden/lib):
//   clang++ -std=c++20 -O2 -DNDEBUG -march=armv8-a+crc+crypto -I$R -I$R/include \
//     -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_MACOSX p2_memtable_bench.cc \
//     $L/librocksdb.a -o p2_memtable_bench
//   ./p2_memtable_bench 100000 1000000
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/lookup_key.h"
#include "db/memtable.h"
#include "db/merge_context.h"
#include "options/cf_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/options.h"
#include "rocksdb/write_buffer_manager.h"
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

constexpr size_t kKey = 16;
constexpr size_t kValue = 100;
constexpr int kRuns = 5;

std::vector<std::string> RandomKeys(size_t n) {
  SplitMix64 r{0x6d656d7461626c65ULL};  // "memtable"
  std::vector<std::string> keys(n, std::string(kKey, '\0'));
  for (auto& k : keys) {
    EncodeFixed64(&k[0], r.next());
    EncodeFixed64(&k[8], r.next());
  }
  return keys;
}

std::vector<std::string> SeqKeys(size_t n) {
  std::vector<std::string> keys(n, std::string(kKey, '\0'));
  for (size_t i = 0; i < n; ++i) {
    uint64_t v = i;
    for (int b = 0; b < 8; ++b) keys[i][15 - b] = static_cast<char>((v >> (8 * b)) & 0xff);
  }
  return keys;
}

// The order a get pass visits the keys: SplitMix64 draws modulo the remaining count
// (Fisher-Yates from the back).
std::vector<size_t> Permutation(size_t n) {
  SplitMix64 r{0x676574676574ULL};
  std::vector<size_t> p(n);
  for (size_t i = 0; i < n; ++i) p[i] = i;
  for (size_t i = n; i > 1; --i) std::swap(p[i - 1], p[r.next() % i]);
  return p;
}

struct Bench {
  Options options;
  ImmutableOptions ioptions;
  MutableCFOptions moptions;
  InternalKeyComparator cmp;
  WriteBufferManager wb;
  Bench()
      : options(Make()), ioptions(options), moptions(options),
        cmp(BytewiseComparator()), wb(0) {}
  static Options Make() {
    Options o;
    o.memtable_factory = std::make_shared<SkipListFactory>();
    o.write_buffer_size = size_t{1} << 30;
    o.arena_block_size = size_t{1} << 20;
    return o;
  }
  MemTable* New() {
    auto* m = new MemTable(cmp, ioptions, moptions, &wb, kMaxSequenceNumber, 0);
    m->Ref();
    return m;
  }
};

double NsPer(std::chrono::steady_clock::time_point t0, size_t n) {
  auto d = std::chrono::steady_clock::now() - t0;
  return std::chrono::duration<double, std::nano>(d).count() / static_cast<double>(n);
}

void Fill(Bench& b, MemTable* m, const std::vector<std::string>& keys) {
  std::string value(kValue, 'v');
  SequenceNumber seq = 0;
  for (const auto& k : keys) {
    Status s = m->Add(++seq, kTypeValue, k, value, nullptr);
    if (!s.ok()) {
      fprintf(stderr, "add: %s\n", s.ToString().c_str());
      exit(1);
    }
  }
}

int main(int argc, char** argv) {
  Bench b;
  for (int a = 1; a < argc; ++a) {
    size_t n = strtoull(argv[a], nullptr, 10);
    auto rkeys = RandomKeys(n);
    auto skeys = SeqKeys(n);
    auto perm = Permutation(n);
    for (int run = 0; run < kRuns; ++run) {
      {
        MemTable* m = b.New();
        auto t0 = std::chrono::steady_clock::now();
        Fill(b, m, rkeys);
        double ns = NsPer(t0, n);
        double bytes = static_cast<double>(m->ApproximateMemoryUsage()) / n;
        printf("fill_random %zu %d %.1f %.1f\n", n, run, ns, bytes);

        std::string value;
        size_t found = 0;
        auto t1 = std::chrono::steady_clock::now();
        for (size_t i : perm) {
          LookupKey lk(rkeys[i], kMaxSequenceNumber);
          MergeContext mc;
          SequenceNumber max_cov = 0;
          Status s;
          if (m->Get(lk, &value, nullptr, nullptr, &s, &mc, &max_cov, ReadOptions(), false) &&
              s.ok()) {
            ++found;
          }
        }
        double gns = NsPer(t1, n);
        if (found != n) {
          fprintf(stderr, "found %zu of %zu\n", found, n);
          return 1;
        }
        printf("get_random %zu %d %.1f %.1f\n", n, run, gns, bytes);
        delete m->Unref();
      }
      {
        MemTable* m = b.New();
        auto t0 = std::chrono::steady_clock::now();
        Fill(b, m, skeys);
        double ns = NsPer(t0, n);
        printf("fill_seq %zu %d %.1f %.1f\n", n, run, ns,
               static_cast<double>(m->ApproximateMemoryUsage()) / n);
        delete m->Unref();
      }
    }
  }
  return 0;
}
