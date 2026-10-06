// Data block read benchmark for mantle-engine P4, RocksDB 11.8.1's side: DataBlockIter over
// blocks BlockBuilder wrote (table/block_based/block.cc), as a table read sees them once a block
// is in memory. The Rust side is crates/engine/benches/block_read.rs; both build the same blocks
// from SplitMix64 keys, and the port's builder writes them byte for byte as RocksDB's
// (tests/block_builder_golden.rs).
//
// Blocks are cut at RocksDB's default block_size, 4096 bytes, with its default restart interval
// of 16 and delta encoding, as FlushBlockBySizePolicy cuts them (deviation 10). Keys are
// 16-byte user keys in order with an 8-byte trailer, values 100 bytes: db_bench's defaults.
// `index` 0 is binary search (RocksDB's default), 1 adds the data block hash index.
// Workloads, after building every block (not timed), each reading blocks as RocksDB's table
// reader does:
//   scan: SeekToFirst then Next over every entry of every block, one iterator object reused
//         from block to block (BlockBasedTableIterator's block_iter_)
//   seek: Seek to every key, in a SplitMix64 permutation, in the block that holds it, the same
//         iterator object reused
//   get:  SeekForGet to every key in the same order, a fresh iterator on the stack each time,
//         as BlockBasedTable::Get reads a data block
// `key` is the user key's length (16 by default; db_bench's), which with its 8-byte trailer is
// within IterKey's 39-byte inline buffer at 16 and past it at 48.
// Output: "workload index key N ns_per_op allocations reallocations minor_faults checksum" for one
// run; the counts are for the whole workload, and the checksum keeps the reads live. The
// iterators of `seek` and `get` are made before either is timed, as a table reader keeps one.
//
// Built outside the repository (R=~/Projects/rocksdb, L=~/Projects/rocksdb-golden/lib):
//   clang++ -std=c++20 -O2 -DNDEBUG -march=armv8-a+crc+crypto -I$R -I$R/include \
//     -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_MACOSX p4_block_bench.cc \
//     $L/librocksdb.a -o p4_block_bench
//   ./p4_block_bench N index key [passes [scan]]
#include <sys/resource.h>

#include <chrono>
#include <new>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <utility>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "rocksdb/table.h"
#include "table/block_based/block.h"
#include "table/block_based/block_builder.h"
#include "util/coding.h"

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
};

size_t kKey = 16;
constexpr size_t kValue = 100, kBlockSize = 4096;
constexpr int kRestart = 16, kDeviation = 10;

std::string Key(uint64_t i) {
  std::string k(8, '\0');
  for (int b = 0; b < 8; ++b) k[b] = static_cast<char>(i >> (56 - 8 * b));
  std::string user(kKey - 8, 'k');
  user += k;
  PutFixed64(&user, PackSequenceAndType(1, kTypeValue));
  return user;
}

// Allocations by the program, counted at the global operator new; RocksDB's iterators, keys and
// blocks allocate through it.
size_t g_allocs = 0;

long Faults() {
  rusage u;
  getrusage(RUSAGE_SELF, &u);
  return u.ru_minflt;
}

double Now() {
  return std::chrono::duration<double, std::nano>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}
}  // namespace

void* operator new(size_t n) {
  ++g_allocs;
  if (void* p = std::malloc(n ? n : 1)) return p;
  throw std::bad_alloc();
}
void* operator new[](size_t n) { return operator new(n); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, size_t) noexcept { std::free(p); }
void operator delete[](void* p, size_t) noexcept { std::free(p); }

int main(int argc, char** argv) {
  size_t n = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 1000000;
  bool hash = argc > 2 && std::atoi(argv[2]) == 1;
  if (argc > 3) kKey = std::strtoull(argv[3], nullptr, 10);
  // Passes over each workload, for profiling one long enough to sample; times are per pass.
  size_t passes = argc > 4 ? std::strtoull(argv[4], nullptr, 10) : 1;
  // `scan` stops after the scan, to profile or compare it alone.
  bool scan_only = argc > 5 && std::string(argv[5]) == "scan";
  SplitMix64 rng{0x626c6f636b};  // "block"
  std::vector<std::string> keys;
  for (size_t i = 0; i < n; ++i) keys.push_back(Key(i * 7));
  std::string value(kValue, '\0');
  // Blocks, and the block each key went to.
  std::vector<std::unique_ptr<Block>> blocks;
  std::vector<uint32_t> where(n);
  std::vector<std::string> storage;
  storage.reserve(n / 8 + 1);
  {
    BlockBuilder b(kRestart, true, false,
                   hash ? BlockBasedTableOptions::kDataBlockBinaryAndHash
                        : BlockBasedTableOptions::kDataBlockBinarySearch);
    const size_t limit = (kBlockSize * (100 - kDeviation) + 99) / 100;
    auto cut = [&]() {
      storage.push_back(b.Finish().ToString());
      blocks.push_back(std::make_unique<Block>(BlockContents(Slice(storage.back())), 0,
                                               nullptr, kRestart));
      b.Reset();
    };
    for (size_t i = 0; i < n; ++i) {
      for (auto& c : value) c = static_cast<char>(rng.next());
      if (!b.empty()) {
        size_t cur = b.CurrentSizeEstimate();
        if (cur >= kBlockSize ||
            (b.EstimateSizeAfterKV(keys[i], value) > kBlockSize && cur > limit)) {
          cut();
        }
      }
      b.Add(keys[i], value);
      where[i] = static_cast<uint32_t>(blocks.size());
    }
    cut();
  }
  std::vector<size_t> order(n);
  for (size_t i = 0; i < n; ++i) order[i] = i;
  for (size_t i = n; i > 1; --i) std::swap(order[i - 1], order[rng.next() % i]);

  uint64_t sum = 0;
  size_t a0 = g_allocs;
  long f0 = Faults();
  double t0 = Now();
  size_t entries = 0;
  DataBlockIter reused;
  for (size_t p = 0; p < passes; ++p)
  for (auto& blk : blocks) {
    DataBlockIter* it =
        blk->NewDataIterator(BytewiseComparator(), kDisableGlobalSequenceNumber, &reused);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      sum += static_cast<uint8_t>(it->key()[kKey - 1]) + static_cast<uint8_t>(it->value()[0]);
      ++entries;
    }
  }
  double t1 = Now();
  size_t a1 = g_allocs;
  long f1 = Faults();
  printf("scan %d %zu %zu %.2f %zu 0 %ld %llu\n", hash, kKey, n, (t1 - t0) / entries, a1 - a0, f1 - f0,
         static_cast<unsigned long long>(sum));

  if (scan_only) return 0;
  sum = 0;
  a0 = g_allocs;
  f0 = Faults();
  t0 = Now();
  for (size_t p = 0; p < passes; ++p)
  for (size_t i : order) {
    DataBlockIter* it = blocks[where[i]]->NewDataIterator(
        BytewiseComparator(), kDisableGlobalSequenceNumber, &reused);
    it->Seek(keys[i]);
    sum += static_cast<uint8_t>(it->value()[0]);
  }
  t1 = Now();
  a1 = g_allocs;
  f1 = Faults();
  printf("seek %d %zu %zu %.2f %zu 0 %ld %llu\n", hash, kKey, n, (t1 - t0) / (n * passes), a1 - a0, f1 - f0, static_cast<unsigned long long>(sum));

  sum = 0;
  a0 = g_allocs;
  f0 = Faults();
  t0 = Now();
  for (size_t p = 0; p < passes; ++p)
  for (size_t i : order) {
    // As BlockBasedTable::Get reads a data block: a fresh iterator on the stack.
    DataBlockIter fresh;
    DataBlockIter* it = blocks[where[i]]->NewDataIterator(
        BytewiseComparator(), kDisableGlobalSequenceNumber, &fresh);
    sum += it->SeekForGet(keys[i]) ? static_cast<uint8_t>(it->value()[0]) : 0;
  }
  t1 = Now();
  a1 = g_allocs;
  f1 = Faults();
  printf("get %d %zu %zu %.2f %zu 0 %ld %llu\n", hash, kKey, n, (t1 - t0) / (n * passes), a1 - a0, f1 - f0, static_cast<unsigned long long>(sum));
  return 0;
}
