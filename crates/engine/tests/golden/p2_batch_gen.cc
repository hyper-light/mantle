// Golden write batches for mantle-engine P2, built by RocksDB 11.8.1's own WriteBatch
// (~/Projects/rocksdb at abeebd963: db/write_batch.cc, db/write_batch_internal.h,
// db/wide/wide_column_serialization.cc, db/blob/blob_index.h).
//
// 512 batches, each from a SplitMix64-driven script of operations over every record the batch
// writes: Put (whole and in parts), Delete, SingleDelete, DeleteRange, Merge, PutEntity
// (duplicate column names included, which RocksDB refuses), LogData, TimedPut (a write time
// of UINT64_MAX becoming a Put), PutBlobIndex, each in the default column family and in others
// (ids to 300, so the varint takes two bytes), with save points set, rolled back and popped,
// Clear, a max_bytes limit on some batches, two-phase-commit markers (a leading Noop turned into
// BeginPrepare by MarkEndPrepare under each write policy, Commit, Rollback,
// CommitWithTimestamp), a sequence number, and Append with and without a WAL termination
// point. Keys and values are random bytes, sometimes over 200 long.
//
// Output, one line per batch: the batch's bytes in hex; each operation's outcome (1 ok, 0
// refused, or "-" for none); and three truncations "k:c" of the batch to k bytes with the
// outcome of iterating the truncated bytes (O ok, C corruption, X other).
//
// Built outside the repository against an unmodified checkout (R=~/Projects/rocksdb) and a
// static library built from its src.mk LIB_SOURCES with -O2 -DNDEBUG (L), on aarch64 macOS:
//   clang++ -std=c++20 -O2 -DNDEBUG -march=armv8-a+crc+crypto -I$R -I$R/include \
//     -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_MACOSX p2_batch_gen.cc \
//     $L/librocksdb.a -o p2_batch_gen
//   ./p2_batch_gen > p2_batch.txt
// crates/engine/tests/write_batch_golden.rs builds the same batches with the port and compares
// bytes, outcomes and truncations, and decodes every batch the oracle wrote.
#include <cstdio>
#include <limits>
#include <string>
#include <vector>

#include "db/blob/blob_index.h"
#include "db/dbformat.h"
#include "db/write_batch_internal.h"
#include "rocksdb/wide_columns.h"
#include "rocksdb/write_batch.h"

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

SplitMix64 r{0x5032424154434831ULL};  // "P2BATCH1"

std::string Bytes(size_t max) {
  size_t n = r.next() % (max + 1);
  if (r.next() % 16 == 0) n += 200;
  std::string s;
  for (size_t i = 0; i < n; ++i) s.push_back(static_cast<char>(r.next() & 0xff));
  return s;
}

uint32_t Cf() {
  if (r.next() % 2 == 0) return 0;
  return 1 + static_cast<uint32_t>(r.next() % 300);
}

struct AcceptAll : public WriteBatch::Handler {
  Status PutCF(uint32_t, const Slice&, const Slice&) override { return Status::OK(); }
  Status TimedPutCF(uint32_t, const Slice&, const Slice&, uint64_t) override {
    return Status::OK();
  }
  Status PutEntityCF(uint32_t, const Slice&, const Slice&) override { return Status::OK(); }
  Status DeleteCF(uint32_t, const Slice&) override { return Status::OK(); }
  Status SingleDeleteCF(uint32_t, const Slice&) override { return Status::OK(); }
  Status DeleteRangeCF(uint32_t, const Slice&, const Slice&) override { return Status::OK(); }
  Status MergeCF(uint32_t, const Slice&, const Slice&) override { return Status::OK(); }
  Status PutBlobIndexCF(uint32_t, const Slice&, const Slice&) override { return Status::OK(); }
  Status MarkBeginPrepare(bool) override { return Status::OK(); }
  Status MarkEndPrepare(const Slice&) override { return Status::OK(); }
  Status MarkNoop(bool) override { return Status::OK(); }
  Status MarkRollback(const Slice&) override { return Status::OK(); }
  Status MarkCommit(const Slice&) override { return Status::OK(); }
  Status MarkCommitWithTimestamp(const Slice&, const Slice&) override { return Status::OK(); }
};

char Outcome(const Status& s) {
  if (s.ok()) return 'O';
  if (s.IsCorruption()) return 'C';
  return 'X';
}

int main() {
  for (int i = 0; i < 512; ++i) {
    size_t max_bytes = 0;
    if (r.next() % 8 == 0) max_bytes = 12 + r.next() % 200;
    WriteBatch b(0, max_bytes);
    const bool two_pc = r.next() % 8 == 0;
    if (two_pc) WriteBatchInternal::InsertNoop(&b);
    std::string outcomes;
    const uint64_t nops = r.next() % 10;
    for (uint64_t op = 0; op < nops; ++op) {
      const uint64_t kind = r.next() % 14;
      Status s;
      bool recorded = true;
      switch (kind) {
        case 0: {
          uint32_t cf = Cf();
          std::string k = Bytes(20);
          std::string v = Bytes(20);
          s = WriteBatchInternal::Put(&b, cf, k, v);
          break;
        }
        case 1: {
          uint32_t cf = Cf();
          std::string k = Bytes(20);
          s = WriteBatchInternal::Delete(&b, cf, k);
          break;
        }
        case 2: {
          uint32_t cf = Cf();
          std::string k = Bytes(20);
          s = WriteBatchInternal::SingleDelete(&b, cf, k);
          break;
        }
        case 3: {
          uint32_t cf = Cf();
          std::string k1 = Bytes(20);
          std::string k2 = Bytes(20);
          s = WriteBatchInternal::DeleteRange(&b, cf, k1, k2);
          break;
        }
        case 4: {
          uint32_t cf = Cf();
          std::string k = Bytes(20);
          std::string v = Bytes(20);
          s = WriteBatchInternal::Merge(&b, cf, k, v);
          break;
        }
        case 5: {
          uint32_t cf = Cf();
          std::string k = Bytes(20);
          uint64_t ncols = r.next() % 4;
          std::vector<std::string> names, values;
          for (uint64_t c = 0; c < ncols; ++c) {
            names.push_back(Bytes(2));
            values.push_back(Bytes(10));
          }
          WideColumns cols;
          for (uint64_t c = 0; c < ncols; ++c) cols.emplace_back(names[c], values[c]);
          s = WriteBatchInternal::PutEntity(&b, cf, k, cols);
          break;
        }
        case 6: {
          std::string blob = Bytes(10);
          s = b.PutLogData(blob);
          break;
        }
        case 7: {
          uint32_t cf = Cf();
          std::string k = Bytes(20);
          std::string v = Bytes(20);
          uint64_t t = std::numeric_limits<uint64_t>::max();
          if (r.next() % 4 != 0) t = r.next();
          s = WriteBatchInternal::TimedPut(&b, cf, k, v, t);
          break;
        }
        case 8: {
          uint32_t cf = Cf();
          std::string k = Bytes(20);
          uint64_t file = r.next() % 1000;
          uint64_t off = r.next() % 100000;
          uint64_t size = r.next() % 5000;
          std::string bi;
          BlobIndex::EncodeBlob(&bi, file, off, size, kNoCompression);
          s = WriteBatchInternal::PutBlobIndex(&b, cf, k, bi);
          break;
        }
        case 9:
          b.SetSavePoint();
          break;
        case 10:
          s = b.RollbackToSavePoint();
          break;
        case 11:
          s = b.PopSavePoint();
          break;
        case 12: {
          uint32_t cf = Cf();
          std::string k1 = Bytes(8), k2 = Bytes(8);
          std::string v1 = Bytes(8), v2 = Bytes(8), v3 = Bytes(8);
          Slice ks[2] = {k1, k2};
          Slice vs[3] = {v1, v2, v3};
          s = WriteBatchInternal::Put(&b, cf, SliceParts(ks, 2), SliceParts(vs, 3));
          break;
        }
        default:
          // Clear would remove the Noop MarkEndPrepare turns into BeginPrepare.
          if (two_pc) {
            recorded = false;
          } else {
            b.Clear();
          }
          break;
      }
      if (recorded) outcomes.push_back(s.ok() ? '1' : '0');
    }
    if (two_pc) {
      bool wac = r.next() % 2 == 0;
      bool unprepared = r.next() % 2 == 0;
      std::string xid = Bytes(6);
      Status s = WriteBatchInternal::MarkEndPrepare(&b, xid, wac, unprepared);
      outcomes.push_back(s.ok() ? '1' : '0');
      uint64_t tail = r.next() % 4;
      if (tail == 1) {
        s = WriteBatchInternal::MarkCommit(&b, xid);
      } else if (tail == 2) {
        s = WriteBatchInternal::MarkRollback(&b, xid);
      } else if (tail == 3) {
        std::string ts = Bytes(8) + "t";
        s = WriteBatchInternal::MarkCommitWithTimestamp(&b, xid, ts);
      }
      outcomes.push_back(s.ok() ? '1' : '0');
    }
    WriteBatchInternal::SetSequence(&b, r.next() & kMaxSequenceNumber);
    if (r.next() % 8 == 0) {
      WriteBatch c;
      std::string k1 = Bytes(8), v1 = Bytes(8);
      Status s = c.Put(k1, v1);
      c.MarkWalTerminationPoint();
      std::string k2 = Bytes(8);
      Status s2 = c.Delete(k2);
      bool wal_only = r.next() % 2 == 0;
      Status s3 = WriteBatchInternal::Append(&b, &c, wal_only);
      outcomes.push_back(s.ok() && s2.ok() && s3.ok() ? '1' : '0');
    }
    const std::string& data = b.Data();
    for (unsigned char ch : data) printf("%02x", ch);
    printf(" %s", outcomes.empty() ? "-" : outcomes.c_str());
    for (int t = 0; t < 3; ++t) {
      size_t k = data.size() + 1;
      if (data.size() > WriteBatchInternal::kHeader) {
        k = WriteBatchInternal::kHeader + r.next() % (data.size() - WriteBatchInternal::kHeader);
      } else {
        k = data.size();
      }
      WriteBatch tb(std::string(data.data(), k));
      AcceptAll h;
      printf(" %zu:%c", k, Outcome(tb.Iterate(&h)));
    }
    AcceptAll whole;
    printf(" %c\n", Outcome(b.Iterate(&whole)));
  }
  return 0;
}
