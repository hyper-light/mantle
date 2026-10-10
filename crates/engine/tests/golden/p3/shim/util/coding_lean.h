// Shim over RocksDB's util/coding_lean.h: the real header, then DecodeFixed32 routed through a
// watch for the one fixed point db/log_reader.cc can reach. Not part of RocksDB.
//
// FragmentBufferedReader::TryReadFragment (log_reader.cc:999-1002 at v11.8.1) returns kOldRecord
// for a recyclable record of another log without consuming it, unlike Reader (:604-607), so
// ReadRecord's loop (:870) asks for the same fragment again with nothing changed, forever.
// Within one buffered block each header the readers decode lies past the last, and a recyclable
// header's log number (header + 7) is followed by its checksum (header); the same address comes
// back only from a new block read into the same backing store, which the shim's
// SequentialFileReader counts. So the same address decoded twice with no read between is that
// fixed point, and only that: the reader will never return. The oracle reports it as its own
// outcome (exit 3) rather than being stopped by a clock.
#pragma once
#include_next "util/coding_lean.h"
#include <cstdio>
#include <cstdlib>

namespace ROCKSDB_NAMESPACE {
// Reads the shim's SequentialFileReader has made.
inline uint64_t& P3Reads() {
  static uint64_t reads = 0;
  return reads;
}
inline uint32_t P3WatchedDecodeFixed32(const char* ptr) {
  static const char* last = nullptr;
  static uint64_t last_reads = 0;
  if (ptr == last && P3Reads() == last_reads) {
    fflush(stdout);
    _Exit(3);
  }
  last = ptr;
  last_reads = P3Reads();
  return DecodeFixed32(ptr);
}
}  // namespace ROCKSDB_NAMESPACE
#define DecodeFixed32(ptr) P3WatchedDecodeFixed32(ptr)
