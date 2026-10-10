// Shim for RocksDB's file/sequence_file_reader.h: the members db/log_reader.cc calls, over a
// FILE*. Not part of RocksDB.
#pragma once
#include <cstdio>
#include <string>
#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/slice.h"
#include "util/coding_lean.h"
namespace ROCKSDB_NAMESPACE {
class SequentialFileReader {
 public:
  SequentialFileReader(FILE* f, std::string name) : f_(f), name_(std::move(name)) {}
  ~SequentialFileReader() { fclose(f_); }
  IOStatus Read(size_t n, Slice* result, char* scratch, Env::IOPriority) {
    ++P3Reads();
    size_t r = fread(scratch, 1, n, f_);
    *result = Slice(scratch, r);
    return ferror(f_) ? IOStatus::IOError("fread") : IOStatus::OK();
  }
  const std::string& file_name() const { return name_; }
 private:
  FILE* f_;
  std::string name_;
};
}  // namespace ROCKSDB_NAMESPACE
