// Shim for RocksDB's file/writable_file_writer.h: the members db/log_writer.cc calls, over
// an in-process byte string flushed to a FILE*. Not part of RocksDB.
#pragma once
#include <cstdio>
#include <string>
#include "monitoring/thread_status_util.h"
#include "test_util/sync_point.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
namespace ROCKSDB_NAMESPACE {
class WritableFileWriter {
 public:
  explicit WritableFileWriter(FILE* f) : f_(f) {}
  IOStatus Append(const IOOptions&, const Slice& data, uint32_t = 0) {
    buf_.append(data.data(), data.size());
    return IOStatus::OK();
  }
  IOStatus Flush(const IOOptions&) {
    if (!buf_.empty() && fwrite(buf_.data(), 1, buf_.size(), f_) != buf_.size())
      return IOStatus::IOError("fwrite");
    buf_.clear();
    fflush(f_);
    return IOStatus::OK();
  }
  IOStatus Close(const IOOptions& o) { IOStatus s = Flush(o); fclose(f_); closed_ = true; return s; }
  static IOStatus PrepareIOOptions(const WriteOptions&, IOOptions&) { return IOStatus::OK(); }
  bool seen_error() const { return false; }
  bool IsClosed() const { return closed_; }
  bool BufferIsEmpty() const { return buf_.empty(); }
 private:
  FILE* f_;
  std::string buf_;
  bool closed_ = false;
};
}  // namespace ROCKSDB_NAMESPACE
