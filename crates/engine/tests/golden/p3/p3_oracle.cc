// The P3 oracle: RocksDB 11.8.1's own db/log_writer.cc and db/log_reader.cc, unmodified,
// linked with a file-layer shim (shim/file/*.h), writing a log from a script and reading a
// log in a given recovery mode. mantle-engine's example p3_wal prints the same lines.
//
//   p3_oracle write SCRIPT OUT
//   p3_oracle read FILE LOG_NUMBER MODE RETRY [MIN_KEEP OBS_LOG OBS_SIZE OBS_SEQ]
//
// Script lines: "options LOG RECYCLE COMPRESSION(0|7) TRACK [REUSE]", "compression_record",
// "record HEX|-", "record_rep N HEXPATTERN", "ts CF SZ", "pred LOG SIZE SEQ", "flush".
#include <cinttypes>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>

#include "db/log_reader.h"
#include "db/log_writer.h"
#include "file/writable_file_writer.h"
#include "util/crc32c.h"

using namespace ROCKSDB_NAMESPACE;

// The thread-status bookkeeping log::Writer's destructor touches; nothing here reads it.
namespace ROCKSDB_NAMESPACE {
ThreadStatus::OperationType ThreadStatusUtil::GetThreadOperation() {
  return ThreadStatus::OP_UNKNOWN;
}
void ThreadStatusUtil::SetThreadOperation(ThreadStatus::OperationType) {}
// util/slice.cc links the options framework; its Slice::ToString, from :282-296.
std::string Slice::ToString(bool hex) const {
  static const char kHex[] = "0123456789ABCDEF";
  std::string result;
  if (hex) {
    for (size_t i = 0; i < size_; ++i) {
      unsigned char c = data_[i];
      result.push_back(kHex[c >> 4]);
      result.push_back(kHex[c & 0xf]);
    }
    return result;
  }
  result.assign(data_, size_);
  return result;
}
}  // namespace ROCKSDB_NAMESPACE

// Compression.cc links most of RocksDB; its ZSTD streaming classes, copied verbatim from
// util/compression.cc:158-266 at v11.8.1.
namespace ROCKSDB_NAMESPACE {
std::unique_ptr<StreamingCompress> StreamingCompress::Create(
    CompressionType compression_type, const CompressionOptions& opts,
    uint32_t compress_format_version, size_t max_output_len) {
  switch (compression_type) {
    case kZSTD: {
      if (!ZSTD_Streaming_Supported()) {
        return nullptr;
      }
      return std::make_unique<ZSTDStreamingCompress>(
          opts, compress_format_version, max_output_len);
    }
    default:
      return nullptr;
  }
}

std::unique_ptr<StreamingUncompress> StreamingUncompress::Create(
    CompressionType compression_type, uint32_t compress_format_version,
    size_t max_output_len) {
  switch (compression_type) {
    case kZSTD: {
      if (!ZSTD_Streaming_Supported()) {
        return nullptr;
      }
      return std::make_unique<ZSTDStreamingUncompress>(compress_format_version,
                                                       max_output_len);
    }
    default:
      return nullptr;
  }
}

int ZSTDStreamingCompress::Compress(const char* input, size_t input_size,
                                    char* output, size_t* output_pos) {
  assert(input != nullptr && output != nullptr && output_pos != nullptr);
  *output_pos = 0;
  // Don't need to compress an empty input
  if (input_size == 0) {
    return 0;
  }
  if (input_buffer_.src == nullptr || input_buffer_.src != input) {
    // New input
    // Catch errors where the previous input was not fully decompressed.
    assert(input_buffer_.pos == input_buffer_.size);
    input_buffer_ = {input, input_size, /*pos=*/0};
  } else if (input_buffer_.src == input) {
    // Same input, not fully compressed.
  }
  ZSTD_outBuffer output_buffer = {output, max_output_len_, /*pos=*/0};
  const size_t remaining =
      ZSTD_compressStream2(cctx_, &output_buffer, &input_buffer_, ZSTD_e_end);
  if (ZSTD_isError(remaining)) {
    // Failure
    Reset();
    return -1;
  }
  // Success
  *output_pos = output_buffer.pos;
  return (int)remaining;
}

void ZSTDStreamingCompress::Reset() {
  ZSTD_CCtx_reset(cctx_, ZSTD_ResetDirective::ZSTD_reset_session_only);
  input_buffer_ = {/*src=*/nullptr, /*size=*/0, /*pos=*/0};
}

int ZSTDStreamingUncompress::Uncompress(const char* input, size_t input_size,
                                        char* output, size_t* output_pos) {
  assert(output != nullptr && output_pos != nullptr);
  *output_pos = 0;
  // Don't need to uncompress an empty input
  if (input_size == 0) {
    return 0;
  }
  if (input) {
    // New input
    input_buffer_ = {input, input_size, /*pos=*/0};
  }
  ZSTD_outBuffer output_buffer = {output, max_output_len_, /*pos=*/0};
  size_t ret = ZSTD_decompressStream(dctx_, &output_buffer, &input_buffer_);
  if (ZSTD_isError(ret)) {
    Reset();
    return -1;
  }
  *output_pos = output_buffer.pos;
  return (int)(input_buffer_.size - input_buffer_.pos);
}

void ZSTDStreamingUncompress::Reset() {
  ZSTD_DCtx_reset(dctx_, ZSTD_ResetDirective::ZSTD_reset_session_only);
  input_buffer_ = {/*src=*/nullptr, /*size=*/0, /*pos=*/0};
}
}  // namespace ROCKSDB_NAMESPACE

static std::string Unhex(const std::string& h) {
  std::string out;
  if (h == "-") return out;
  for (size_t i = 0; i + 1 < h.size(); i += 2) {
    out.push_back(static_cast<char>(std::stoi(h.substr(i, 2), nullptr, 16)));
  }
  return out;
}

struct PrintReporter : public log::Reader::Reporter {
  void Corruption(size_t bytes, const Status& s, uint64_t log_number) override {
    printf("drop %zu %s", bytes, s.ToString().c_str());
    if (log_number != kMaxSequenceNumber) printf(" log=%" PRIu64, log_number);
    printf("\n");
  }
  void OldLogRecord(size_t bytes) override { printf("old %zu\n", bytes); }
};

static int Write(const char* script, const char* out) {
  std::ifstream in(script);
  std::string line;
  std::unique_ptr<log::Writer> w;
  bool track = false;
  WriteOptions wo;
  while (std::getline(in, line)) {
    std::istringstream ls(line);
    std::string op;
    ls >> op;
    IOStatus s;
    if (op == "options") {
      uint64_t log;
      int recycle, comp, tr, reuse = 0;
      ls >> log >> recycle >> comp >> tr >> reuse;
      track = tr != 0;
      // reuse: overwrite an existing file from its start, as ReuseWritableFile does.
      FILE* f = fopen(out, reuse ? "r+b" : "wb");
      w.reset(new log::Writer(std::make_unique<WritableFileWriter>(f), log,
                              recycle != 0, false,
                              static_cast<CompressionType>(comp), track));
    } else if (op == "compression_record") {
      s = w->AddCompressionTypeRecord(wo);
    } else if (op == "record") {
      std::string h;
      ls >> h;
      s = w->AddRecord(wo, Unhex(h));
    } else if (op == "record_rep") {
      size_t n;
      std::string h;
      ls >> n >> h;
      std::string pat = Unhex(h), rec;
      while (rec.size() < n) rec += pat;
      rec.resize(n);
      s = w->AddRecord(wo, rec);
    } else if (op == "ts") {
      uint32_t cf;
      size_t sz;
      ls >> cf >> sz;
      UnorderedMap<uint32_t, size_t> m;
      m.emplace(cf, sz);
      s = w->MaybeAddUserDefinedTimestampSizeRecord(wo, m);
    } else if (op == "pred") {
      uint64_t a, b, c;
      ls >> a >> b >> c;
      s = w->MaybeAddPredecessorWALInfo(wo, PredecessorWALInfo(a, b, c));
    } else if (op == "flush") {
      s = w->WriteBuffer(wo);
    }
    if (!s.ok()) {
      fprintf(stderr, "%s: %s\n", line.c_str(), s.ToString().c_str());
      return 1;
    }
  }
  return w->Close(wo).ok() ? 0 : 1;
}

static int Read(int argc, char** argv) {
  const char* path = argv[2];
  uint64_t log = strtoull(argv[3], nullptr, 10);
  auto mode = static_cast<WALRecoveryMode>(atoi(argv[4]));
  bool retry = atoi(argv[5]) != 0;
  bool track = argc >= 10;
  uint64_t min_keep = track ? strtoull(argv[6], nullptr, 10) : UINT64_MAX;
  PredecessorWALInfo observed;
  if (track && strtoull(argv[7], nullptr, 10) != 0) {
    observed = PredecessorWALInfo(strtoull(argv[7], nullptr, 10),
                                  strtoull(argv[8], nullptr, 10),
                                  strtoull(argv[9], nullptr, 10));
  }
  FILE* f = fopen(path, "rb");
  if (!f) return 2;
  auto file = std::make_unique<SequentialFileReader>(f, path);
  PrintReporter rep;
  std::unique_ptr<log::Reader> r;
  if (retry) {
    r.reset(new log::FragmentBufferedReader(nullptr, std::move(file), &rep, true, log));
  } else {
    r.reset(new log::Reader(nullptr, std::move(file), &rep, true, log, track, false,
                            min_keep, observed));
  }
  std::string scratch;
  Slice record;
  while (r->ReadRecord(&record, &scratch, mode)) {
    printf("record %" PRIu64 " %zu %08x\n", r->LastRecordOffset(), record.size(),
           crc32c::Value(record.data(), record.size()));
  }
  std::map<uint32_t, size_t> ts(r->GetRecordedTimestampSize().begin(),
                                r->GetRecordedTimestampSize().end());
  for (auto& [cf, sz] : ts) printf("ts %u %zu\n", cf, sz);
  printf("end eof=%d\n", r->IsEOF() ? 1 : 0);
  return 0;
}

int main(int argc, char** argv) {
  if (argc >= 4 && std::string(argv[1]) == "write") return Write(argv[2], argv[3]);
  if (argc >= 6 && std::string(argv[1]) == "read") return Read(argc, argv);
  fprintf(stderr, "usage: see p3_oracle.cc\n");
  return 2;
}
