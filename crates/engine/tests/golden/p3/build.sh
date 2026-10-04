#!/bin/sh
# Builds p3_oracle from RocksDB 11.8.1's unmodified sources (R, at abeebd963) and the shim beside
# this file, linking the reference libzstd (Z) for the compressed WALs. Run outside the tests.
set -e
cd "$(dirname "$0")"
R=${R:?the RocksDB checkout}
Z=${Z:?the libzstd prefix}
clang++ -std=c++20 -O1 -DNDEBUG -march=armv8-a+crc+crypto -Ishim -I"$R" -I"$R/include" -I"$Z/include" \
  -DROCKSDB_PLATFORM_POSIX -DOS_MACOSX -DZSTD \
  p3_oracle.cc "$R/db/log_writer.cc" "$R/db/log_reader.cc" "$R/util/crc32c.cc" "$R/util/crc32c_arm64.cc" \
  "$R/util/xxhash.cc" "$R/util/coding.cc" "$R/util/status.cc" \
  -L"$Z/lib" -lzstd -o p3_oracle "$@"
