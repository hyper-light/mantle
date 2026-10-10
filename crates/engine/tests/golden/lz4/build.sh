#!/bin/sh
# Builds lz4_oracle against the reference LZ4 (L, the library's prefix) and writes the fixtures
# beside this file. Run outside the tests.
set -e
cd "$(dirname "$0")"
L=${L:?the LZ4 prefix}
clang++ -std=c++20 -O2 -I"$L/include" lz4_oracle.cc -L"$L/lib" -llz4 -o lz4_oracle
./lz4_oracle write .
rm -f lz4_oracle
