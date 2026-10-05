#!/bin/sh
# Builds deflate_oracle against the system's zlib and writes the fixtures beside this file. Run
# outside the tests.
set -e
cd "$(dirname "$0")"
clang++ -std=c++20 -O2 deflate_oracle.cc -lz -o deflate_oracle
./deflate_oracle write .
rm -f deflate_oracle
