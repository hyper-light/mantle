#!/bin/sh
# Builds snappy_oracle against the reference Snappy (S, the library's prefix) and writes the
# fixtures beside this file. Run outside the tests.
set -e
cd "$(dirname "$0")"
S=${S:?the Snappy prefix}
clang++ -std=c++20 -O2 -I"$S/include" snappy_oracle.cc -L"$S/lib" -lsnappy -o snappy_oracle
./snappy_oracle write .
rm -f snappy_oracle
