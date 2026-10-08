#!/usr/bin/env python3
"""Compare Mantle's benchmark operation/key stream with RocksDB's C++ generator.

Requires rustc and a C++ compiler. This checks output through several MT state twists,
including zero and large seeds, rather than asserting implementation state.
"""
import argparse
import pathlib
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rocksdb-source", type=pathlib.Path, required=True)
    args = parser.parse_args()
    root = pathlib.Path(__file__).resolve().parent.parent
    support = root / "crates/engine/benches/support/rocks_workload.rs"
    with tempfile.TemporaryDirectory(prefix="mantle-workload-") as directory:
        work = pathlib.Path(directory)
        (work / "oracle.cc").write_text(r'''
#include "util/random.h"
#include <cstdio>
#include <cstdint>
int main() {
  for (uint64_t seed : {uint64_t{0}, uint64_t{301}, UINT64_MAX - 3}) {
    for (uint64_t phase = 1; phase <= 3; ++phase) {
      rocksdb::Random64 rng(seed + phase);
      for (int i = 0; i < 1000; ++i) {
        if (phase != 3) rng.Next(); // DoWrite/SelectDBWithCfh database choice
        const uint64_t n = rng.Next() % 10000000;
        std::printf("%llu %llu ", (unsigned long long)seed,
                    (unsigned long long)phase);
        // GenerateKeyFromInt: big-endian prefix followed by ASCII '0'.
        for (int b = 7; b >= 0; --b) std::printf("%02x", unsigned((n >> (b * 8)) & 255));
        std::puts("3030303030303030");
      }
    }
  }
}
''')
        (work / "probe.rs").write_text(
            '#[path = ' + repr(str(support)).replace("'", '"') + '] mod workload;\n'
            + '''fn main() {
    for seed in [0, 301, u64::MAX - 3] {
        for phase in 1..=3 {
            let mut rng = workload::Rng::new(seed, phase);
            for _ in 0..1000 {
                print!("{seed} {phase} ");
                for byte in workload::key(rng.next() % 10_000_000) {
                    print!("{byte:02x}");
                }
                println!();
            }
        }
    }
}\n'''
        )
        subprocess.run(["c++", "-std=c++17", "-I", str(args.rocksdb_source),
                        "-I", str(args.rocksdb_source / "include"),
                        str(work / "oracle.cc"), "-o", str(work / "oracle")], check=True)
        subprocess.run(["rustc", "--edition=2024", str(work / "probe.rs"),
                        "-o", str(work / "probe")], check=True)
        expected = subprocess.check_output([str(work / "oracle")])
        actual = subprocess.check_output([str(work / "probe")])
        if expected != actual:
            raise SystemExit("benchmark operation/key stream differs from RocksDB oracle")
        print("9000 operation/key outputs match RocksDB Random64 and db_bench's draw schedule")


if __name__ == "__main__":
    main()
