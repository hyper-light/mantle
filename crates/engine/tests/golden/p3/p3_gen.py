#!/usr/bin/env python3
"""Golden fixtures for mantle-engine P3's differential (docs/design/engine.md §8): RocksDB 11.8.1's
own log writer and reader (p3_oracle, built by build.sh) run over scripts this program writes, and
over every mutation of each WAL that `mutations` names, in each recovery mode, plain and
fragment-buffered.

Writes, beside itself:
  scripts/<name>.txt     the scripts (`record_rep` records, so they stay small);
  wal/<name>.log         the oracle's WALs that are ZSTD-compressed: the port's encoder writes
                         other bytes for the same content, so its reader reads the oracle's;
  p3.txt                 `write <name> <fnv>` for each uncompressed WAL the oracle wrote, and
                         `read <name> <mutation> <mode> <retry> <args> <fnv|hang>` for each read:
                         the FNV-1a 64 digest of the oracle's transcript, or `hang` where the
                         oracle's reader can never return: the fixed point shim/util/coding_lean.h
                         detects (exit 3), never a clock.
tests/p3_differential.rs writes the same scripts with the port, checks each uncompressed WAL's
digest, reads every mutation the same way, and checks each transcript's digest.

Usage: p3_gen.py ORACLE
"""
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ORACLE = sys.argv[1]
BLOCK = 32768
# A bound on each oracle run, not a verdict: every read ends or reports its fixed point within
# milliseconds, so reaching it is a failure of the generator (CLAUDE.md §2), never a fixture.
TIMEOUT = 20
MASK = (1 << 64) - 1


class SplitMix:
    def __init__(self, seed):
        self.x = seed & MASK

    def next(self):
        self.x = (self.x + 0x9E3779B97F4A7C15) & MASK
        z = self.x
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & MASK
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK
        return z ^ (z >> 31)

    def below(self, n):
        return self.next() % n


def fnv(data):
    h = 0xCBF29CE484222325
    for b in data:
        h = ((h ^ b) * 0x100000001B3) & MASK
    return f"{h:016x}"


def script(seed, log, recycle, comp, track, reuse=0, n=300):
    """A script of `n` records: lengths of every size class, block-spanning ones among them, and
    the meta records the writer can add."""
    r = SplitMix(seed)
    lines = [f"options {log} {recycle} {comp} {track} {reuse}"]
    if comp:
        lines.append("compression_record")
    if track:
        lines.append("pred 41 123456 777")
    for i in range(n):
        k = r.below(16)
        if k == 0:
            length = 0
        elif k < 9:
            length = 1 + r.below(100)
        elif k < 14:
            length = 100 + r.below(5000)
        else:
            length = BLOCK - 20 + r.below(3 * BLOCK)
        pattern = "".join(f"{r.below(256):02x}" for _ in range(1 + r.below(8)))
        lines.append(f"record_rep {length} {pattern}")
        if r.below(40) == 0:
            # A column family's timestamp size is fixed while the DB runs: RocksDB's writer
            # asserts it (db/log_writer.cc:279-282), the port refuses a change.
            cf = r.below(4)
            lines.append(f"ts {cf} {8 * (1 + cf % 2)}")
        if r.below(25) == 0:
            lines.append("flush")
    return lines


def mutations(n):
    """The mutations of an n-byte WAL, each `(name, kind, at, extra)`: truncations at each block
    boundary, one byte either side of it, and past a header; 16 strided bit flips; zeroed spans
    at the first four blocks' starts. tests/p3_differential.rs computes the same list."""
    out = [("whole", "none", 0, 0)]
    b = 0
    while b < n:
        for d in (-1, 0, 1, 7):
            at = b + d
            if 0 < at < n:
                out.append((f"cut{at}", "cut", at, 0))
        b += BLOCK
    for i in range(16):
        at = (i * 2654435761) % n if n else 0
        out.append((f"flip{at}.{i % 8}", "flip", at, i % 8))
    for k in range(4):
        at = k * BLOCK
        if at < n:
            out.append((f"zero{at}", "zero", at, 11))
    return out


def mutate(data, kind, at, extra):
    if kind == "cut":
        return data[:at]
    b = bytearray(data)
    if kind == "flip":
        b[at] ^= 1 << extra
    elif kind == "zero":
        b[at:at + extra] = bytes(len(b[at:at + extra]))
    return bytes(b)


def run(args):
    p = subprocess.run(args, capture_output=True, timeout=TIMEOUT)
    return p.returncode, p.stdout


CONFIGS = [
    # name, seed, log, recycle, compression, track
    ("legacy", 1, 9, 0, 0, 0),
    ("recycle", 2, 9, 1, 0, 0),
    ("track", 3, 9, 0, 0, 1),
    ("track_recycle", 4, 9, 1, 0, 1),
    ("biglog", 5, (1 << 32) | 9, 1, 0, 0),
    ("zstd", 6, 9, 0, 7, 0),
    ("zstd_recycle", 7, 9, 1, 7, 0),
]


def main():
    os.makedirs(os.path.join(HERE, "scripts"), exist_ok=True)
    os.makedirs(os.path.join(HERE, "wal"), exist_ok=True)
    tmp = os.path.join(HERE, ".tmp")
    os.makedirs(tmp, exist_ok=True)
    out = []
    wals = []
    for name, seed, log, recycle, comp, track in CONFIGS:
        s = os.path.join(HERE, "scripts", name + ".txt")
        open(s, "w").write("\n".join(script(seed, log, recycle, comp, track)) + "\n")
        w = os.path.join(tmp, name + ".log")
        assert run([ORACLE, "write", s, w])[0] == 0, name
        data = open(w, "rb").read()
        if comp:
            open(os.path.join(HERE, "wal", name + ".log"), "wb").write(data)
        else:
            out.append(f"write {name} {fnv(data)}")
        wals.append((name, data, log, track))
    # A recycled file: a longer log 8 overwritten from its start by a shorter log 9.
    s1 = os.path.join(HERE, "scripts", "reused.1.txt")
    s2 = os.path.join(HERE, "scripts", "reused.2.txt")
    open(s1, "w").write("\n".join(script(8, 8, 1, 0, 0, n=600)) + "\n")
    open(s2, "w").write("\n".join(script(9, 9, 1, 0, 0, reuse=1, n=150)) + "\n")
    w = os.path.join(tmp, "reused.log")
    if os.path.exists(w):
        os.remove(w)
    assert run([ORACLE, "write", s1, w])[0] == 0
    assert run([ORACLE, "write", s2, w])[0] == 0
    data = open(w, "rb").read()
    out.append(f"write reused {fnv(data)}")
    wals.append(("reused", data, 9, 0))
    for name, data, log, track in wals:
        track_args = ["0", "41", "123456", "777"] if track else []
        variants = [("-", track_args)]
        if track:
            variants += [("mismatch", ["0", "41", "123456", "778"]), ("missing", ["40", "0", "0", "0"])]
        for mname, kind, at, extra in mutations(len(data)):
            m = os.path.join(tmp, "m.log")
            open(m, "wb").write(mutate(data, kind, at, extra))
            for vname, targs in variants:
                if vname != "-" and mname != "whole":
                    continue
                for retry in (0, 1):
                    if retry and targs:
                        continue
                    for mode in (0, 1, 2, 3):
                        # The writer stores a log number's low 32 bits and RocksDB's readers compare
                        # all 64, losing every record of a recycled log numbered 2^32 or more; the
                        # port compares the low 32. The oracle is asked with the number it can
                        # match, which is the reading the port's must equal.
                        oracle_log = log & 0xFFFFFFFF
                        code, stdout = run([ORACLE, "read", m, str(oracle_log), str(mode), str(retry), *targs])
                        assert code in (0, 3), (name, mname, mode, retry, code)
                        digest = "hang" if code == 3 else fnv(stdout)
                        out.append(f"read {name} {mname} {vname} {mode} {retry} {digest}")
    open(os.path.join(HERE, "p3.txt"), "w").write("\n".join(out) + "\n")
    for f in os.listdir(tmp):
        os.remove(os.path.join(tmp, f))
    os.rmdir(tmp)


if __name__ == "__main__":
    main()
