#!/usr/bin/env python3
"""Source contracts the compiler cannot state (CLAUDE.md §7).

`unsafe` is denied workspace-wide; a file may opt in with `#![allow(unsafe_code)]` only if it
is listed here, and every such file is FFI to an OS interface with each block commented.
"""
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent

# Each entry: the file and the interface it binds.
UNSAFE_ALLOWED = {
    "crates/disk/src/probe/macos.rs": "IOKit and CoreFoundation (device identification)",
}

ALLOW_UNSAFE = re.compile(r"allow\s*\(\s*unsafe_code\s*\)")
UNSAFE_BLOCK = re.compile(r"\bunsafe\s*(\{|fn\b|extern\b|impl\b)")


def main() -> int:
    failures = []
    for path in sorted(ROOT.glob("crates/**/*.rs")):
        rel = path.relative_to(ROOT).as_posix()
        text = path.read_text(encoding="utf-8")
        if rel in UNSAFE_ALLOWED:
            continue
        if ALLOW_UNSAFE.search(text):
            failures.append(f"{rel}: allows unsafe_code but is not in UNSAFE_ALLOWED")
        elif UNSAFE_BLOCK.search(text):
            failures.append(f"{rel}: uses unsafe but is not in UNSAFE_ALLOWED")
    for rel in UNSAFE_ALLOWED:
        if not (ROOT / rel).exists():
            failures.append(f"{rel}: listed in UNSAFE_ALLOWED but missing")
    for failure in failures:
        print(failure, file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
