#!/usr/bin/env python3
"""Source contracts the compiler cannot state (CLAUDE.md §4, §7).

`unsafe` is denied workspace-wide; a file may opt in with `#![allow(unsafe_code)]` only if it
is listed here, and every such file is FFI to an OS interface with each block commented.

Every numeric constant in production code has a row in docs/design/constants.md saying what
kind of value it is and what establishes it, and every row names a constant that exists
(audit §12.6).
"""
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent

# Each entry: the file and the interface it binds.
UNSAFE_ALLOWED = {
    "crates/disk/src/probe/macos.rs": "IOKit and CoreFoundation (device identification)",
    "crates/disk/src/probe/windows.rs": "volume management and IOCTL_STORAGE_QUERY_PROPERTY (device identification)",
    "crates/engine/src/port/mmap.rs": "mmap/munmap and CreateFileMappingW/MapViewOfFile (the memtable arena's lazily zeroed blocks)",
}

INVENTORY = ROOT / "docs/design/constants.md"
KINDS = {"format", "external", "derived", "cited", "measured", "bound", "open"}
# Rows marked open may number at most this; it only falls, as open constants gain a basis or
# leave mantle's production code.
OPEN_CEILING = 9
NUMERIC = {
    "u8", "u16", "u32", "u64", "u128", "usize",
    "i8", "i16", "i32", "i64", "i128", "isize",
    "f32", "f64", "Duration",
    # Integer aliases of OS interfaces, and the word type of the CRC macro.
    "Opcode", "CFIndex", "MachPort", "$word",
}
CONST = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?const\s+([A-Z_][A-Z0-9_]*)\s*:\s*([$A-Za-z0-9_:]+)\s*="
)
ROW = re.compile(r"^\|\s*`([^`]+)`\s+`([^`]+)`\s*\|\s*([a-z]+)\s*\|")
TEST_MODULE = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+\w+")

ALLOW_UNSAFE = re.compile(r"allow\s*\(\s*unsafe_code\s*\)")
UNSAFE_BLOCK = re.compile(r"\bunsafe\s*(\{|fn\b|extern\b|impl\b)")


def production_constants():
    """Every numeric constant outside test modules in crates/*/src, as (file, name)."""
    found = []
    for path in sorted(ROOT.glob("crates/*/src/**/*.rs")):
        rel = path.relative_to(ROOT).as_posix()
        lines = path.read_text(encoding="utf-8").splitlines()
        for i, line in enumerate(lines):
            # A test module ends what is production: the rest of the file is its body.
            if line.strip() == "#[cfg(test)]":
                after = next((l for l in lines[i + 1:] if l.strip() and not l.strip().startswith("#[")), "")
                if TEST_MODULE.match(after):
                    break
            m = CONST.match(line)
            if m and m.group(2).split("::")[-1] in NUMERIC:
                found.append((rel, m.group(1)))
    return found


def check_constants():
    failures = []
    if not INVENTORY.exists():
        return [f"{INVENTORY.relative_to(ROOT)}: missing"]
    rows = {}
    for n, line in enumerate(INVENTORY.read_text(encoding="utf-8").splitlines(), 1):
        m = ROW.match(line)
        if not m:
            continue
        key = (m.group(1), m.group(2))
        if m.group(3) not in KINDS:
            failures.append(f"constants.md:{n}: {key[0]} {key[1]}: unknown kind {m.group(3)}")
        rows[key] = m.group(3)
    open_rows = sum(1 for kind in rows.values() if kind == "open")
    if open_rows > OPEN_CEILING:
        failures.append(
            f"docs/design/constants.md: {open_rows} open constants, more than {OPEN_CEILING}"
        )
    found = set(production_constants())
    for rel, name in sorted(found - set(rows)):
        failures.append(f"{rel}: {name} has no row in docs/design/constants.md")
    for rel, name in sorted(set(rows) - found):
        failures.append(f"docs/design/constants.md: {rel} {name} names no constant")
    return failures


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
    failures.extend(check_constants())
    for failure in failures:
        print(failure, file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
