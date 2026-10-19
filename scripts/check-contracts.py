#!/usr/bin/env python3
"""Source contracts the compiler cannot state (CLAUDE.md §4, §7).

`unsafe` is denied workspace-wide; a file may opt in with `#![allow(unsafe_code)]` only if it
is listed here, and every such file is FFI to an OS interface with each block commented.

Every numeric constant in production code has a row in docs/design/constants.md saying what
kind of value it is and what establishes it, and every row names a constant that exists
(audit §12.6). Test code is not production code: an inline `#[cfg(test)]` module's body, and a
file whose first item is the inner attribute `#![cfg(test)]`, which rustc compiles only in test
builds however the file is included. Each run first checks that rule on small fixture trees
(`fixtures`).
"""
import os
import pathlib
import re
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent.parent

# Each entry: the file and the interface it binds.
UNSAFE_ALLOWED = {
    "crates/disk/src/probe/macos.rs": "IOKit and CoreFoundation (device identification)",
    "crates/disk/src/probe/windows.rs": "volume management and IOCTL_STORAGE_QUERY_PROPERTY (device identification)",
    "crates/engine/src/port/mmap.rs": "mmap/munmap and CreateFileMappingW/MapViewOfFile (the memtable arena's lazily zeroed blocks); glibc's mallopt (freed large blocks back to the OS)",
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
# A dedicated test file's proof: its first item is `#![cfg(test)]`.
TEST_FILE = re.compile(r"^\s*#!\[cfg\(test\)\]\s*$")

ALLOW_UNSAFE = re.compile(r"allow\s*\(\s*unsafe_code\s*\)")
UNSAFE_BLOCK = re.compile(r"\bunsafe\s*(\{|fn\b|extern\b|impl\b)")


def test_only_files():
    """The files under crates/*/src whose first item, after comments and blank lines, is
    `#![cfg(test)]`: rustc compiles such a file only in test builds wherever it is included, and an
    include macro refuses its inner attribute, so its constants are test code. Every other file's
    are production code, whatever declares it."""
    found = set()
    for path in ROOT.glob("crates/*/src/**/*.rs"):
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip() or line.lstrip().startswith("//"):
                continue
            if TEST_FILE.match(line):
                found.add(path.resolve())
            break
    return found


# The test-only rule on small trees, each with whether an uninventoried constant in it must be
# refused: a test-only file's is not; one a production form also reaches is.
FIXTURES = {
    "ordinary_production": ({"lib.rs": "const MISSED: u64 = 1;\n"}, True),
    "marked_test_file": (
        {
            "lib.rs": '#[cfg(test)]\n#[path = "checks.rs"]\nmod checks;\n',
            "checks.rs": "#![cfg(test)]\nconst MISSED: u64 = 1;\n",
        },
        False,
    ),
    "marked_after_its_docs": (
        {
            "lib.rs": "mod checks;\n",
            "checks.rs": "//! Checks.\n\n#![cfg(test)]\nconst MISSED: u64 = 1;\n",
        },
        False,
    ),
    # A test declaration alone proves nothing: another importer may name the same file.
    "unmarked_test_declaration": (
        {
            "lib.rs": '#[cfg(test)]\n#[path = "shared.rs"]\nmod checks;\n',
            "shared.rs": "const MISSED: u64 = 1;\n",
        },
        True,
    ),
    "unmarked_shared_include": (
        {
            "lib.rs": 'include !("shared.rs");\n#[cfg(test)]\n#[path = "shared.rs"]\nmod checks;\n',
            "shared.rs": "const MISSED: u64 = 1;\n",
        },
        True,
    ),
    "marker_not_first": (
        {"lib.rs": "mod checks;\n", "checks.rs": "const MISSED: u64 = 1;\n#![cfg(test)]\n"},
        True,
    ),
    "a_different_inner_attribute": (
        {"lib.rs": "mod checks;\n", "checks.rs": "#![cfg(unix)]\nconst MISSED: u64 = 1;\n"},
        True,
    ),
}


def fixtures():
    """Runs this checker on each fixture tree: the failures where it refuses a constant it must
    not, or passes one it must refuse."""
    failures = []
    with tempfile.TemporaryDirectory(prefix="check-contracts-") as directory:
        for name, (files, refuse) in FIXTURES.items():
            root = pathlib.Path(directory) / name
            tree = {
                "scripts/check-contracts.py": pathlib.Path(__file__).read_text(encoding="utf-8"),
                "docs/design/constants.md": "",
                **{rel: "" for rel in UNSAFE_ALLOWED},
                **{f"crates/fixture/src/{rel}": text for rel, text in files.items()},
            }
            for rel, text in tree.items():
                (root / rel).parent.mkdir(parents=True, exist_ok=True)
                (root / rel).write_text(text, encoding="utf-8")
            run = subprocess.run(
                [sys.executable, str(root / "scripts/check-contracts.py")],
                capture_output=True,
                text=True,
                timeout=60,
                env={**os.environ, "CHECK_CONTRACTS_FIXTURE": "1"},
            )
            # A refusal exits 1 naming the constant; a pass exits 0 with nothing to say.
            if refuse:
                ok = run.returncode == 1 and "MISSED has no row" in run.stderr
            else:
                ok = run.returncode == 0 and not run.stderr.strip()
            if not ok:
                failures.append(
                    f"scripts/check-contracts.py: fixture {name}: "
                    + ("did not refuse an uninventoried production constant" if refuse else "did not pass a test-only constant")
                    + f" (exit {run.returncode}: {run.stderr.strip()})"
                )
    return failures


def production_constants():
    """Every numeric constant outside test code in crates/*/src, as (file, name)."""
    found = []
    skip = test_only_files()
    for path in sorted(ROOT.glob("crates/*/src/**/*.rs")):
        if path.resolve() in skip:
            continue
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
    # A fixture tree's own run checks its tree alone.
    failures = [] if os.environ.get("CHECK_CONTRACTS_FIXTURE") else fixtures()
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
