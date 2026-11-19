#!/usr/bin/env python3
"""Call-graph cycles in the built code (CLAUDE.md §10): no function of mantle's calls itself,
directly or through others.

The code read is what the pinned toolchain builds: every workspace crate's library and the
`mantle` binary, without cfg(test), tests/, benches/ or examples/, with every feature, as LLVM IR
after rustc's own -O0 pipeline. That is a dev build, which inlines nothing but `#[inline(always)]`:
such a function's calls become its caller's, so a cycle through one stays a cycle, named by the
functions it was inlined into (zstd's `copy_match` shows as `copy_from_dictionary` calling
itself). The build has its own target directory, target/check-recursion, and cargo runs this file
as its RUSTC_WORKSPACE_WRAPPER, which adds `--emit=llvm-ir -C codegen-units=1` to the workspace's
compiles alone: one .ll a crate, found by the stem of the outputs cargo reports, dependencies built
as usual. A changed source rebuilds its crate and the crates above it, as any build does; this
file's own flags are not part of cargo's fingerprint, so after changing them, remove
target/check-recursion. The wrapper is a script cargo runs as a program, which Windows cannot do.

Each `define` is a function and each direct `call` or `invoke` an edge. Symbols are rustc's v0
mangling, the pinned toolchain's default (the legacy `_ZN..17h<hash>E` scheme needs
`-Z unstable-options` on a stable compiler), read here without crate disambiguators. A function is
mantle's when the item it instantiates is defined in a workspace crate (a method, a closure, a
trait impl, a derive), and a drop glue shim is mantle's when the type it drops names a workspace
type: a recursive type's glue is that type's recursion. Strongly connected components are found by
Tarjan's algorithm on an explicit stack; a component with a function of mantle's is a cycle of
ours, reported with every function in it. A generic function of std, core, alloc or a dependency
is instantiated in the crate that uses it, so a closure passed to an iterator adapter that calls
back into its own function is a cycle through the adapter's instance, and it is ours: the report
names the adapter's functions beside ours. A component of std's or a dependency's generic code
alone (a slice sort's recursion instantiated at our type) is not ours and is only counted.

Each cycle's stable name is its functions of mantle's, sorted and joined by ` | `. The known
cycles are named in scripts/recursion-open.txt, one a line, a list that only shrinks: `--known`
with it passes while the cycles found are exactly that list, and a run without it fails on every
cycle, as §10 asks until each is rewritten as flat work.

What a direct-call graph does not see:
- Indirect calls: a `dyn Trait` method (a vtable slot loaded and called), a function pointer, and
  so any recursion that passes through one. That misses a recursive `Display` or `Debug` (core::fmt
  calls each argument's `fmt` through the pointers `fmt::Arguments` carries, from std's own compiled
  code), a recursive async fn (it must box its future, which is then polled through `dyn Future`),
  a callback in a `Box<dyn Fn>` that calls back into its caller, and tables of function pointers.
  A vtable call cannot be resolved from this IR in practice: the slot is loaded from a vtable that
  flows in as data from wherever the trait object was made, and stable rustc marks no call site
  with the trait it calls through (the type metadata of LLVM's CFI comes only with the unstable
  `-Zsanitizer=cfi`), so resolving one is a whole-program points-to analysis, and matching by
  signature over opaque `ptr` arguments would match most functions. The count of indirect call
  sites in mantle's functions is reported with each run.
- Code this build does not instantiate: a generic function with no instance in production code,
  and code under a `cfg` of another platform (the IR is the host's).
- Calls inside std's or a dependency's compiled code, which reach our code only through pointers.
"""
import filecmp
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent.parent
TARGET = ROOT / "target" / "check-recursion"
KNOWN_DEFAULT = "scripts/recursion-open.txt"
# Set in cargo's environment, so this file run by cargo as its workspace wrapper knows it.
WRAPPER_ENV = "MANTLE_CHECK_RECURSION_WRAPPER"
# The IR build: the dev profile (no optimization, so the IR's calls are the source's) with line
# tables for mantle's crates, which give each function's file and line, and no debug information
# for dependencies, which are not read.
CARGO_BUILD = (
    "cargo", "build", "--workspace", "--lib", "--bins", "--all-features", "--locked",
    "--message-format=json-render-diagnostics",
    "--config", 'profile.dev.debug="line-tables-only"',
    "--config", 'profile.dev.package."*".debug=false',
)
# Steps one symbol's reading may take: a v0 symbol's back-references point only backward, but
# nested ones can repeat work; rustc's symbols take a few hundred.
DEMANGLE_STEPS = 100_000

B62 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
BASIC = {
    "a": "i8", "b": "bool", "c": "char", "d": "f64", "e": "str", "f": "f32", "h": "u8",
    "i": "isize", "j": "usize", "l": "i32", "m": "u32", "n": "i128", "o": "u128", "s": "i16",
    "t": "u16", "u": "()", "v": "...", "x": "i64", "y": "u64", "z": "!", "p": "_",
}
INTEGER = set("ahijlmnostxy")
# The compiler's drop glue for a type, as the pinned toolchain names it.
DROP_GLUE = "core::ptr::drop_glue::<"

# A line of a function's body that calls: `call`/`invoke`, maybe as a value and maybe a tail call;
# its callee is the first `@name(` or `%name(` after the keyword (a named return type is followed
# by a space, an argument by `,` or `)`).
CALL = re.compile(r'^\s+(?:%(?:"[^"]*"|[-\w$.]+) = )?(?:(?:tail|musttail|notail) )?(?:call|invoke)\b')
CALLEE = re.compile(r'([@%])("(?:[^"\\]|\\.)*"|[-\w$.]+)\(')
DEFINE = re.compile(r'^define\b[^@]*@("(?:[^"\\]|\\.)*"|[-\w$.]+)\(')
DBG = re.compile(r"!dbg !(\d+)")
SUBPROGRAM = re.compile(r"^!(\d+) = (?:distinct )?!DISubprogram\(")
SP_FILE = re.compile(r"\bfile: !(\d+)")
SP_LINE = re.compile(r"\bline: (\d+)")
DIFILE = re.compile(r'^!(\d+) = !DIFile\(filename: "((?:[^"\\]|\\.)*)", directory: "((?:[^"\\]|\\.)*)"')


def base62(s, pos):
    """A <base-62-number> at `pos`: its value and the position after it."""
    if s[pos] == "_":
        return 0, pos + 1
    value = 0
    while s[pos] != "_":
        value = value * 62 + B62.index(s[pos])
        pos += 1
    return value + 1, pos + 1


def identifier(s, pos):
    """An <undisambiguated-identifier> at `pos`: its text and the position after it."""
    punycode = s[pos] == "u"
    if punycode:
        pos += 1
    end = pos + 1
    while s[pos] != "0" and s[end].isdigit():  # "0", or a number with no leading zero
        end += 1
    length = int(s[pos:end])
    pos = end + 1 if s[end] == "_" else end
    text = s[pos:pos + length]
    if len(text) != length:
        raise ValueError("short identifier")
    if punycode:
        basic, _, encoded = text.rpartition("_")
        text = (basic + "-" + encoded).encode("ascii").decode("punycode")
    return text, pos + length


def disambiguator(s, pos):
    """An optional `s<base-62-number>` at `pos`: its value (0 when absent) and the position after."""
    if s[pos] != "s":
        return 0, pos
    value, pos = base62(s, pos + 1)
    return value + 1, pos


def demangle(symbol):
    """A v0 symbol read: (its path as text, the crate defining the item it instantiates, every crate
    its path names), or None for a symbol of another form or one this reader cannot parse.

    The grammar (rustc's "v0 Symbol Format", RFC 2603) is read by a stack of tasks, each a
    production still to read, so nesting deepens the stack and never the call stack. A task's
    `show` is false inside an impl's own path, which says where the impl is and is not printed."""
    if not symbol.startswith("_R"):
        return None
    s = symbol[2:]
    if not s or s[0].isdigit():  # an encoding version: none is defined
        return None
    out, crates, defining = [], set(), [None]
    pos = 0
    tasks = [("path", True, True, True)]
    steps = 0
    try:
        while tasks:
            steps += 1
            if steps > DEMANGLE_STEPS:
                return None
            task = tasks.pop()
            kind, show = task[0], task[1]
            if kind == "text":
                if show:
                    out.append(task[2])
            elif kind == "seek":
                pos = task[2]
            elif kind == "path":
                in_value, defines = task[2], task[3]
                tag = s[pos]
                pos += 1
                if tag == "C":
                    _, pos = disambiguator(s, pos)
                    name, pos = identifier(s, pos)
                    crates.add(name)
                    if defines and defining[0] is None:
                        defining[0] = name
                    if show:
                        out.append(name)
                elif tag == "N":
                    namespace = s[pos]
                    pos += 1
                    tasks.append(("name", show, namespace))
                    tasks.append(("path", show, in_value, defines))
                elif tag == "I":
                    tasks.append(("args", show, in_value, 0))
                    tasks.append(("path", show, in_value, defines))
                elif tag in "MX":
                    _, pos = disambiguator(s, pos)
                    tasks.append(("text", show, ">"))
                    if tag == "X":
                        tasks.append(("path", show, False, False))
                        tasks.append(("text", show, " as "))
                    tasks.append(("type", show))
                    tasks.append(("text", show, "<"))
                    tasks.append(("path", False, False, defines))
                elif tag == "Y":
                    tasks.append(("text", show, ">"))
                    tasks.append(("path", show, False, defines))
                    tasks.append(("text", show, " as "))
                    tasks.append(("type", show))
                    tasks.append(("text", show, "<"))
                elif tag == "B":
                    target, pos = base62(s, pos)
                    if target >= pos - 1:
                        return None
                    tasks.append(("seek", show, pos))
                    tasks.append(("path", show, in_value, defines))
                    pos = target
                else:
                    return None
            elif kind == "name":
                number, pos = disambiguator(s, pos)
                name, pos = identifier(s, pos)
                if show:
                    namespace = task[2]
                    if namespace.islower():
                        out.append("::" + name if name else "")
                    else:
                        label = {"C": "closure", "S": "shim"}.get(namespace, namespace)
                        out.append("::{" + label + (":" + name if name else "") + "#" + str(number) + "}")
            elif kind == "args":
                in_value, count = task[2], task[3]
                if count == 0 and show:
                    out.append("::<" if in_value else "<")
                if s[pos] == "E":
                    pos += 1
                    if show:
                        out.append(">")
                    continue
                if count and show:
                    out.append(", ")
                tasks.append(("args", show, in_value, count + 1))
                if s[pos] == "L":
                    _, pos = base62(s, pos + 1)
                    if show:
                        out.append("'_")
                elif s[pos] == "K":
                    pos += 1
                    tasks.append(("const", show))
                else:
                    tasks.append(("type", show))
            elif kind == "type":
                tag = s[pos]
                if tag in BASIC:
                    pos += 1
                    if show:
                        out.append(BASIC[tag])
                elif tag in "RQ":
                    pos += 1
                    if s[pos] == "L":
                        _, pos = base62(s, pos + 1)
                    if show:
                        out.append("&" if tag == "R" else "&mut ")
                    tasks.append(("type", show))
                elif tag in "PO":
                    pos += 1
                    if show:
                        out.append("*const " if tag == "P" else "*mut ")
                    tasks.append(("type", show))
                elif tag in "AS":
                    pos += 1
                    if show:
                        out.append("[")
                    tasks.append(("text", show, "]"))
                    if tag == "A":
                        tasks.append(("const", show))
                        tasks.append(("text", show, "; "))
                    tasks.append(("type", show))
                elif tag == "T":
                    pos += 1
                    if show:
                        out.append("(")
                    tasks.append(("tuple", show, 0))
                elif tag == "F":
                    pos += 1
                    if s[pos] == "G":
                        _, pos = base62(s, pos + 1)
                    prefix = ""
                    if s[pos] == "U":
                        pos += 1
                        prefix = "unsafe "
                    if s[pos] == "K":
                        pos += 1
                        if s[pos] == "C":
                            pos += 1
                            abi = "C"
                        else:
                            abi, pos = identifier(s, pos)
                        prefix += 'extern "' + abi.replace("_", "-") + '" '
                    if show:
                        out.append(prefix + "fn(")
                    tasks.append(("fn", show, 0))
                elif tag == "D":
                    pos += 1
                    if s[pos] == "G":
                        _, pos = base62(s, pos + 1)
                    if show:
                        out.append("dyn ")
                    tasks.append(("dyn", show, 0))
                elif tag == "B":
                    target, pos = base62(s, pos + 1)
                    if target >= pos - 1:
                        return None
                    tasks.append(("seek", show, pos))
                    tasks.append(("type", show))
                    pos = target
                else:
                    tasks.append(("path", show, False, False))
            elif kind == "tuple":
                count = task[2]
                if s[pos] == "E":
                    pos += 1
                    if show:
                        out.append(",)" if count == 1 else ")")
                    continue
                if count and show:
                    out.append(", ")
                tasks.append(("tuple", show, count + 1))
                tasks.append(("type", show))
            elif kind == "fn":
                count = task[2]
                if s[pos] == "E":
                    pos += 1
                    if show:
                        out.append(")")
                    if s[pos] == "u":
                        pos += 1
                    else:
                        if show:
                            out.append(" -> ")
                        tasks.append(("type", show))
                    continue
                if count and show:
                    out.append(", ")
                tasks.append(("fn", show, count + 1))
                tasks.append(("type", show))
            elif kind == "dyn":
                count = task[2]
                if s[pos] == "E":
                    if s[pos + 1] != "L":
                        return None
                    _, pos = base62(s, pos + 2)
                    continue
                if count and show:
                    out.append(" + ")
                tasks.append(("dyn", show, count + 1))
                tasks.append(("bindings", show, 0))
                tasks.append(("path", show, False, False))
            elif kind == "bindings":
                count = task[2]
                if s[pos] != "p":
                    if count and show:
                        out.append(">")
                    continue
                name, pos = identifier(s, pos + 1)
                if show:
                    # Inside the trait's own generic arguments, if it has them, as rustc-demangle does.
                    opened = count == 0 and out[-1] == ">"
                    if opened:
                        out.pop()
                    out.append((", " if count or opened else "<") + name + " = ")
                tasks.append(("bindings", show, count + 1))
                tasks.append(("type", show))
            elif kind == "const":
                tag = s[pos]
                pos += 1
                if tag == "B":
                    target, pos = base62(s, pos)
                    if target >= pos - 1:
                        return None
                    tasks.append(("seek", show, pos))
                    tasks.append(("const", show))
                    pos = target
                elif tag == "p":
                    if show:
                        out.append("_")
                elif tag in INTEGER or tag in "bc":
                    negative = s[pos] == "n"
                    if negative:
                        pos += 1
                    end = s.index("_", pos)
                    value = int(s[pos:end], 16) if end > pos else 0
                    pos = end + 1
                    if tag == "b":
                        text = "true" if value else "false"
                    elif tag == "c":
                        text = repr(chr(value))
                    else:
                        text = ("-" if negative else "") + str(value)
                    if show:
                        out.append(text)
                else:
                    return None
            else:
                return None
    except (IndexError, ValueError, UnicodeError):
        return None
    return "".join(out), defining[0], crates


def wrap(argv):
    """As cargo's RUSTC_WORKSPACE_WRAPPER: runs rustc (argv[0]) with argv[1:]; a library's or
    binary's compile also writes its LLVM IR beside its outputs, in one codegen unit, so one .ll
    holds the crate and no stale unit's file is read with it."""
    args = list(argv)
    types = [args[i + 1] for i, a in enumerate(args[:-1]) if a == "--crate-type"]
    names = [args[i + 1] for i, a in enumerate(args[:-1]) if a == "--crate-name"]
    if types and set(types) <= {"lib", "bin"} and names != ["build_script_build"]:
        args += ["--emit=llvm-ir", "-C", "codegen-units=1"]
    # The descriptors stay open: cargo's jobserver reaches rustc through them.
    return subprocess.run(args, close_fds=False).returncode


def cargo_build(root, target):
    """Builds the IR; returns cargo's exit code and the workspace's units: (crate name, kind,
    package name, the IR files)."""
    env = dict(os.environ)
    env.update({
        "CARGO_TARGET_DIR": str(target),
        "CARGO_INCREMENTAL": "0",
        "RUSTC_WORKSPACE_WRAPPER": str(pathlib.Path(__file__).resolve()),
        WRAPPER_ENV: "1",
    })
    run = subprocess.run(CARGO_BUILD, cwd=root, env=env, stdout=subprocess.PIPE, text=True)
    deps = target / "debug" / "deps"
    units = []
    for line in run.stdout.splitlines():
        try:
            message = json.loads(line)
        except ValueError:
            continue
        if message.get("reason") != "compiler-artifact":
            continue
        manifest = pathlib.Path(message["manifest_path"]).resolve()
        if root / "crates" not in manifest.parents:
            continue
        name = message["target"]["name"].replace("-", "_")
        kind = message["target"]["kind"][0]
        # `path+file:///../crates/s3#mantle-s3@0.1.0`, or `../crates/mantle#0.1.0` when the
        # package is named as its directory.
        url, _, fragment = message["package_id"].rpartition("#")
        package = fragment.partition("@")[0] if "@" in fragment else url.rstrip("/").rpartition("/")[2]
        stems = set()
        for filename in message["filenames"]:
            path = pathlib.Path(filename)
            if path.parent == deps and path.suffix == ".rmeta" and path.name.startswith("lib"):
                stems.add(path.stem[3:])
        executable = message.get("executable")
        if executable:
            # The binary in deps/ whose bytes the uplifted executable has: cargo hard-links it on
            # Linux and copies it on macOS.
            for candidate in deps.glob(name + "-*"):
                if candidate.suffix in ("", ".exe") and filecmp.cmp(candidate, executable, shallow=False):
                    stems.add(candidate.stem)
        files = sorted(p for stem in stems for p in deps.glob(stem + "*.ll") if p.name.split(".", 1)[0] == stem)
        units.append((name, kind, package, files))
    return run.returncode, units


def build(root, target):
    """The IR files of every workspace unit and the workspace's crate names, or the failures. A unit
    cargo found fresh but whose IR is missing (a target directory built otherwise) is cleaned and
    built once more."""
    code, units = cargo_build(root, target)
    if code != 0:
        return None, None, [f"check-recursion: the IR build failed (cargo exit {code})"]
    missing = sorted({package for _, _, package, files in units if len(files) != 1})
    if missing:
        clean = ["cargo", "clean", "--target-dir", str(target)] + [a for p in missing for a in ("-p", p)]
        subprocess.run(clean, cwd=root, check=False)
        code, units = cargo_build(root, target)
        if code != 0:
            return None, None, [f"check-recursion: the IR build failed (cargo exit {code})"]
    failures = [f"check-recursion: {name} ({kind}): {len(files)} IR files, not one" for name, kind, _, files in units if len(files) != 1]
    if not any(kind == "bin" for _, kind, _, _ in units):
        failures.append("check-recursion: the build reported no binary")
    return [files[0] for _, _, _, files in units if len(files) == 1], {name for name, _, _, _ in units}, failures


def read_ir(paths):
    """The functions the IR files define, by symbol: [direct callees, (file, directory, line) or None,
    indirect call sites]."""
    functions = {}
    for path in paths:
        sites, files, symbol_dbg = {}, {}, []
        current = None
        with open(path, encoding="utf-8", errors="replace") as ir:
            for line in ir:
                if current is not None:
                    if line.startswith("}"):
                        current = None
                        continue
                    if "call" not in line and "invoke" not in line:
                        continue
                    call = CALL.match(line)
                    callee = call and CALLEE.search(line, call.end())
                    if not callee:
                        continue
                    if callee.group(1) == "%":
                        current[2] += 1
                    elif not callee.group(2).startswith("llvm."):
                        current[0].add(callee.group(2).strip('"'))
                elif line.startswith("define"):
                    m = DEFINE.match(line)
                    if m is None:
                        continue
                    symbol = m.group(1).strip('"')
                    current = functions.setdefault(symbol, [set(), None, 0])
                    dbg = DBG.search(line)
                    if dbg:
                        symbol_dbg.append((symbol, dbg.group(1)))
                elif line.startswith("!"):
                    if "DISubprogram(" in line:
                        m = SUBPROGRAM.match(line)
                        f, n = SP_FILE.search(line), SP_LINE.search(line)
                        if m and f and n:
                            sites[m.group(1)] = (f.group(1), int(n.group(1)))
                    elif "DIFile(" in line:
                        m = DIFILE.match(line)
                        if m:
                            files[m.group(1)] = (m.group(2), m.group(3))
        for symbol, dbg in symbol_dbg:
            site = sites.get(dbg)
            if site and site[0] in files and functions[symbol][1] is None:
                functions[symbol][1] = files[site[0]] + (site[1],)
    return functions


def shown(site):
    """A function's file:line, relative to the workspace when it is in it."""
    if site is None:
        return "(no line)"
    filename, directory, line = site
    full = pathlib.Path(directory, filename)
    try:
        filename = full.resolve().relative_to(ROOT).as_posix()
    except ValueError:
        if not filename.startswith("library/"):
            filename = full.as_posix()
    return f"{filename}:{line}"


def components(graph):
    """The strongly connected components of `graph` (node: successors) with a cycle: more than one
    node, or one that calls itself. Tarjan's algorithm, its depth-first walk on an explicit stack of
    (node, next successor) frames."""
    index, low, on_stack, stack, found = {}, {}, set(), [], []
    counter = 0
    for root in graph:
        if root in index:
            continue
        index[root] = low[root] = counter
        counter += 1
        stack.append(root)
        on_stack.add(root)
        frames = [(root, iter(graph[root]))]
        while frames:
            node, successors = frames[-1]
            advanced = False
            for successor in successors:
                if successor not in index:
                    index[successor] = low[successor] = counter
                    counter += 1
                    stack.append(successor)
                    on_stack.add(successor)
                    frames.append((successor, iter(graph[successor])))
                    advanced = True
                    break
                if successor in on_stack:
                    low[node] = min(low[node], index[successor])
            if advanced:
                continue
            frames.pop()
            if frames:
                parent = frames[-1][0]
                low[parent] = min(low[parent], low[node])
            if low[node] == index[node]:
                component = []
                while True:
                    member = stack.pop()
                    on_stack.discard(member)
                    component.append(member)
                    if member == node:
                        break
                if len(component) > 1 or node in graph[node]:
                    found.append(component)
    return found


def cycles(functions, ours):
    """The cycles of the call graph: (ours, not ours), each a (stable name, [(text, site, crate)])
    sorted by name; a symbol this reader cannot parse shows raw and is no one's."""
    graph = {symbol: [c for c in callees if c in functions] for symbol, (callees, _, _) in functions.items()}
    mine, others = [], []
    for component in components(graph):
        members = []
        for symbol in component:
            read = demangle(symbol)
            text, defining, named = read if read else (symbol, None, set())
            owner = defining if defining in ours else None
            if owner is None and text.startswith(DROP_GLUE):
                owner = next((c for c in sorted(named) if c in ours), None)
            members.append((text, shown(functions[symbol][1]), owner))
        members.sort()
        own = sorted({text for text, _, owner in members if owner})
        if own:
            mine.append((" | ".join(own), members))
        else:
            others.append((" | ".join(sorted({text for text, _, _ in members})), members))
    return sorted(mine), sorted(others)


def known_cycles(path):
    if not path.exists():
        return None
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip() and not line.startswith("#")]


def check(found, known, name):
    """The failures of the cycles found against the known list (None: every cycle fails)."""
    failures = []
    if known is None:
        return [describe("a call-graph cycle", stable, members) for stable, members in found]
    names = [stable for stable, _ in found]
    if len(known) != len(set(known)):
        failures.append(f"{name}: a cycle listed twice")
    for stable, members in found:
        if stable not in known:
            failures.append(describe("a new call-graph cycle (CLAUDE.md §10): rewrite it as flat work", stable, members))
    for stable in sorted(set(known) - set(names)):
        failures.append(f"{name}: {stable} is no longer a cycle: take it off the list")
    return failures


def describe(what, stable, members):
    lines = [f"{what}: {stable}"]
    for text, site, owner in members:
        lines.append(f"    {site}  {text}" + ("" if owner else "  [std or a dependency]"))
    return "\n".join(lines)


# The reader on small inputs: v0 symbols and what they read as (the forms the IR holds: a method of
# an inherent impl, a trait impl, a closure in a generic function, drop glue, a shim, a function
# pointer type and a back-reference), and a module whose cycles are known.
SYMBOLS = {
    "_RNvMs_NtCsjDp8xmfcqRr_9mantle_s34jsonNtB4_5Value5write": ("<mantle_s3::json::Value>::write", "mantle_s3"),
    "_RINvNtCs8Mbv00yxnRz_4core3ptr9drop_glueINtNtB4_6option6OptionNtNtCsaH3QtraahVu_11mantle_node4node9NodeErrorEEB11_": (
        "core::ptr::drop_glue::<core::option::Option<mantle_node::node::NodeError>>", "core"),
    "_RNvMNtNtCs8Mbv00yxnRz_4core3ptr7mut_ptrOy3subCs91YuFR6N3tf_7probe_x": ("<*mut u64>::sub", "core"),
    "_RINvMNtNtNtNtCs8Mbv00yxnRz_4core5slice4sort6stable5mergeINtB3_10MergeStateyE10merge_downNvYyNtNtBb_3cmp10PartialOrd2ltECs91YuFR6N3tf_7probe_x": (
        "<core::slice::sort::stable::merge::MergeState<u64>>::merge_down::<<u64 as core::cmp::PartialOrd>::lt>", "core"),
    "_RINvMNtCs8Mbv00yxnRz_4core3stre10split_onceNCNvNtCsjDp8xmfcqRr_9mantle_s35sigv412header_claim0EBL_": (
        "<str>::split_once::<mantle_s3::sigv4::header_claim::{closure#0}>", "core"),
    "_RNvXsJ_NtCs8Mbv00yxnRz_4core6optionINtB5_6OptionTjjEENtNtNtB7_3ops9try_trait3Try6branchCsjDp8xmfcqRr_9mantle_s3": (
        "<core::option::Option<(usize, usize)> as core::ops::try_trait::Try>::branch", "core"),
    "_RNCNvCs1_5crate1f0B3_": ("crate::f::{closure#0}", "crate"),
    "_RNvYNtCs1_1a1TNtCs2_1b5Trait1m": ("<a::T as b::Trait>::m", "b"),
}
MODULE = """\
define internal void @_RNvCs1_6mantle4walk(ptr %n) unnamed_addr !dbg !10 {
start:
  call void @_RNvCs1_6mantle4step(ptr %n), !dbg !12
  %r = invoke i64 @_RNvCs1_6mantle4walk(ptr %n)
          to label %ok unwind label %cleanup
}
define internal void @_RNvCs1_6mantle4step(ptr %n) unnamed_addr {
start:
  tail call void @_RNvCs1_6mantle4walk(ptr %n)
  call void %f(ptr %n)
  call void @llvm.memcpy.p0.p0.i64(ptr %n, ptr %n, i64 1, i1 false)
}
define internal void @_RNvCs1_4core4sort(ptr %n) unnamed_addr {
start:
  call void @_RNvCs1_4core4sort(ptr %n)
}
define internal void @_RNvCs1_6mantle4leaf(ptr %n) unnamed_addr {
start:
  call void @_RNvCs1_4core4sort(ptr %n)
}
!10 = distinct !DISubprogram(name: "walk", scope: !11, file: !11, line: 7, type: !13, unit: !1)
!11 = !DIFile(filename: "crates/fixture/src/lib.rs", directory: "/nowhere")
"""


def fixtures():
    """The reader on the inputs above: the failures where it reads other than it must."""
    failures = []
    for symbol, expect in SYMBOLS.items():
        read = demangle(symbol)
        got = None if read is None else (read[0], read[1])
        if got != expect:
            failures.append(f"scripts/check-recursion.py: fixture {symbol}: read {got}, not {expect}")
    with tempfile.TemporaryDirectory(prefix="check-recursion-") as directory:
        path = pathlib.Path(directory, "fixture.ll")
        path.write_text(MODULE, encoding="utf-8")
        functions = read_ir([path])
    mine, others = cycles(functions, {"mantle"})
    names = [stable for stable, _ in mine]
    if names != ["mantle::step | mantle::walk"] or [s for s, _ in others] != ["core::sort"]:
        failures.append(f"scripts/check-recursion.py: fixture module: cycles {names}, others {[s for s, _ in others]}")
    if functions.get("_RNvCs1_6mantle4step", [None, None, 0])[2] != 1:
        failures.append("scripts/check-recursion.py: fixture module: the indirect call was not counted")
    site = shown(functions.get("_RNvCs1_6mantle4walk", [None, None])[1])
    if site != "/nowhere/crates/fixture/src/lib.rs:7":
        failures.append(f"scripts/check-recursion.py: fixture module: walk's site {site}")
    return failures


def main() -> int:
    if os.environ.get(WRAPPER_ENV) == "1" and len(sys.argv) > 1 and not sys.argv[1].startswith("--"):
        return wrap(sys.argv[1:])
    args = sys.argv[1:]
    known_path = None
    if args[:1] == ["--known"] and len(args) == 2:
        known_path = pathlib.Path(args[1]).resolve()
    elif args:
        print(f"usage: check-recursion.py [--known {KNOWN_DEFAULT}]", file=sys.stderr)
        return 2
    failures = fixtures()
    paths, ours, build_failures = build(ROOT, TARGET)
    failures.extend(build_failures)
    if paths is None:
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1
    functions = read_ir(paths)
    mine, others = cycles(functions, ours)
    known = None
    name = KNOWN_DEFAULT
    if known_path is not None:
        known = known_cycles(known_path)
        try:
            name = known_path.relative_to(ROOT).as_posix()
        except ValueError:
            name = str(known_path)
        if known is None:
            failures.append(f"{name}: missing")
            known = []
    failures.extend(check(mine, known, name))
    for failure in failures:
        print(failure, file=sys.stderr)
    edges = sum(len(callees) for callees, _, _ in functions.values())
    # Mantle's functions are those whose line tables name a workspace file.
    indirect = sum(sites for _, site, sites in functions.values() if site and site[0].startswith("crates/"))
    print(
        f"check-recursion: {len(functions)} functions, {edges} direct calls, {len(mine)} cycles of ours"
        f" ({len(others)} in std's or dependencies' generic code alone, not ours),"
        f" {indirect} indirect call sites of ours not followed",
        file=sys.stderr if failures else sys.stdout,
    )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
