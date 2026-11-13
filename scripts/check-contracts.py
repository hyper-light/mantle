#!/usr/bin/env python3
"""Source contracts the compiler cannot state (CLAUDE.md §4, §7).

`unsafe` is denied workspace-wide; a file may opt in with `#![allow(unsafe_code)]` only if it
is listed here, and every such file is FFI to an OS interface with each block commented.

Every number production code runs with has a row in docs/design/constants.md: each `const` and
`static` whose value holds a number, of any type (a scalar, an array, a tuple, a struct, a
`Duration`), and each literal parameter, a number written as the whole value of a struct field,
an assignment, a binding, a count, duration or fallback argument, a command-line default or a
size shift, and a literal array (a ladder or a table) outside a constant. Comparisons and other
call arguments are not read yet.
The vendored crates on mantle's write path (`VENDORED`) are inventoried the same way. A row
states its kind, the value the code has, and its basis, and the gate checks all three: the row's
value against the code's, token by token; its kind against the owner's rule, which accepts a
number only if it is learned, derived by a stated model from measured inputs or structural facts,
or backed by well-researched primary sources; and its basis against the bases the rule refuses.

A row whose kind the rule refuses is an open item. Each is named in scripts/arbitrary-open.txt
(one `file key` a line), the list of the known open items, which only shrinks: `--known` with it
passes while the open rows are exactly that list and it is as long as `OPEN_CEILING`, and a run
without it fails on every open item, as the rule asks until each is replaced.

Test code is not production code: an item under a `cfg` that requires `test` (an inline test
module, a test-only function or constant), a file whose first item is `#![cfg(test)]`, and a file
every declaration of which, in the whole tree, is test code (rustc compiles a file only where a
`mod` declaration or an `include!` names it). Each run first checks these rules on small fixture
trees (`fixtures`), in this process.
"""
import hashlib
import pathlib
import re
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent.parent

# Each entry: the file and the interface it binds.
UNSAFE_ALLOWED = {
    "crates/disk/src/probe/macos.rs": "IOKit and CoreFoundation (device identification)",
    "crates/disk/src/probe/windows.rs": "volume management and IOCTL_STORAGE_QUERY_PROPERTY (device identification)",
    "crates/node/src/secret/windows.rs": "CreateFileW with a protected DACL and GetKernelObjectSecurity (owner-only key files)",
    "crates/engine/src/port/mmap.rs": "mmap/munmap and CreateFileMappingW/MapViewOfFile (the memtable arena's lazily zeroed blocks); mmap/munmap/madvise for the cache's page buffers on huge-page boundaries (Linux); glibc's mallopt (freed large blocks back to the OS)",
}

# The vendored crates a PUT's path runs on (docs/measurements/2026-11-12-arbitrary-values-audit.md):
# hyper-tls, hyper-seal, hyper-swim, hyper-sim and hyper-measure carry no number it runs on, and
# aws-lc-rs and aws-lc-sys are cryptography as published.
VENDORED = (
    "hyper-block", "hyper-datagram", "hyper-durable", "hyper-liveness", "hyper-log", "hyper-quic",
    "hyper-raft", "hyper-rt", "hyper-rt-transport", "hyper-timing", "hyper-transport",
)

# Kinds the rule accepts as a basis.
ACCEPTED = {
    "format": "a field width, tag, version, magic or length of an encoding",
    "external": "fixed by a specification, an interface, or a stated property of a system mantle must meet",
    "derived": "computed by a stated model from measured inputs or structural facts",
    "cited": "prescribed by peer-reviewed work or a primary source used as it prescribes",
    "learned": "measured by the running system",
    "counted": "bytes or items a structure allocates, counted by the allocator and pinned by a test",
}
# Kinds the rule refuses: each row of one is an open item.
REFUSED = {
    "picked": "a value picked by hand, a bound or a floor taken as the operating value",
    "knee": "read off one measured curve by a chosen criterion and frozen",
    "once": "measured once, on one machine",
    "copied": "another system's tuning",
    "guessed": "a guessed timeout, deadline or wait",
    "inherited": "derived from an arbitrary input",
    "open": "nothing establishes it yet",
}
# The known open items may number at most this; it only falls, as each gains a basis.
OPEN_CEILING = 287
# A basis the rule refuses, whatever kind its row claims: each pattern and what it names.
REFUSED_BASES = (
    (re.compile(r"\bknee\b", re.I), "a knee read off a curve"),
    (re.compile(r"\bmeasured (?:once|on (?:the|this|one|an?) (?:box|machine|host|runner|M\d))", re.I), "a measurement of one machine"),
    (re.compile(r"\b(?:picked|hand-set|arbitrar(?:y|ily))\b", re.I), "a picked value"),
)
# Another system whose tuning is not a basis, unless the row is `format` or `external` (a fact
# about that system mantle must meet: its encoding, a client's stated part size) or cites the
# peer-reviewed work or standard that measured or prescribes the value.
PRIMARY = re.compile(r"\b(?:RFC|IETF|draft-ietf|ISO|IEEE|POSIX)\b|et al\.|\b(?:19|20)\d\d\b")
OTHER_SYSTEMS = re.compile(
    r"\b(?:RocksDB|LevelDB|SplinterDB|botocore|AWS CLI|quinn|quiche|Chromium|Google QUIC|tokio|focal|slates|HDFS|GFS|Tectonic|Centrifuge)\b"
)
SUFFIX = r"(?:u8|u16|u32|u64|u128|usize|i8|i16|i32|i64|i128|isize|f32|f64)?"
# The numeric types a constant of no literal still counts by: integer aliases of OS interfaces
# and the word type of the CRC macro beside Rust's own.
NUMERIC = {
    "u8", "u16", "u32", "u64", "u128", "usize", "i8", "i16", "i32", "i64", "i128", "isize",
    "f32", "f64", "Duration", "Opcode", "CFIndex", "MachPort", "$word",
}
# Zero as a parameter's whole value is the empty count, offset or sum; one as `min`, `max`,
# `clamp`, `unwrap_or` or `map_or`'s argument is "at least one" (a divisor, a length); and one bound by `let` is a count's
# or a product's first value: none is a choice, so none is inventoried. A zero that configures something (no spin, no reserved core) is the absence of what
# it configures; its field's own row, where the field has a choice, states why it is absent.
ZERO = re.compile(r"0(?:\.0)?" + SUFFIX)
ONE = re.compile(r"1(?:\.0)?" + SUFFIX)
# A value longer than this is stated as the SHA-256 of its tokens.
VALUE_WIDTH = 100

CALLS = {
    "from_secs", "from_millis", "from_micros", "from_nanos", "from_secs_f64", "from_secs_f32",
    "with_capacity", "channel", "sync_channel", "bounded", "take", "repeat", "sleep", "reserve",
    "resize", "truncate", "min", "max", "clamp", "unwrap_or", "map_or",
}

TOKEN = re.compile(
    r"(?P<nl>\n)|(?P<ws>[ \t\r\f]+)|(?P<lc>//[^\n]*)|(?P<bc>/\*)"
    r"|(?P<raw>b?r(?P<hashes>#*)\")"
    r"|(?P<str>b?\"(?:[^\"\\]|\\.)*\")"
    r"|(?P<chr>b?'(?:[^'\\\n]|\\(?:u\{[0-9a-fA-F]{1,6}\}|x[0-9a-fA-F]{2}|.))')"
    r"|(?P<life>'[A-Za-z_][A-Za-z0-9_]*)"
    r"|(?P<num>(?:0x[0-9a-fA-F_]+|0b[01_]+|0o[0-7_]+|[0-9][0-9_]*(?:\.(?![.A-Za-z_])[0-9_]*)?(?:[eE][+-]?[0-9_]+)?)" + SUFFIX + r")"
    r"|(?P<id>[A-Za-z_][A-Za-z0-9_]*)"
    r"|(?P<p>::|<<=|>>=|<<|>>|=>|->|==|!=|<=|>=|\.\.=|\.\.\.|\.\.|&&|\|\||[-+*/%^&|]=|.)",
    re.S,
)
ROW = re.compile(r"^\|\s*`([^`]+)`\s+`([^`]+)`\s*\|\s*([a-z]+)\s*\|\s*(`+)(.*?)\4\s*\|(.*)\|\s*$")
# The inventory's rows name Rust files; its notes on scripts and measurement plans are not read.
TABLE_ROW = re.compile(r"^\|\s*`(?:crates|vendor)/[^`]+`\s+`")

ALLOW_UNSAFE = re.compile(r"allow\s*\(\s*unsafe_code\s*\)")
UNSAFE_BLOCK = re.compile(r"\bunsafe\s*(\{|fn\b|extern\b|impl\b)")


def tokens(text):
    """The tokens of Rust source as (kind, text, line): idents, numbers, strings and punctuation,
    comments dropped. A raw string's text is its body."""
    out, i, n, line = [], 0, len(text), 1
    while i < n:
        m = TOKEN.match(text, i)
        kind = m.lastgroup
        if kind == "nl":
            line += 1
            i = m.end()
        elif kind in ("ws", "lc"):
            i = m.end()
        elif kind == "bc":
            depth, j = 1, m.end()
            while depth:
                a, b = text.find("/*", j), text.find("*/", j)
                if b < 0:
                    j = n
                    break
                if 0 <= a < b:
                    depth, j = depth + 1, a + 2
                else:
                    depth, j = depth - 1, b + 2
            line += text.count("\n", i, j)
            i = j
        elif kind in ("raw", "hashes"):
            end = '"' + (m.group("hashes") or "")
            j = text.find(end, m.end())
            j = n if j < 0 else j + len(end)
            out.append(("str", text[m.end():j - len(end)], line))
            line += text.count("\n", i, j)
            i = j
        elif kind in ("str", "chr"):
            out.append(("str", m.group(0), line))
            line += m.group(0).count("\n")
            i = m.end()
        else:
            out.append((kind, m.group(0), line))
            i = m.end()
    return out


def requires_test(toks):
    """Whether a `cfg` predicate's tokens (inside its parentheses) hold only in test builds:
    `test`, or `all(..)` with an argument that requires it."""
    if [t for _, t, _ in toks] == ["test"]:
        return True
    if len(toks) >= 3 and toks[0][1] == "all" and toks[1][1] == "(":
        depth, start, args = 0, 2, []
        for k in range(2, len(toks)):
            t = toks[k][1]
            if t in "([{":
                depth += 1
            elif t in ")]}":
                if depth == 0:
                    args.append(toks[start:k])
                    break
                depth -= 1
            elif t == "," and depth == 0:
                args.append(toks[start:k])
                start = k + 1
        return any(requires_test(a) for a in args if a)
    return False


def attribute_at(toks, i):
    """At a `#` starting an attribute: (end index past its `]`, inner, cfg tokens or None,
    path string or None)."""
    inner = i + 1 < len(toks) and toks[i + 1][1] == "!"
    k = i + (2 if inner else 1)
    if k >= len(toks) or toks[k][1] != "[":
        return None
    depth, j = 0, k
    while j < len(toks):
        t = toks[j][1]
        if t in "([{":
            depth += 1
        elif t in ")]}":
            depth -= 1
            if depth == 0:
                break
        j += 1
    body = toks[k + 1:j]
    cfg = body[2:-1] if len(body) >= 3 and body[0][1] == "cfg" and body[1][1] == "(" else None
    path = None
    if len(body) == 3 and body[0][1] == "path" and body[1][1] == "=" and body[2][0] == "str":
        path = body[2][1].strip('"')
    return j + 1, inner, cfg, path


def item_end(toks, i):
    """The index past the item or statement starting at `i`: its `;` at depth zero, or the `}`
    closing its first brace when that brace comes first (a function, module, impl, type)."""
    depth, j = 0, i
    head = [t for _, t, _ in toks[i:i + 4]]
    semicolon_item = any(t in ("const", "static", "let", "use", "type") for t in head[:2]) and "fn" not in head[:3]
    while j < len(toks):
        t = toks[j][1]
        if t in "([{":
            if t == "{" and depth == 0 and not semicolon_item:
                close = matching(toks, j)
                return close + 1
            depth += 1
        elif t in ")]}":
            if depth == 0:
                return j
            depth -= 1
        elif t == ";" and depth == 0:
            return j + 1
        j += 1
    return j


def matching(toks, j):
    """The index of the bracket closing the one at `j`."""
    depth = 0
    for k in range(j, len(toks)):
        t = toks[k][1]
        if t in "([{":
            depth += 1
        elif t in ")]}":
            depth -= 1
            if depth == 0:
                return k
    return len(toks) - 1


class Source:
    """One file's tokens, split into production and test code, with its `mod` declarations and
    `include!`s: (target path, whether test code declares it)."""

    def __init__(self, path):
        self.path = path
        self.toks = tokens(path.read_text(encoding="utf-8"))
        self.marked = False
        self.test = [False] * len(self.toks)
        self.imports = []
        self.split()

    def split(self):
        toks, i = self.toks, 0
        # The inline modules open at each point: (closing index, directory part, test code).
        inline = []
        pending_cfg, pending_path = False, None
        first_item = True
        while i < len(toks):
            while inline and i > inline[-1][0]:
                inline.pop()
            in_test = any(t for _, _, t in inline)
            if toks[i][1] == "#":
                attr = attribute_at(toks, i)
                if attr:
                    end, inner, cfg, path = attr
                    if inner and cfg is not None and requires_test(cfg) and first_item:
                        self.marked = True
                        self.test = [True] * len(toks)
                    elif inner:
                        pass
                    else:
                        if cfg is not None and requires_test(cfg):
                            pending_cfg = True
                        if path is not None:
                            pending_path = path
                    i = end
                    continue
            if toks[i][1] not in ("pub", "(", "crate", "super", "self", "in", ")"):
                first_item = False
            gated = pending_cfg or in_test
            if pending_cfg:
                end = item_end(toks, i)
                for k in range(i, end):
                    self.test[k] = True
            # `mod name;` or `mod name {`, after any visibility.
            if toks[i][1] == "mod" and i + 2 < len(toks) and toks[i + 1][0] == "id":
                name, after = toks[i + 1][1], toks[i + 2][1]
                parts = [p for _, p, _ in inline]
                if after == ";":
                    self.imports.append((self.resolve(parts, name, pending_path), gated or self.marked))
                elif after == "{":
                    inline.append((matching(toks, i + 2), pending_path or name, gated))
                    pending_cfg, pending_path = False, None
                    i += 3
                    continue
            if toks[i][1] == "include" and i + 4 < len(toks) and toks[i + 1][1] == "!" and toks[i + 3][0] == "str":
                target = (self.path.parent / toks[i + 3][1].strip('"')).resolve()
                self.imports.append((target, gated or self.marked))
            if pending_cfg:
                i = item_end(toks, i)
                pending_cfg, pending_path = False, None
                continue
            if toks[i][1] in (";", "}", "{"):
                pending_path = None
            i += 1

    def resolve(self, parts, name, path):
        base = self.path.parent
        own = self.path.stem not in ("lib", "main", "mod")
        if path is not None:
            if parts:
                return (base / pathlib.Path(*parts) / path).resolve() if not own else (base / self.path.stem / pathlib.Path(*parts) / path).resolve()
            return (base / path).resolve()
        directory = base / self.path.stem if own else base
        directory = directory / pathlib.Path(*parts) if parts else directory
        file = directory / f"{name}.rs"
        return (file if file.exists() else directory / name / "mod.rs").resolve()


def sources(root, globs):
    found = {}
    for pattern in globs:
        for path in sorted(root.glob(pattern)):
            if "/target/" in path.as_posix():
                continue
            found[path.resolve()] = Source(path.resolve())
    # A file is test code when it is marked, or every declaration naming it is test code or sits
    # in a test file. Marking only grows, so the loop ends within one pass a file.
    declared = {}
    for source in found.values():
        for target, gated in source.imports:
            declared.setdefault(target, []).append((source.path, gated))
    test_files = {p for p, s in found.items() if s.marked}
    for _ in range(len(found) + 1):
        grown = {
            p for p in found
            if p not in test_files and declared.get(p) and all(g or importer in test_files for importer, g in declared[p])
        }
        if not grown:
            break
        test_files |= grown
    for p in test_files:
        found[p].test = [True] * len(found[p].toks)
    return found


def literal_run(toks, j):
    """The index past a literal-only expression at `j`: numbers joined by arithmetic and shifts,
    each optionally cast with `as`."""
    k, want_number = j, True
    while k < len(toks):
        kind, t, _ = toks[k]
        if want_number and kind == "num":
            k, want_number = k + 1, False
        elif not want_number and kind == "id" and t == "as" and k + 1 < len(toks) and toks[k + 1][0] == "id":
            k += 2
        elif not want_number and kind == "p" and t in ("*", "<<", "+", "-", "/", ">>"):
            k, want_number = k + 1, True
        else:
            break
    if want_number:
        k -= 1
    return k


def render(toks):
    """A value's canonical text: its tokens, a space apart, joined where Rust joins them."""
    text = " ".join(t for _, t, _ in toks)
    for a, b in ((" ( ", "("), ("( ", "("), (" )", ")"), (" ,", ","), (" ::", "::"), (":: ", "::"), (" .", "."), (". ", "."), (" [ ", "["), ("[ ", "["), (" ]", "]"), (" ;", ";"), ("& ", "&"), (" : ", ": "), ("::< ", "::<")):
        text = text.replace(a, b)
    return text


def stated(text):
    """A value as a row states it: its canonical text, or the SHA-256 of that text when long."""
    if len(text) <= VALUE_WIDTH:
        return text
    return "sha256:" + hashlib.sha256(text.encode()).hexdigest()[:16]


def numbers(source, rel):
    """Every number of one production file: (key, value, line) for each `const` and `static`
    holding a number and each literal parameter."""
    toks, test = source.toks, source.test
    found = []
    # (closing brace index, label) of the impls, traits, modules and functions open here.
    scopes = []
    pending = None
    skip_to = -1
    counts = {}
    i = 0
    while i < len(toks):
        while scopes and i > scopes[-1][0]:
            scopes.pop()
        kind, t, line = toks[i]
        if test[i]:
            i += 1
            continue
        before = toks[i - 1][1] if i else ""
        if t in ("impl", "trait", "mod") and kind == "id" and before in ITEM_START:
            j = i + 1
            while j < len(toks) and toks[j][1] not in ("{", ";"):
                j += 1
            if j < len(toks) and toks[j][1] == "{":
                head = [x for x in toks[i + 1:j]]
                label = impl_label(head) if t == "impl" else (head[0][1] if head else "")
                scopes.append((matching(toks, j), label))
            i += 1
            continue
        if t == "fn" and kind == "id" and i + 1 < len(toks) and toks[i + 1][0] == "id":
            pending = toks[i + 1][1]
            j = i + 2
            depth = 0
            while j < len(toks):
                x = toks[j][1]
                if x in "([":
                    depth += 1
                elif x in ")]":
                    depth -= 1
                elif depth == 0 and x in ("{", ";"):
                    break
                j += 1
            if j < len(toks) and toks[j][1] == "{":
                scopes.append((matching(toks, j), pending))
            i += 1
            continue
        if t in ("const", "static") and kind == "id" and before in ITEM_START and i + 2 < len(toks) and toks[i + 1][0] == "id" and toks[i + 1][1] != "fn":
            k = i + 1
            if toks[k][1] == "mut":
                k += 1
            name = toks[k][1]
            end = item_end(toks, i)
            eq = next((x for x in range(k, end) if toks[x][1] == "="), None)
            # `const _` is a compile-time assertion, not a value.
            if eq is not None and name != "_" and re.fullmatch(r"[A-Z_][A-Z0-9_]*", name):
                value = toks[eq + 1:end - 1] if toks[end - 1][1] == ";" else toks[eq + 1:end]
                typ = [x[1] for x in toks[k + 2:eq]]
                # A number of its own, or a numeric type's value computed from others; a string,
                # a label and a cell filled at run time are not numbers.
                literal = any(x[0] == "num" for x in value)
                scalar = "".join(typ) in NUMERIC or "".join(typ) == "$word"
                typed = scalar or (any(x in NUMERIC for x in typ) and not any(x[0] == "str" for x in value))
                cell = any(x.startswith(("Atomic", "OnceLock", "Cell", "Mutex")) for x in typ)
                if (literal or typed) and not cell:
                    found.append((name, stated(render(value)), line))
            i = end
            continue
        if kind == "str" and before == "=" and i > 1 and toks[i - 2][1] == "default_value" and re.search(r"[0-9]", t):
            key = "--" + clap_field(toks, i)
            found.append((key, t, line))
            i += 1
            continue
        if t == "[" and i and before not in ("]", ")") and toks[i - 1][0] != "id":
            close = literal_array(toks, i)
            if close is not None:
                text = render(toks[i:close + 1])
                scope = "::".join(label for _, label in scopes if label)
                key = f"{scope}:{text}" if scope else text
                counts[key] = counts.get(key, 0) + 1
                if counts[key] > 1:
                    key = f"{key}#{counts[key]}"
                found.append((stated(key), stated(text), line))
                i = close + 1
                continue
        if kind == "num":
            slot = parameter_slot(toks, i)
            end = literal_run(toks, i)
            text = render(toks[i:end])
            local = slot is not None and before == "=" and assigned_by_let(toks, i - 1)
            if slot and (ZERO.fullmatch(text) or (ONE.fullmatch(text) and (slot in ("min()", "max()", "clamp()", "unwrap_or()", "map_or()") or local))):
                slot = None
            if slot:
                # A call's or a shift's slot has no name of its own: its value names it, so that
                # one more call elsewhere in the function renames none of the others.
                if slot.endswith("()"):
                    slot = f"{slot[:-1]}{text})"
                elif slot == "<<":
                    slot = f"({text})"
                scope = "::".join(label for _, label in scopes if label)
                key = f"{scope}:{slot}" if scope else slot
                counts[key] = counts.get(key, 0) + 1
                if counts[key] > 1:
                    key = f"{key}#{counts[key]}"
                found.append((key, stated(text), line))
                i = end
                continue
        i += 1
    return found


def literal_array(toks, i):
    """The index of the `]` closing a literal array at `i`: two or more literal-only elements, a
    ladder or a table written in code; None for any other bracket."""
    k, elements = i + 1, 0
    while k < len(toks):
        if toks[k][0] != "num":
            return None
        end = literal_run(toks, k)
        elements += 1
        if end >= len(toks):
            return None
        if toks[end][1] == "]":
            return end if elements >= 2 else None
        if toks[end][1] != ",":
            return None
        k = end + 1
        if k < len(toks) and toks[k][1] == "]":
            return k if elements >= 2 else None
    return None


def impl_label(head):
    """The type an impl's header names: the one after `for`, or the first, generics left out."""
    names, depth = [], 0
    after_for = None
    for _, t, _ in head:
        if t == "<":
            depth += 1
        elif t == ">":
            depth -= 1
        elif depth == 0 and t == "for":
            after_for = len(names)
        elif depth == 0 and t == "where":
            break
        elif depth == 0 and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", t) and t not in ("dyn", "mut", "const", "unsafe"):
            names.append(t)
    if after_for is not None and after_for < len(names):
        return names[after_for]
    return names[0] if names else ""


def parameter_slot(toks, i):
    """The slot a number at `i` is the whole value of, or None: `field` of a struct literal,
    `name` of an assignment or binding, `call()` of a count or duration argument, `--name` of a
    command-line default, `<<` of a size shift."""
    prev = toks[i - 1] if i else ("", "", 0)
    prev2 = toks[i - 2] if i > 1 else ("", "", 0)
    prev3 = toks[i - 3] if i > 2 else ("", "", 0)
    if prev[0] == "num" or (prev[0] == "p" and prev[1] in ("*", "<<", "+", "-", "/", ">>", "%", "&", "|", "^", "!")):
        return None
    end = literal_run(toks, i)
    after = toks[end][1] if end < len(toks) else ""
    run = [t for _, t, _ in toks[i:end]]
    if prev[1] == ":" and prev2[0] == "id" and after in (",", "}") and prev3[1] in ("{", ",", "(") :
        return prev2[1]
    if prev[1] == "=" and after == ";":
        return assigned(toks, i - 1)
    if prev[1] == "=" and prev2[1] == "default_value_t":
        return "--" + clap_field(toks, i)
    if prev[1] == "(" and prev2[0] == "id" and prev2[1] in CALLS and after in (")", ","):
        return prev2[1] + "()"
    if "<<" in run and prev[1] in ("(", ",", "=", "{", "[", "return") and after in (")", ",", ";", "}", "]"):
        return "<<"
    return None


def assigned(toks, eq):
    """The name an `=` at `eq` assigns or binds: the binding after `let` (and `mut`), else the
    last identifier of the place."""
    j = eq - 1
    while j >= 0 and toks[j][1] not in (";", "{", "}"):
        j -= 1
    head = toks[j + 1:eq]
    words = [t for _, t, _ in head]
    if words[:1] == ["let"]:
        rest = [x for x in head[1:] if x[1] != "mut"]
        return rest[0][1] if rest and rest[0][0] == "id" else None
    if head and head[-1][0] == "id":
        return head[-1][1]
    return None


def assigned_by_let(toks, eq):
    """Whether the `=` at `eq` is a `let` binding's."""
    j = eq - 1
    while j >= 0 and toks[j][1] not in (";", "{", "}"):
        j -= 1
    return j + 1 < len(toks) and toks[j + 1][1] == "let"


def clap_field(toks, i):
    """The field a `default_value_t` attribute at `i` sets: the first identifier past its `]`."""
    j = i
    while j < len(toks) and toks[j][1] != "]":
        j += 1
    while j < len(toks):
        if toks[j][0] == "id" and toks[j][1] not in ("pub", "crate"):
            if j + 1 < len(toks) and toks[j + 1][1] == ":":
                return toks[j][1]
        j += 1
    return "?"


# Tokens an item may follow: what ends the item before it, an attribute's `]`, a visibility.
ITEM_START = {"", ";", "}", "{", "]", "pub", ")", "unsafe", "async", "extern", "default"}


SCANNED = ("crates/*/src/**/*.rs",) + tuple(f"vendor/{c}/src/**/*.rs" for c in VENDORED)


def production_numbers(root):
    """Every production number in the scanned trees: {(file, key): (value, line)}."""
    found = {}
    for path, source in sources(root, SCANNED).items():
        rel = path.relative_to(root.resolve()).as_posix()
        if all(source.test):
            continue
        for key, value, line in numbers(source, rel):
            found.setdefault((rel, key), (value, line))
    return found


def inventory(text):
    """The inventory's rows {(file, key): (kind, value, basis, line)}, and the rows it cannot read."""
    rows, failures = {}, []
    for n, line in enumerate(text.splitlines(), 1):
        if not TABLE_ROW.match(line):
            continue
        m = ROW.match(line)
        if not m:
            failures.append(f"constants.md:{n}: a row must be | `file` `key` | kind | `value` | basis |")
            continue
        key = (m.group(1), m.group(2))
        if key in rows:
            failures.append(f"constants.md:{n}: {key[0]} {key[1]}: a second row")
        rows[key] = (m.group(3), m.group(5).strip().replace("\\|", "|"), m.group(6).strip(), n)
    return rows, failures


def known_items(root, path):
    if not path.exists():
        return [], [f"{path.relative_to(root)}: missing"]
    items = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            file, _, key = line.partition(" ")
            items.append((file, key.strip()))
    return items, []


def check_constants(root, found, known, ceiling):
    """The inventory's failures, and the open items (rows of a refused kind)."""
    inventory_path = root / "docs/design/constants.md"
    if not inventory_path.exists():
        return [f"{inventory_path.relative_to(root)}: missing"], []
    rows, failures = inventory(inventory_path.read_text(encoding="utf-8"))
    for (rel, key), (value, line) in sorted(found.items()):
        row = rows.get((rel, key))
        if row is None:
            failures.append(f"{rel}:{line}: {key} = {value} has no row in docs/design/constants.md")
        elif row[1] != value:
            failures.append(f"constants.md:{row[3]}: {rel} {key}: the row says `{row[1]}`, the code `{value}` ({rel}:{line})")
    for (rel, key), (kind, _, basis, n) in sorted(rows.items()):
        if (rel, key) not in found:
            failures.append(f"constants.md:{n}: {rel} {key} names no number in production code")
        if kind not in ACCEPTED and kind not in REFUSED:
            failures.append(f"constants.md:{n}: {rel} {key}: unknown kind {kind}")
        elif kind in ACCEPTED:
            for pattern, what in REFUSED_BASES:
                if pattern.search(basis):
                    failures.append(f"constants.md:{n}: {rel} {key}: a {kind} row whose basis is {what}")
            if kind not in ("format", "external") and OTHER_SYSTEMS.search(basis) and not PRIMARY.search(basis):
                failures.append(f"constants.md:{n}: {rel} {key}: a {kind} row whose basis is another system's tuning ({OTHER_SYSTEMS.search(basis).group(0)})")
    open_items = sorted((rel, key) for (rel, key), row in rows.items() if row[0] in REFUSED)
    if known is not None:
        items, missing = known_items(root, known)
        failures.extend(missing)
        listed = set(items)
        try:
            name = known.relative_to(root)
        except ValueError:
            name = known
        if len(items) != len(listed):
            failures.append(f"{name}: an item listed twice")
        for rel, key in sorted(set(open_items) - listed):
            failures.append(f"{rel} {key}: a new open item, of kind {rows[(rel, key)][0]}: give it a basis the rule accepts")
        for rel, key in sorted(listed - set(open_items)):
            failures.append(f"{name}: {rel} {key} is no longer open: take it off the list and lower OPEN_CEILING")
        if len(items) > ceiling:
            failures.append(f"{name}: {len(items)} open items, more than OPEN_CEILING {ceiling}")
        elif len(items) < ceiling:
            failures.append(f"{name}: {len(items)} open items: lower OPEN_CEILING from {ceiling} to {len(items)}")
    else:
        for rel, key in open_items:
            kind, value, _, n = rows[(rel, key)]
            failures.append(f"constants.md:{n}: {rel} {key} = {value}: {kind}, which the rule refuses ({REFUSED[kind]})")
    return failures, open_items


def contracts(root, known, ceiling):
    """Every failure of the tree at `root`, its production numbers and its open items."""
    failures = []
    for path in sorted(root.glob("crates/**/*.rs")):
        rel = path.relative_to(root).as_posix()
        text = path.read_text(encoding="utf-8")
        if rel in UNSAFE_ALLOWED:
            continue
        if ALLOW_UNSAFE.search(text):
            failures.append(f"{rel}: allows unsafe_code but is not in UNSAFE_ALLOWED")
        elif UNSAFE_BLOCK.search(text):
            failures.append(f"{rel}: uses unsafe but is not in UNSAFE_ALLOWED")
    for rel in UNSAFE_ALLOWED:
        if not (root / rel).exists():
            failures.append(f"{rel}: listed in UNSAFE_ALLOWED but missing")
    found = production_numbers(root)
    inventory_failures, open_items = check_constants(root, found, known, ceiling)
    return failures + inventory_failures, found, open_items


# The rules above on small trees: each tree's files, its inventory rows, its known open items,
# and what the checks must say of it (a phrase of the refusal; None for a pass). A fixture's
# MISSED never has a row.
ROW_OF = "| `crates/fixture/src/lib.rs` `{}` | {} | `{}` | {} |\n"
NO_ROW = "MISSED = 1 has no row"
FIXTURES = {
    "ordinary_production": ({"lib.rs": "const MISSED: u64 = 1;\n"}, "", "", NO_ROW),
    "marked_test_file": (
        {
            "lib.rs": '#[cfg(test)]\n#[path = "checks.rs"]\nmod checks;\n',
            "checks.rs": "#![cfg(test)]\nconst MISSED: u64 = 1;\n",
        },
        "", "", None,
    ),
    "marked_after_its_docs": (
        {"lib.rs": "mod checks;\n", "checks.rs": "//! Checks.\n\n#![cfg(test)]\nconst MISSED: u64 = 1;\n"},
        "", "", None,
    ),
    # Every declaration of the file is test code, so rustc compiles it only in test builds.
    "only_test_declarations": (
        {"lib.rs": '#[cfg(test)]\n#[path = "shared.rs"]\nmod checks;\n', "shared.rs": "const MISSED: u64 = 1;\n"},
        "", "", None,
    ),
    "a_production_declaration_too": (
        {
            "lib.rs": '#[cfg(test)]\n#[path = "shared.rs"]\nmod checks;\n#[path = "shared.rs"]\nmod real;\n',
            "shared.rs": "const MISSED: u64 = 1;\n",
        },
        "", "", "shared.rs:1: MISSED = 1 has no row",
    ),
    "unmarked_shared_include": (
        {
            "lib.rs": 'include!("shared.rs");\n#[cfg(test)]\n#[path = "shared.rs"]\nmod checks;\n',
            "shared.rs": "const MISSED: u64 = 1;\n",
        },
        "", "", NO_ROW,
    ),
    "marker_not_first": ({"lib.rs": "mod checks;\n", "checks.rs": "const MISSED: u64 = 1;\n#![cfg(test)]\n"}, "", "", NO_ROW),
    "a_different_inner_attribute": ({"lib.rs": "mod checks;\n", "checks.rs": "#![cfg(unix)]\nconst MISSED: u64 = 1;\n"}, "", "", NO_ROW),
    "a_test_function": ({"lib.rs": "#[cfg(test)]\nfn f() { let missed = 3; }\n"}, "", "", None),
    "production_after_a_test_module": (
        {"lib.rs": "#[cfg(test)]\nmod tests {\n    const T: u8 = 2;\n}\nconst MISSED: u64 = 1;\n"},
        "", "", NO_ROW,
    ),
    "all_test_and_unix": ({"lib.rs": "#[cfg(all(unix, test))]\nconst MISSED: u64 = 1;\n"}, "", "", None),
    "not_test": ({"lib.rs": "#[cfg(not(test))]\nconst MISSED: u64 = 1;\n"}, "", "", NO_ROW),
    "a_struct_constant": ({"lib.rs": "const MISSED: Rounds = Rounds { min: 3 };\n"}, "", "", "MISSED = Rounds { min: 3 } has no row"),
    "a_literal_field": ({"lib.rs": "fn plan() -> Plan { Plan { depth: 4 } }\n"}, "", "", "plan:depth = 4 has no row"),
    "a_command_line_default": (
        {"lib.rs": "struct A {\n    #[arg(long, default_value_t = 16)]\n    clients: usize,\n}\n"},
        "", "", "--clients = 16 has no row",
    ),
    "a_zero_is_no_choice": ({"lib.rs": "fn f() -> S { S { count: 0 } }\n"}, "", "", None),
    "a_ladder": ({"lib.rs": "fn f() -> Vec<u32> { vec![1, 4, 16] }\n"}, "", "", "f:[1, 4, 16] = [1, 4, 16] has no row"),
    "a_drifted_value": ({"lib.rs": "const KEPT: u64 = 352;\n"}, ROW_OF.format("KEPT", "counted", "346", "Counted."), "", "the row says `346`, the code `352`"),
    "an_unlisted_open_item": ({"lib.rs": "const KEPT: u64 = 8;\n"}, ROW_OF.format("KEPT", "picked", "8", "Picked."), "", "a new open item, of kind picked"),
    "a_listed_open_item": ({"lib.rs": "const KEPT: u64 = 8;\n"}, ROW_OF.format("KEPT", "picked", "8", "Picked."), "crates/fixture/src/lib.rs KEPT\n", None),
    "a_list_that_did_not_shrink": ({"lib.rs": "const KEPT: u64 = 8;\n"}, ROW_OF.format("KEPT", "derived", "8", "Derived."), "crates/fixture/src/lib.rs KEPT\n", "is no longer open"),
    "a_knee_called_derived": ({"lib.rs": "const KEPT: u64 = 8;\n"}, ROW_OF.format("KEPT", "derived", "8", "The knee of the curve."), "", "whose basis is a knee"),
    "a_copy_called_cited": ({"lib.rs": "const KEPT: u64 = 10;\n"}, ROW_OF.format("KEPT", "cited", "10", "RocksDB's default."), "", "another system's tuning (RocksDB)"),
    "a_row_of_nothing": ({"lib.rs": ""}, ROW_OF.format("GONE", "format", "1", "Gone."), "", "GONE names no number"),
}


def fixtures():
    """Runs these checks on each fixture tree: the failures where they say other than they must."""
    failures = []
    with tempfile.TemporaryDirectory(prefix="check-contracts-") as directory:
        for name, (files, rows, listed, expect) in FIXTURES.items():
            root = pathlib.Path(directory, name).resolve()
            tree = {
                "scripts/arbitrary-open.txt": listed,
                "docs/design/constants.md": rows,
                **{rel: "" for rel in UNSAFE_ALLOWED},
                **{f"crates/fixture/src/{rel}": text for rel, text in files.items()},
            }
            for rel, text in tree.items():
                (root / rel).parent.mkdir(parents=True, exist_ok=True)
                (root / rel).write_text(text, encoding="utf-8")
            said, _, _ = contracts(root, root / "scripts/arbitrary-open.txt", len(listed.splitlines()))
            ok = (not said) if expect is None else any(expect in f for f in said)
            if not ok:
                failures.append(
                    f"scripts/check-contracts.py: fixture {name}: "
                    + (f"did not say {expect!r}" if expect else "did not pass")
                    + f" ({'; '.join(said) or 'nothing said'})"
                )
    return failures


def main() -> int:
    args = sys.argv[1:]
    known = None
    if args[:1] == ["--known"] and len(args) == 2:
        known = pathlib.Path(args[1]).resolve()
    elif args:
        print("usage: check-contracts.py [--known scripts/arbitrary-open.txt]", file=sys.stderr)
        return 2
    failures = fixtures()
    said, found, open_items = contracts(ROOT, known, OPEN_CEILING)
    failures.extend(said)
    for failure in failures:
        print(failure, file=sys.stderr)
    print(f"check-contracts: {len(found)} production numbers inventoried, {len(open_items)} open", file=sys.stderr if failures else sys.stdout)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
