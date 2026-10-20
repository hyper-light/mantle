#!/usr/bin/env python3
"""Snapshots crates from hyper-raft into vendor/, by the rules vendor/UPSTREAM.md states.

    scripts/snapshot-hyper-raft.py REPO REV OUT CRATE...      (Python 3.11 or later, for tomllib)

For each CRATE, `crates/CRATE` at REV is exported from REPO's objects (`git archive`, nothing from
a working tree) into OUT/CRATE, replacing what was there, and made a root of its own:

- the manifest's workspace-inherited package fields and dependency specs are written out from the
  repository's root manifest at REV, `publish = false` added; an inherited dependency's features are
  the crate's own followed by the workspace's it does not name;
- its `[[bench]]` tables are dropped, and its `[lints]`, keeping only the declaration of the cfgs a
  crate's interleaving tests select where its sources use them;
- a path dependency on a crate not in the snapshot, and loom's target table, are commented out with
  a note, since hyper-raft's CI runs the tests that take them;
- `benches/` is dropped but for files a test or source includes by `#[path]`;
- an empty `[workspace]` is appended, and `SNAPSHOT` names the revision and path.

A snapshot is never edited by hand afterwards: a change is made in hyper-raft and taken by running
this again.
"""
import io
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tomllib

NOT_VENDORED = "# Not vendored here; hyper-raft's CI runs the tests that take it."
ROOT_NOTE = ("# A root of its own: a vendored snapshot of hyper-raft (SNAPSHOT), outside any "
             "consumer workspace.")
CFG_NOTE = ("# hyper-raft's workspace declares the cfgs its interleaving and schedule tests select; "
            "the\n# snapshot keeps that declaration alone of the workspace's lints, so `cfg(loom)` "
            "is known here.")
SELECTED_CFGS = ("loom", "shuttle", "fuzzing")


def git(repo, *args, binary=False):
    out = subprocess.run(["git", "-C", repo, *args], check=True, capture_output=True)
    return out.stdout if binary else out.stdout.decode()


def toml_value(v):
    """A TOML literal for the plain values a manifest's specs hold."""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, str):
        return '"' + v.replace("\\", "\\\\").replace('"', '\\"') + '"'
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, list):
        return "[" + ", ".join(toml_value(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{ " + ", ".join(f"{k} = {toml_value(x)}" for k, x in v.items()) + " }"
    raise ValueError(f"no TOML literal for {v!r}")


def spec_table(spec):
    return {"version": spec} if isinstance(spec, str) else dict(spec)


def inherit(name, crate_spec, workspace_deps):
    """`name`'s dependency spec with the workspace's written in: the crate's own keys win, and
    its features come first, the workspace's it does not name after them."""
    if name not in workspace_deps:
        raise SystemExit(f"{name} inherits from the workspace, which does not declare it")
    out = spec_table(workspace_deps[name])
    own = {k: v for k, v in crate_spec.items() if k != "workspace"}
    features = list(own.pop("features", []))
    for f in out.get("features", []):
        if f not in features:
            features.append(f)
    out.update(own)
    if features:
        out["features"] = features
    if list(out) == ["version"]:
        return toml_value(out["version"])
    return toml_value(out)


def bracket_balance(text):
    return text.count("[") - text.count("]") + text.count("{") - text.count("}")


def logical_lines(lines):
    """Groups a manifest's physical lines into its items: an assignment whose array or table
    spans several lines is one item."""
    i = 0
    while i < len(lines):
        line = lines[i]
        item = [line]
        stripped = line.split("#", 1)[0] if not line.lstrip().startswith("#") else ""
        depth = bracket_balance(stripped) if "=" in stripped else 0
        while depth > 0 and i + 1 < len(lines):
            i += 1
            item.append(lines[i])
            depth += bracket_balance(lines[i].split("#", 1)[0])
        yield item
        i += 1


def transform_manifest(text, workspace, vendored, uses_cfgs):
    package = workspace.get("package", {})
    deps = workspace.get("dependencies", {})
    out = []
    section = None
    skip_section = False
    pending_comments = []
    for item in logical_lines(text.splitlines()):
        first = item[0]
        header = re.match(r"^\s*\[\[?([^\]]+)\]\]?\s*$", first)
        if header:
            # Comments right above a header belong to it: they go or stay with its section.
            name = header.group(1).strip()
            section = name
            if first.strip().startswith("[[bench"):
                skip_section = True
                pending_comments = []
                continue
            if name == "lints" or name.startswith("lints."):
                skip_section = True
                pending_comments = []
                continue
            skip_section = False
            if re.match(r"target\.'cfg\(loom\)'\.dependencies", name):
                out.extend(pending_comments)
                pending_comments = []
                out.append("# Not vendored here: loom, which hyper-raft's CI runs for the "
                           "interleavings its `cfg(loom)` tests select.")
                out.append("# " + first)
                skip_section = "comment"
                continue
            out.extend(pending_comments)
            pending_comments = []
            out.append(first)
            continue
        if skip_section == "comment":
            out.extend(("# " + l if l.strip() else l) for l in item)
            continue
        if skip_section:
            continue
        if first.strip().startswith("#") or not first.strip():
            pending_comments.extend(item)
            continue
        out.extend(pending_comments)
        pending_comments = []
        key = first.split("=", 1)[0].strip()
        if section == "package":
            m = re.match(r"^(\s*)([\w-]+)\.workspace\s*=\s*true\s*$", first)
            if m:
                field = m.group(2)
                if field not in package:
                    raise SystemExit(f"package.{field} inherits from a workspace that lacks it")
                out.append(f"{m.group(1)}{field} = {toml_value(package[field])}")
                continue
            out.extend(item)
            continue
        if section and (section.endswith("dependencies")):
            m = re.match(r"^(\s*)([\w-]+)\.workspace\s*=\s*true\s*$", first)
            if m:
                out.append(f"{m.group(1)}{m.group(2)} = {inherit(m.group(2), {}, deps)}")
                continue
            parsed = tomllib.loads("\n".join(item))
            name, spec = next(iter(parsed.items()))
            if isinstance(spec, dict) and spec.get("workspace") is True:
                out.append(f"{name} = {inherit(name, spec, deps)}")
                continue
            if isinstance(spec, dict) and "path" in spec:
                target = os.path.basename(os.path.normpath(spec["path"]))
                # A crate's dependency on itself, as hyper-raft's tests take it with features.
                if os.path.normpath(spec["path"]) != "." and target not in vendored:
                    if not section.endswith("dev-dependencies"):
                        raise SystemExit(f"{name} is a dependency on {target}, not in the snapshot")
                    out.append(NOT_VENDORED)
                    out.extend("# " + l for l in item)
                    continue
            out.extend(item)
            continue
        out.extend(item)
    out.extend(pending_comments)
    while out and not out[-1].strip():
        out.pop()
    publish_unstated(out)
    if uses_cfgs:
        out.append("")
        out.append(CFG_NOTE)
        out.append("[lints.rust]")
        cfgs = ", ".join(f'"cfg({c})"' for c in SELECTED_CFGS)
        out.append(f'unexpected_cfgs = {{ level = "warn", check-cfg = [{cfgs}] }}')
    out.append("")
    out.append(ROOT_NOTE)
    out.append("[workspace]")
    return "\n".join(out) + "\n"


def publish_unstated(lines):
    """`publish = false` at the end of `[package]` where the crate does not state it."""
    start = next((i for i, l in enumerate(lines) if l.strip() == "[package]"), None)
    if start is None:
        return
    end = next((i for i in range(start + 1, len(lines)) if lines[i].lstrip().startswith("[")),
               len(lines))
    if any(re.match(r"^\s*publish\s*=", l) for l in lines[start:end]):
        return
    at = end
    while at > start + 1 and (not lines[at - 1].strip() or lines[at - 1].lstrip().startswith("#")):
        at -= 1
    lines.insert(at, "publish = false")


def included_benches(root):
    """Files under benches/ that a source includes by `#[path]`."""
    keep = set()
    for dirpath, _, files in os.walk(root):
        if os.path.relpath(dirpath, root).split(os.sep)[0] == "benches":
            continue
        for f in files:
            if not f.endswith(".rs"):
                continue
            p = os.path.join(dirpath, f)
            for m in re.finditer(r'#\[path\s*=\s*"([^"]+)"\]', open(p, encoding="utf-8").read()):
                target = os.path.normpath(os.path.join(dirpath, m.group(1)))
                rel = os.path.relpath(target, root)
                if rel.split(os.sep)[0] == "benches":
                    keep.add(rel)
    return keep


def uses_selected_cfgs(root):
    pattern = re.compile(r"cfg\((?:[^()]*\()?\s*(?:" + "|".join(SELECTED_CFGS) + r")\b")
    for dirpath, _, files in os.walk(root):
        for f in files:
            if f.endswith(".rs") and pattern.search(open(os.path.join(dirpath, f), encoding="utf-8").read()):
                return True
    return False


def main():
    if len(sys.argv) < 5:
        raise SystemExit(__doc__)
    repo, rev, out_dir, crates = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:]
    sha = git(repo, "rev-parse", f"{rev}^{{commit}}").strip()
    workspace = tomllib.loads(git(repo, "show", f"{sha}:Cargo.toml")).get("workspace", {})
    vendored = set(crates)
    for crate in crates:
        src = f"crates/{crate}"
        archive = git(repo, "archive", "--format=tar", sha, src, binary=True)
        dest = os.path.join(out_dir, crate)
        if os.path.exists(dest):
            shutil.rmtree(dest)
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
            for member in tar.getmembers():
                rel = os.path.relpath(member.name, src)
                if rel == "." or rel.startswith(".."):
                    continue
                if member.isdir():
                    continue
                target = os.path.join(dest, rel)
                os.makedirs(os.path.dirname(target), exist_ok=True)
                with open(target, "wb") as f:
                    f.write(tar.extractfile(member).read())
                os.chmod(target, member.mode & 0o777)
        keep = included_benches(dest)
        benches = os.path.join(dest, "benches")
        if os.path.isdir(benches):
            for dirpath, _, files in os.walk(benches, topdown=False):
                for f in files:
                    rel = os.path.relpath(os.path.join(dirpath, f), dest)
                    if rel not in keep:
                        os.remove(os.path.join(dirpath, f))
                if not os.listdir(dirpath):
                    os.rmdir(dirpath)
        manifest = os.path.join(dest, "Cargo.toml")
        text = open(manifest, encoding="utf-8").read()
        with open(manifest, "w", encoding="utf-8") as f:
            f.write(transform_manifest(text, workspace, vendored, uses_selected_cfgs(dest)))
        with open(os.path.join(dest, "SNAPSHOT"), "w", encoding="utf-8") as f:
            f.write(f"hyper-raft {sha} {src}\n")
        print(f"{crate} {sha[:7]}")


if __name__ == "__main__":
    main()
