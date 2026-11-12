#!/usr/bin/env python3
"""The release's archives (docs/design/release.md; .github/workflows/release.yml).

`package VERSION TARGET BINARY OUT` puts the `mantle` binary built for TARGET in OUT as the
archive `mantle-VERSION-TARGET.tar.gz` (`.zip` for Windows) beside the repository's LICENSE and
README.md under one directory of the archive's own name, and writes the archive's SHA-256 beside
it in `sha256sum`'s form. Every entry carries the commit's time and no owner, so the same binary
packs to the same bytes.

`check VERSION TARGET ARCHIVE` proves an archive as a user takes it: its SHA-256 and its entries,
then the binary it holds, which must say VERSION. That binary makes a cell of three on loopback
(`mantle init`, each founder's ports stated) and serves it (three `mantle serve` processes, each
waited for until it says the cell's root range enlisted it, and ready to `mantle status`). As the
client of a directory `mantle init client` made, its object commands ask each door in turn, the
native QUIC door (`--door native`) and the S3 HTTP door (`--door s3`, SigV4-signed with the profile
`mantle init client` gave the client): `mb`, `ls`, `cp` up and back from a file and from standard
input below the AWS CLI's multipart threshold and past it, a copy killed once it committed and
resumed by running it again, `head`, a multipart upload's every step, `rm` and `rb`. Each object is
read back through the other door byte for byte, and both doors list and head alike; the door named
by `MANTLE_DOOR` and the client by `MANTLE_CLIENT` where no flag names them. The AWS CLI v2 asks
the S3 HTTP door with the profile `mantle init` wrote: `mb`, `cp` of an object below its multipart
threshold and one past it, up and back, `ls`, `head-object`, `rm` and `rb`, and reads back what each
of the client's doors put. Each object is compared byte for byte with what was put; one node
then stops in order. Last, the binary's own transfer harness at the scale CI runs it
(crates/mantle/tests/transfer.rs), which checks every byte through both doors with boto3 and the
AWS CLI while it kills and restarts each founder. The AWS CLI and boto3 are those
scripts/stock-clients.sh installs.

Every wait ends on what a process says or on its end, never on a guess of time; the CI job's own
time limit bounds the whole.
"""

import argparse
import gzip
import hashlib
import os
import random
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# The AWS CLI's multipart threshold and part size, 8 MiB each (docs/research/73 [10]): a file past
# it goes up in parts. A native copy from standard input is read in runs of the same 8 MiB
# (docs/design/gateway.md §6).
CLI_PART = 8 << 20
# S3's least part but the last, 5 MiB (docs/research/05 §4.1).
LEAST_PART = 5 << 20
# Each founder's volume: the bytes crates/mantle/tests/support/cell.rs gives each founder of the
# suites' cells, and the transfer harness's default.
VOLUME = "256M"


class Failed(Exception):
    """A check that did not hold, said as the reason."""


def archive_names(version, target):
    """The archive's directory and file names for VERSION and TARGET."""
    stem = f"mantle-{version}-{target}"
    return stem, stem + (".zip" if "windows" in target else ".tar.gz")


def binary_name(target):
    return "mantle.exe" if "windows" in target else "mantle"


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def package(version, target, binary, out):
    stem, name = archive_names(version, target)
    entries = [
        (binary_name(target), Path(binary), 0o755),
        ("LICENSE", ROOT / "LICENSE", 0o644),
        ("README.md", ROOT / "README.md", 0o644),
    ]
    made = subprocess.run(
        ["git", "-C", str(ROOT), "log", "-1", "--format=%ct", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    epoch = int(made.stdout.strip())
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    archive = out / name
    if name.endswith(".zip"):
        stamp = time.gmtime(epoch)[:6]
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as z:
            for entry, source, mode in entries:
                info = zipfile.ZipInfo(f"{stem}/{entry}", date_time=stamp)
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = (0o100000 | mode) << 16
                with open(source, "rb") as f, z.open(info, "w") as w:
                    shutil.copyfileobj(f, w, 1 << 20)
    else:
        with open(archive, "wb") as raw, gzip.GzipFile(
            filename="", mode="wb", fileobj=raw, mtime=epoch
        ) as packed, tarfile.open(fileobj=packed, mode="w", format=tarfile.PAX_FORMAT) as tar:

            def info(path, mode, kind, size=0):
                entry = tarfile.TarInfo(path)
                entry.type, entry.mode, entry.size, entry.mtime = kind, mode, size, epoch
                entry.uid = entry.gid = 0
                entry.uname = entry.gname = ""
                return entry

            tar.addfile(info(stem, 0o755, tarfile.DIRTYPE))
            for entry, source, mode in entries:
                with open(source, "rb") as f:
                    size = os.fstat(f.fileno()).st_size
                    tar.addfile(info(f"{stem}/{entry}", mode, tarfile.REGTYPE, size), f)
    digest = sha256(archive)
    # Bytes, not text: a line `sha256sum --check` reads on any host, with no CR from Windows.
    (out / f"{name}.sha256").write_bytes(f"{digest}  {name}\n".encode())
    print(f"{archive}: {archive.stat().st_size} bytes, sha256 {digest}")


def unpack(archive, stem, target, into):
    """The binary ARCHIVE holds, unpacked under INTO once its entries are exactly the package's."""
    files = {f"{stem}/{binary_name(target)}", f"{stem}/LICENSE", f"{stem}/README.md"}
    if archive.name.endswith(".zip"):
        with zipfile.ZipFile(archive) as z:
            names = set(z.namelist())
            if names != files:
                raise Failed(f"{archive.name} holds {sorted(names)}, not {sorted(files)}")
            z.extractall(into)
    else:
        with tarfile.open(archive) as tar:
            members = {m.name: m for m in tar.getmembers()}
            if set(members) != files | {stem}:
                raise Failed(f"{archive.name} holds {sorted(members)}, not {sorted(files | {stem})}")
            if not members[stem].isdir() or not all(members[f].isfile() for f in files):
                raise Failed(f"{archive.name}: its directory and three regular files")
            mode = members[f"{stem}/{binary_name(target)}"].mode
            if mode != 0o755:
                raise Failed(f"{archive.name}: the binary's mode is {mode:o}, not 755")
            # The data filter where this Python has it (PEP 706): no entry leaves INTO.
            if hasattr(tarfile, "data_filter"):
                tar.extractall(into, filter="data")
            else:
                tar.extractall(into)
    return into / stem / binary_name(target)


def run(args, input=None, env=None, ok=True):
    """ARGS run to their end, INPUT on standard input: the run, which must have exited 0 unless
    OK is false."""
    ran = subprocess.run(
        [str(a) for a in args],
        input=input if input is not None else b"",
        capture_output=True,
        env=env,
    )
    if ok and ran.returncode != 0:
        raise Failed(
            f"{' '.join(str(a) for a in args)}: exit {ran.returncode}\n"
            f"{ran.stdout.decode(errors='replace')}{ran.stderr.decode(errors='replace')}"
        )
    return ran


def said(ran):
    return ran.stdout.decode()


def object_bytes(seed, size):
    return random.Random(seed).randbytes(size)


def same(what, got, want):
    if got != want:
        raise Failed(f"{what}: {len(got)} bytes came back where {len(want)} were put, or others")


def free_addresses(count):
    """COUNT founders' loopback addresses, each a QUIC and a plane port (UDP) and an S3 port (TCP)
    the operating system gives free, all held until every one is drawn so none repeats."""
    held = []

    def port(kind):
        s = socket.socket(socket.AF_INET, kind)
        held.append(s)
        s.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{s.getsockname()[1]}"

    try:
        return [
            (port(socket.SOCK_DGRAM), port(socket.SOCK_DGRAM), port(socket.SOCK_STREAM))
            for _ in range(count)
        ]
    finally:
        for s in held:
            s.close()


class Node:
    """A founder's `mantle serve` process. A thread reads what it says into its log and keeps the
    facts the check waits on: where it bound, that it enlisted, its orderly stop, and its end."""

    def __init__(self, mantle, name, config, logs):
        self.name = name
        self.out = logs / f"{name}.out"
        self.err = logs / f"{name}.err"
        self.facts = {}
        self.changed = threading.Condition()
        with open(self.err, "wb") as err:
            self.process = subprocess.Popen(
                [str(mantle), "serve", str(config)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=err,
            )
        self.reader = threading.Thread(target=self.read, daemon=True)
        self.reader.start()

    def read(self):
        with open(self.out, "wb") as log:
            for raw in self.process.stdout:
                log.write(raw)
                log.flush()
                words = raw.decode(errors="replace").split()
                fact = None
                if len(words) == 3 and words[0] in ("bound", "s3-bound"):
                    fact = (words[0], tuple(words[1:]))
                elif words in (["enlisted"], ["stopping"]):
                    fact = (words[0], True)
                elif len(words) == 2 and words[0] == "stopped":
                    fact = ("stopped", words[1])
                if fact is not None:
                    with self.changed:
                        self.facts[fact[0]] = fact[1]
                        self.changed.notify_all()
        with self.changed:
            self.facts["ended"] = True
            self.changed.notify_all()

    def until(self, fact):
        """What the node said of FACT, waited for; refused if its output ends first."""
        with self.changed:
            while fact not in self.facts:
                if "ended" in self.facts:
                    raise Failed(f"{self.name}'s process ended before it said {fact}")
                self.changed.wait()
            return self.facts[fact]

    def stop(self):
        """Asks the orderly stop with a `stop` line, as on every host (SIGTERM is Unix's alone),
        and waits for its end: its exit status."""
        self.process.stdin.write(b"stop\n")
        self.process.stdin.flush()
        self.until("stopping")
        self.until("stopped")
        status = self.process.wait()
        self.reader.join()
        return status

    def end(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait()
        self.process.stdin.close()
        self.reader.join()

    def said(self):
        lines = []
        for path in (self.out, self.err):
            text = path.read_text(errors="replace") if path.exists() else ""
            tail = text.splitlines()[-60:]
            lines.append(f"== {path.name}, its last {len(tail)} lines\n" + "\n".join(tail))
        return "\n".join(lines)


def session(mantle, root):
    """The cell of three, its doors asked as a user asks them."""
    volumes = [root / f"volume-{n}" for n in (1, 2, 3)]
    for volume in volumes:
        volume.mkdir(parents=True)
    addresses = free_addresses(len(volumes))
    out = root / "cell"
    args = ["init", out, "--max-nodes", len(volumes), "--max-volumes", len(volumes)]
    for n, ((quic, plane, s3), volume) in enumerate(zip(addresses, volumes), 1):
        args += ["--node", f"quic={quic},plane={plane},s3={s3},domain=rack-{n},volume={volume}:{VOLUME}"]
    made = said(run([mantle, *args]))
    print(made, end="")
    # The Nth `node` line is the Nth --node's founder; a path ends at a colon and a space, never
    # at a drive's colon.
    dirs = [
        Path(line.split(" ", 2)[2].split(": ", 1)[0])
        for line in made.splitlines()
        if line.startswith("node ")
    ]
    if len(dirs) != len(volumes):
        raise Failed(f"mantle init said {len(dirs)} founders, not {len(volumes)}")
    if "s3 profile mantle: AWS_CONFIG_FILE=" not in made:
        raise Failed("mantle init wrote no S3 profile naming the endpoint")

    logs = root / "logs"
    logs.mkdir()
    nodes = []
    try:
        for n, d in enumerate(dirs, 1):
            nodes.append(Node(mantle, f"node-{n}", d / "config", logs))
        for node, (quic, plane, s3) in zip(nodes, addresses):
            if node.until("bound") != (quic, plane):
                raise Failed(f"{node.name} bound {node.facts['bound']}, not {(quic, plane)}")
            if node.until("s3-bound")[0] != s3:
                raise Failed(f"{node.name}'s S3 listener bound {node.facts['s3-bound'][0]}, not {s3}")
        for node in nodes:
            node.until("enlisted")
        for node, d in zip(nodes, dirs):
            run([mantle, "status", d])
        print("the cell of three serves: each founder enlisted, and ready to `mantle status`")
        client = root / "client"
        print(said(run([mantle, "init", "client", client, "--cell", out])), end="")
        doors(mantle, client, out / "s3", root / "objects")
        # Its peers serving on, a founder stops in order: its ranges handed over, exiting 0.
        status = nodes[0].stop()
        if status != 0:
            raise Failed(f"{nodes[0].name} stopped in order and exited {status}")
        print(f"{nodes[0].name} stopped in order, exit 0")
    except BaseException:
        for node in nodes:
            print(node.said(), file=sys.stderr)
        raise
    finally:
        for node in nodes:
            node.end()


# The doors a client's object commands ask the cell through (`mantle --door`).
DOORS = ("native", "s3")


def committed(lines, at):
    """Each `AT: run N committed, D bytes durable` a copy through the native door said, or `AT:
    part N committed, ...` through the S3 door: (N, D), in order."""
    runs = []
    for line in lines:
        words = line.split()
        if (
            words[:1] == [f"{at}:"]
            and words[1:2] in (["run"], ["part"])
            and len(words) == 7
            and words[3] == "committed,"
            and words[5:] == ["bytes", "durable"]
        ):
            runs.append((int(words[2]), int(words[4])))
    return runs


def resume(mantle, client, door, at, data):
    """A copy of DATA from standard input through DOOR, its length stated, killed once the cell
    committed a run or a part (SIGKILL, TerminateProcess on Windows), then run again
    (docs/design/gateway.md §6, docs/design/doors.md): it says it resumes at the offset the cell
    holds, commits each run or part after and none before, and AT holds DATA, its ETag DATA's MD5
    as one PUT's is through the native door, the MD5 of its parts' MD5s and their count through the
    S3 door's multipart upload. What the killed copy said last it committed is what the cell lists
    of the open upload and where the copy run again goes on.

    Standard input is fed a piece at a time, and no piece follows once a run or a part is said
    committed, so the copy holds at most its run or part and a piece past it when it is killed: far
    short of DATA, which is four of the AWS CLI's parts long."""
    # A fifth of S3's least part, the least a run can be (crates/meta/src/name.rs, run_length): the
    # copy is fed less than one run past its first before the feed stops.
    piece = LEAST_PART // 5
    unit = "runs" if door == "native" else "parts"
    command = [mantle, "--client", client, "--door", door, "cp", "-", at, "--size", len(data)]
    copying = subprocess.Popen(
        [str(a) for a in command],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    lines, changed, fed = [], threading.Condition(), threading.Event()

    def heard():
        for raw in copying.stderr:
            with changed:
                lines.append(raw.decode(errors="replace").rstrip())
                changed.notify_all()
        with changed:
            lines.append(None)
            changed.notify_all()

    def feed():
        # A write to a copy killed meanwhile fails: that ends the feed.
        try:
            for start in range(0, len(data), piece):
                if fed.is_set():
                    return
                copying.stdin.write(data[start : start + piece])
                copying.stdin.flush()
        except OSError:
            return

    hearing = threading.Thread(target=heard, daemon=True)
    feeding = threading.Thread(target=feed, daemon=True)
    hearing.start()
    feeding.start()
    with changed:
        while not committed([l for l in lines if l is not None], at):
            if None in lines:
                raise Failed(f"the copy into {at} ended before it committed any: {lines}")
            changed.wait()
    fed.set()
    copying.kill()
    copying.wait()
    feeding.join()
    hearing.join()
    try:
        copying.stdin.close()
    except OSError:
        pass
    said_killed = [l for l in lines if l is not None]
    runs, offset = committed(said_killed, at)[-1]

    def asked(*args):
        return said(run([mantle, "--client", client, "--door", door, *args]))

    # The cell holds the open upload the killed copy left, its parts what it said committed.
    bucket, key = at[len("s3://"):].split("/", 1)
    uploads = asked("multipart", "uploads", f"s3://{bucket}")
    open_ids = [w[1] for w in (l.split() for l in uploads.splitlines()) if len(w) == 3 and w[2] == key]
    if len(open_ids) != 1:
        raise Failed(f"multipart uploads lists {len(open_ids)} open uploads of {key}: {uploads}")
    parts = asked("multipart", "parts", at, open_ids[0])
    sizes = [int(l.split()[1]) for l in parts.splitlines()]
    if len(sizes) != runs or sum(sizes) != offset:
        raise Failed(f"the killed copy said {runs} {unit}, {offset} bytes durable; the cell lists {parts}")

    again = run(command, input=data)
    lines = again.stderr.decode(errors="replace").splitlines()
    first = next((n for n, l in enumerate(lines) if l.startswith(f"{at}: resuming upload ")), None)
    if first is None or not lines[first].endswith(f" at {offset} bytes, {runs} {unit}"):
        raise Failed(f"the copy run again did not resume at {offset} bytes, {runs} {unit}: {lines}")
    # Every run or part the length of the first, the last whatever is left.
    length = sizes[0]
    total = -(-len(data) // length)
    after = [n for n, _ in committed(lines[first:], at)]
    if after != list(range(runs + 1, total + 1)):
        raise Failed(f"the resumed copy committed {after}, not {runs + 1} to {total}")
    same(f"cp of {at}, resumed, through {door}", run([mantle, "--client", client, "--door", door, "cp", at, "-"]).stdout, data)
    if door == "native":
        etag = hashlib.md5(data).hexdigest()
    else:
        cut = [data[n : n + length] for n in range(0, len(data), length)]
        etag = hashlib.md5(b"".join(hashlib.md5(c).digest() for c in cut)).hexdigest() + f"-{len(cut)}"
    head = asked("head", at).splitlines()
    if f"size {len(data)}" not in head or f'etag "{etag}"' not in head:
        raise Failed(f"head of {at} through {door}: {head}, not etag {etag}")
    print(f"{at}: killed after {runs} {unit}, {offset} bytes; resumed there, {runs + 1} to {total}")


def commands(mantle, client, door, bucket, dir, seed):
    """Every object command of the client through DOOR, into BUCKET: `mb`; `cp` from a file below
    the AWS CLI's 8 MiB multipart threshold and one past it, and back to a file; from standard input
    below it and past it, and back to standard output; `head`; `ls` of a prefix's keys and of the
    bucket's prefixes; a multipart upload's every step, read back, and another aborted; a copy killed
    and resumed. The objects it put, by key."""
    s3 = f"s3://{bucket}"

    def asked(*args, input=None, ok=True):
        return run([mantle, "--client", client, "--door", door, *args], input=input, ok=ok)

    def file(name, data):
        path = dir / f"{door}-{name}"
        path.write_bytes(data)
        return path

    objects = {}
    if f"made {s3}" not in said(asked("mb", s3)):
        raise Failed(f"mantle --door {door} mb said no bucket made")
    for key, size in (("from/file", (3 << 20) + 17), ("from/large", 2 * CLI_PART + 12_345)):
        data = object_bytes(seed, size)
        seed += 1
        asked("cp", file(key.replace("/", "-"), data), f"{s3}/{key}")
        back = dir / f"{door}-back"
        asked("cp", f"{s3}/{key}", back)
        same(f"mantle --door {door} cp of {key} to a file", back.read_bytes(), data)
        objects[key] = data
    for key, size in (("from/stdin", CLI_PART + (1 << 20) + 3), ("from/small", 100_003)):
        data = object_bytes(seed, size)
        seed += 1
        asked("cp", "-", f"{s3}/{key}", input=data)
        same(f"mantle --door {door} cp of {key} to standard output", asked("cp", f"{s3}/{key}", "-").stdout, data)
        objects[key] = data
    head = said(asked("head", f"{s3}/from/stdin")).splitlines()
    if f"size {len(objects['from/stdin'])}" not in head:
        raise Failed(f"mantle --door {door} head: {head}")
    keys = said(asked("ls", f"{s3}/from/"))
    for key, data in objects.items():
        if not any(l.split()[1:] == [str(len(data)), key] for l in keys.splitlines()):
            raise Failed(f"mantle --door {door} ls lists no {len(data)} {key}: {keys}")
    if "PRE from/" not in said(asked("ls", s3)).splitlines():
        raise Failed(f"mantle --door {door} ls lists no prefix from/")

    # A multipart upload's steps: a part from a file, the last from standard input, listed,
    # completed and read back; another aborted.
    upload = said(asked("multipart", "create", f"{s3}/parts")).strip()
    first, last = object_bytes(seed, LEAST_PART), object_bytes(seed + 1, 4096)
    seed += 2
    one = said(asked("multipart", "put", f"{s3}/parts", upload, "1", file("part-1", first)))
    two = said(
        asked(
            "multipart", "put", f"{s3}/parts", upload, "2", "-", "--length", len(last),
            input=last,
        )
    )
    parts = said(asked("multipart", "parts", f"{s3}/parts", upload))
    if [l.split()[:2] for l in parts.splitlines()] != [["1", str(len(first))], ["2", str(len(last))]]:
        raise Failed(f"mantle --door {door} multipart parts: {parts}")
    if upload not in said(asked("multipart", "uploads", s3)):
        raise Failed(f"mantle --door {door} multipart uploads lists no open upload")
    etags = [f"{n}={line.split()[1]}" for n, line in ((1, one), (2, two))]
    completed = said(asked("multipart", "complete", f"{s3}/parts", upload, *etags))
    if '-2"' not in completed:
        raise Failed(f"mantle --door {door} multipart complete: {completed}")
    same(f"mantle --door {door}: a multipart upload", asked("cp", f"{s3}/parts", "-").stdout, first + last)
    objects["parts"] = first + last
    aborted = said(asked("multipart", "create", f"{s3}/aborted")).strip()
    asked("multipart", "abort", f"{s3}/aborted", aborted)
    if aborted in said(asked("multipart", "uploads", s3)):
        raise Failed(f"mantle --door {door} multipart uploads lists an aborted upload")
    resumed = object_bytes(seed, 4 * CLI_PART + 12_345)
    resume(mantle, client, door, f"{s3}/resumed", resumed)
    objects["resumed"] = resumed
    print(
        f"{door} door: mb, cp from and to a file and standard input below and past the multipart "
        "threshold, a copy killed and resumed, head, ls, every multipart step"
    )
    return objects


def doors(mantle, client, profile, dir):
    """The client's object commands through each door, and the S3 door's AWS CLI, each object read
    back through the other door and compared byte for byte."""
    dir.mkdir()
    aws = stock_client(profile)
    print(said(aws("--version")), end="")
    put = {door: commands(mantle, client, door, door, dir, 10 * n) for n, door in enumerate(DOORS)}
    across(mantle, client, put, dir)
    stock(aws, mantle, client, put, dir)
    removed(mantle, client, put)


def through(mantle, client, door, *args, input=None, ok=True):
    return run([mantle, "--client", client, "--door", door, *args], input=input, ok=ok)


def stock_client(profile):
    """The AWS CLI as `aws(*ARGS)`, with the profile init wrote in PROFILE and nothing of the host's:
    an AWS_* variable would outrank it."""
    program = shutil.which("aws")
    if program is None:
        raise Failed("no AWS CLI on the PATH: scripts/stock-clients.sh installs it")
    env = {k: v for k, v in os.environ.items() if not k.upper().startswith("AWS_")}
    env.update(
        AWS_CONFIG_FILE=str(profile / "config"),
        AWS_SHARED_CREDENTIALS_FILE=str(profile / "credentials"),
        AWS_PROFILE="mantle",
        AWS_EC2_METADATA_DISABLED="true",
        AWS_PAGER="",
    )
    return lambda *args: run([program, *args], env=env)


def across(mantle, client, put, dir):
    """One store behind both doors: each door reads, to standard output and to a file, every object
    the other PUT, and lists the other's keys, buckets and uploads, and heads its objects, line for
    line as the other does; the door MANTLE_DOOR names, as MANTLE_CLIENT's client, says the same."""
    for door, other in (("native", "s3"), ("s3", "native")):
        for key, data in put[other].items():
            got = through(mantle, client, door, "cp", f"s3://{other}/{key}", "-").stdout
            same(f"--door {door} cp of {other}/{key}", got, data)
        back = dir / "across.back"
        through(mantle, client, door, "cp", f"s3://{other}/from/large", back)
        same(f"--door {door} cp of {other}/from/large to a file", back.read_bytes(), put[other]["from/large"])
    for listing in (["ls"], ["ls", "s3://native/from/"], ["ls", "s3://s3/"], ["multipart", "uploads", "s3://s3"]):
        native, s3 = (said(through(mantle, client, d, *listing)) for d in DOORS)
        if native != s3:
            raise Failed(f"{' '.join(listing)}: the native door says\n{native}the S3 door says\n{s3}")
    for key in ("from/file", "parts", "resumed"):
        heads = [
            [
                l
                for l in said(through(mantle, client, d, "head", f"s3://s3/{key}")).splitlines()
                if not l.startswith("version")
            ]
            for d in DOORS
        ]
        if heads[0] != heads[1]:
            raise Failed(f"head of s3/{key}: the native door says {heads[0]}, the S3 door {heads[1]}")
    named = said(run([mantle, "ls"], env={**os.environ, "MANTLE_CLIENT": str(client), "MANTLE_DOOR": "s3"}))
    if named != said(through(mantle, client, "s3", "ls")):
        raise Failed(f"mantle ls as MANTLE_CLIENT's client through MANTLE_DOOR's door: {named}")
    print("across the doors: each reads back what the other put, and lists and heads it as the other")


def stock(aws, mantle, client, put, dir):
    """The stock client: a bucket, an object below the CLI's multipart threshold and one past it, up
    and back, listed and headed, then read back through both of the client's doors; what each door
    PUT read back by the AWS CLI; and its bucket removed."""
    aws("s3", "mb", "s3://stock")
    small, large = object_bytes(50, 100_003), object_bytes(51, 2 * CLI_PART + 12_345)
    for key, data in (("small", small), ("large", large)):
        path = dir / key
        path.write_bytes(data)
        aws("s3", "cp", "--no-progress", path, f"s3://stock/{key}")
    listed = said(aws("s3", "ls", "s3://stock/"))
    for size, key in ((len(small), "small"), (len(large), "large")):
        if not any(line.split()[-2:] == [str(size), key] for line in listed.splitlines()):
            raise Failed(f"aws s3 ls lists no {size} {key}: {listed}")
    length = said(
        aws("s3api", "head-object", "--bucket", "stock", "--key", "large",
            "--query", "ContentLength", "--output", "text")
    ).strip()
    if length != str(len(large)):
        raise Failed(f"aws s3api head-object: ContentLength {length}, not {len(large)}")
    for key, data in (("small", small), ("large", large)):
        back = dir / f"{key}.back"
        aws("s3", "cp", "--no-progress", f"s3://stock/{key}", back)
        same(f"aws s3 cp of {key}", back.read_bytes(), data)
        for door in DOORS:
            got = through(mantle, client, door, "cp", f"s3://stock/{key}", "-").stdout
            same(f"--door {door} cp of stock/{key}", got, data)
    for door in DOORS:
        for key in ("from/file", "parts", "resumed"):
            back = dir / "aws.back"
            aws("s3", "cp", "--no-progress", f"s3://{door}/{key}", back)
            same(f"aws s3 cp of {door}/{key}", back.read_bytes(), put[door][key])
    aws("s3", "rm", "s3://stock/small")
    aws("s3", "rm", "s3://stock/large")
    aws("s3", "rb", "s3://stock")
    print("the AWS CLI: s3 mb, cp below and past the multipart threshold, ls, s3api head-object, rm, "
          "rb; each of the client's doors reads what it put, and it reads what each put")


def removed(mantle, client, put):
    """Through each door its own objects and bucket removed, a bucket refused while it holds any. A
    HEAD's answer has no body to name its code (RFC 9110 §9.3.2): the S3 door says its 404."""
    for door, gone_says in (("native", b"NoSuchKey"), ("s3", b"404")):
        keys = list(put[door])
        for key in keys[:2]:
            through(mantle, client, door, "rm", f"s3://{door}/{key}")
        gone = through(mantle, client, door, "head", f"s3://{door}/{keys[0]}", ok=False)
        if gone.returncode == 0 or gone_says not in gone.stderr:
            raise Failed(f"--door {door} head of a removed object did not say {gone_says}: {gone.stderr}")
        held = through(mantle, client, door, "rb", f"s3://{door}", ok=False)
        if held.returncode == 0 or b"BucketNotEmpty" not in held.stderr:
            raise Failed(f"--door {door} rb of a bucket holding an object did not say BucketNotEmpty")
        for key in keys[2:]:
            through(mantle, client, door, "rm", f"s3://{door}/{key}")
        if f"removed s3://{door}" not in said(through(mantle, client, door, "rb", f"s3://{door}")):
            raise Failed(f"--door {door} rb said no bucket removed")
    for door in DOORS:
        buckets = said(through(mantle, client, door, "ls")).split()
        if set(buckets) & {"native", "s3", "stock"}:
            raise Failed(f"--door {door} ls lists a removed bucket: {buckets}")
    print("removed: mantle rm and rb through each door")


def transfer(mantle, root):
    """The binary's transfer harness at the scale CI runs it (crates/mantle/tests/transfer.rs): it
    exits non-zero when any check is not exact."""
    ran = run(
        [mantle, "bench", "transfer", root, "--small", "1000", "--large", "20M", "--count", "4",
         "--seed", "5"]
    )
    print(said(ran), end="")


def check(version, target, archive):
    archive = Path(archive).resolve()
    stem, name = archive_names(version, target)
    if archive.name != name:
        raise Failed(f"{archive.name} is not {name}")
    stated = (archive.parent / f"{name}.sha256").read_bytes()
    digest = sha256(archive)
    if stated != f"{digest}  {name}\n".encode():
        raise Failed(f"{name}.sha256 says {stated!r}; the archive's SHA-256 is {digest}")
    work = Path(tempfile.mkdtemp(prefix="release-check-", dir=os.environ.get("RUNNER_TEMP")))
    try:
        mantle = unpack(archive, stem, target, work / "unpacked")
        print(f"{name}: its SHA-256 and entries as packed")
        version_said = said(run([mantle, "--version"])).strip()
        if version_said != f"mantle {version}":
            raise Failed(f"the binary says {version_said!r}, not 'mantle {version}'")
        print(version_said)
        session(mantle, work / "session")
        transfer(mantle, work / "transfer")
    finally:
        shutil.rmtree(work, ignore_errors=True)
    print(f"{name}: server and client through both doors, every byte as put")


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    steps = parser.add_subparsers(dest="step", required=True)
    made = steps.add_parser("package")
    made.add_argument("version")
    made.add_argument("target")
    made.add_argument("binary")
    made.add_argument("out")
    checked = steps.add_parser("check")
    checked.add_argument("version")
    checked.add_argument("target")
    checked.add_argument("archive")
    args = parser.parse_args()
    # Each line as it is said, interleaved with the commands' own in the job's log.
    sys.stdout.reconfigure(line_buffering=True)
    try:
        if args.step == "package":
            package(args.version, args.target, args.binary, args.out)
        else:
            check(args.version, args.target, args.archive)
    except Failed as e:
        print(f"release: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
