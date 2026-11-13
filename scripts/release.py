#!/usr/bin/env python3
"""The release's archives (docs/design/release.md; .github/workflows/release.yml).

`version` prints the workspace's version (`[workspace.package]` in Cargo.toml), which every archive
names, and refuses a pushed tag that is not `vVERSION`, as GitHub names the ref a run is for
(`GITHUB_REF_TYPE`, `GITHUB_REF_NAME`). Each target's job asks it for itself, so no job waits on
another's to begin (research/85 [17]).

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
import io
import os
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

# The cell's founders: 2f + 1 voters for f = 1, the least cell that keeps a majority with one
# founder lost (docs/design/architecture.md, decision 2), as the check stops one and the transfer
# harness kills each in turn; one failure domain each, so a block's three copies, R3 one copy a
# domain, sit on three nodes (docs/design/durability.md; research/76 [4] f34).
FOUNDERS = 3
COPIES = 3

# The AWS CLI's multipart threshold and part size, 8 MiB each (docs/research/73 [10]), which the S3
# door's copies take as theirs (crates/mantle/src/door.rs, PART): an object of fewer bytes goes in
# one PutObject, one of as many or more as a multipart upload in parts of it.
CLI_PART = 8 << 20
# S3's least part but the last, 5 MiB (docs/research/05 §4.1).
LEAST_PART = 5 << 20
# The piece mantle's client reads standard input in (`mantle_client::PIECE`): hyper-transport's
# receive chunk, the 65,527-byte ceiling of a QUIC datagram's payload (RFC 9000 §18.2).
PIECE = 65_527
# How an object lies on a volume (crates/gateway/src/layout.rs; docs/design/gateway.md §1): sealed
# in 64 KiB segments, each with its 16-byte tag (crates/s3/src/seal.rs, SEGMENT and TAG), and cut
# into blocks of as many whole sealed segments as the 8 MiB chunk holds (layout.rs, CHUNK), each
# block one chunk a copy.
SEAL = 64 << 10
TAG = 16
CHUNK = 8 << 20
BLOCK = CHUNK // (SEAL + TAG) * SEAL
# The native door's run: S3's least part rounded up to whole blocks (crates/meta/src/name.rs,
# run_length; crates/mantle/tests/native.rs, least_run). An object a run holds is put whole; a
# copy from standard input holds a run until it commits (crates/mantle/src/client.rs).
RUN = -(-LEAST_PART // BLOCK) * BLOCK
# The attempts each client makes of a request: the AWS SDKs' and CLI's standard retry mode's three,
# which mantle's own client keeps (`mantle_client::ATTEMPTS`; research/73 [5]). An attempt answered
# SlowDown may leave its block's chunks on the volumes that took them (d0d91066).
ATTEMPTS = 3

# A chunk volume's layout (crates/chunk/src): a record's header and its alignment
# (record.rs, HEADER_LEN, RECORD_ALIGN), a word of CRC-32C a checksum block, at most 16 KiB
# (crates/node/src/derive.rs, CHECKSUM_SHIFT_MOST); the superblocks' second copy's offset and the
# least index log of the standard layout (superblock.rs, OFFSET_B_STANDARD; layout.rs,
# Geometry::plan); the index log's frame and its records (frame.rs, FRAME_HEADER, PUT_LEN,
# DELETE_LEN, SEGMENT_LEN; layout.rs, MAX_FRAME_BYTES); and the free segments the writer keeps
# from new writes for its cleaner (writer.rs, CLEANER_RESERVE).
HEADER_LEN = 96
RECORD_ALIGN = 8
CHECKSUM_SHIFT_MOST = 14
OFFSET_B_STANDARD = 16 << 20
LEAST_LOG = 64 << 20
FRAME_HEADER = 48
PUT_LEN = 1 + 22 + 4 + 8 + 4 + 4 + 8 + 4 + 4 + 8 + 8 + 1
DELETE_LEN = 1 + 22 + 8 + 8
SEGMENT_LEN = 1 + 4 + 8 + 1 + 4
MAX_FRAME_BYTES = 4 << 20
CLEANER_RESERVE = 1
# The bits of a volume's size as `mantle init` takes it, a u64: the most halvings a search of
# sizes makes.
SIZE_BITS = 64


def up(n, to):
    return -(-n // to) * to


def cut(length, unit):
    """LENGTH bytes in pieces of UNIT, the last what is left: an object's runs or parts, a file's
    blocks. None of an empty one."""
    return [min(unit, length - at) for at in range(0, length, unit)]


def records(length, block):
    """The bytes one copy of a file of LENGTH bytes writes on a volume of BLOCK-byte blocks: a record
    a block, the block's sealed segments behind the record's header and checksum table, aligned."""
    shift = min(block.bit_length() - 1, CHECKSUM_SHIFT_MOST)
    out = []
    for plain in cut(length, BLOCK):
        sealed = plain + -(-plain // SEAL) * TAG
        table = -(-sealed // (1 << shift)) * 4
        out.append(up(HEADER_LEN + table + sealed, RECORD_ALIGN))
    return out


def files(door, length):
    """The files an object of LENGTH bytes is put as through DOOR: the native door's runs; one
    PutObject below the S3 door's threshold, its parts past it (the AWS CLI's alike)."""
    if door == "native":
        return cut(length, RUN)
    return [length] if length < CLI_PART else cut(length, CLI_PART)


def segments(puts, block, segment):
    """The segments of each volume the check's puts take, the cleaner's reserve among them.

    PUTS is every file the check puts, by length. Each block's three copies sit on three nodes, a
    volume each, so every volume holds one copy of every block (COPIES over FOUNDERS). Every attempt
    of a put may have stored its chunks (ATTEMPTS), and nothing removed or replaced returns within
    the check: a chunk is freed only a grace after its release, three days (crates/meta/src/
    collector.rs, GRACE_NS). A record longer than half a segment takes a segment of its own; shorter
    ones share segments, each taken up to a block past its end, and a segment sealed when the next
    does not fit holds at least its room less the longest of them (crates/chunk/src/writer.rs,
    Stream::of_write and place). The writer keeps CLEANER_RESERVE segments from new writes."""
    copies = -(-COPIES // FOUNDERS) * ATTEMPTS
    whole, shared, longest = 0, 0, 0
    for length in puts:
        for record in records(length, block):
            if record > segment // 2:
                whole += copies
            else:
                taken = up(record, block)
                shared += copies * taken
                longest = max(longest, taken)
    room = segment - block
    packed = -(-shared // (room - longest)) if shared else 0
    return whole + packed + CLEANER_RESERVE


def data_offset(size, block, segment):
    """Where the segments of a standard volume of SIZE bytes begin: past its two superblocks and
    the index log sized at format (crates/chunk/src/layout.rs, Geometry::plan and log_bytes), its
    fragments at most one a block, the most a node lets its index hold (derive.rs, volume)."""
    fragments, upper = size // block, size // segment
    payload = fragments * PUT_LEN + upper * SEGMENT_LEN
    frame_payload = MAX_FRAME_BYTES - FRAME_HEADER
    frames = -(-payload // (frame_payload - PUT_LEN)) + 2
    checkpoint = payload + frames * (FRAME_HEADER + block)
    placed = max(PUT_LEN, DELETE_LEN) + 2 * SEGMENT_LEN
    batch = up(FRAME_HEADER + placed + SEGMENT_LEN + placed, block)
    largest = max(batch, min(up(FRAME_HEADER + payload, block), MAX_FRAME_BYTES))
    log = up(max(3 * checkpoint + batch + 2 * largest, LEAST_LOG), block)
    return up(2 * OFFSET_B_STANDARD + log, block)


def volume_bytes(need, block, segment):
    """The least volume of BLOCK-byte blocks and SEGMENT-byte segments that lays out NEED segments:
    its size doubled until it holds them, then halved between, a search of SIZE_BITS steps each.
    A volume's segments only grow with its size, since its log grows by a few bytes a block."""

    def held(size):
        offset = data_offset(size, block, segment)
        return (size - offset) // segment if size > offset else 0

    low, high = 0, need * segment
    for _ in range(SIZE_BITS):
        if held(high) >= need:
            break
        low, high = high, 2 * high
    else:
        raise Failed(f"no volume of a u64's bytes holds {need} segments of {segment}")
    for _ in range(SIZE_BITS):
        if high - low <= 1:
            break
        middle = (low + high) // 2
        if held(middle) >= need:
            high = middle
        else:
            low = middle
    return high


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
        for block in iter(lambda: f.read(io.DEFAULT_BUFFER_SIZE), b""):
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
                    shutil.copyfileobj(f, w)
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


def content(name, size):
    """The SIZE bytes of the object NAME: SHAKE-256's output for its name (FIPS 202 §6.2), so every
    object's bytes are its own and made again from its name alone."""
    return hashlib.shake_256(name.encode()).digest(size)


def same(what, got, want):
    if got != want:
        raise Failed(f"{what}: {len(got)} bytes came back where {len(want)} were put, or others")


def free_addresses(count):
    """COUNT founders' addresses, each a QUIC and a plane port (UDP) and an S3 port (TCP) the
    operating system gives free, all held until every one is drawn so none repeats: loopback, but
    the first founder's S3 endpoint, at every address (0.0.0.0), which init serves under TLS
    (node.md §4.1; docs/design/release.md §2)."""
    held = []

    def port(kind, host="127.0.0.1"):
        s = socket.socket(socket.AF_INET, kind)
        held.append(s)
        s.bind((host, 0))
        return f"{host}:{s.getsockname()[1]}"

    try:
        return [
            (
                port(socket.SOCK_DGRAM),
                port(socket.SOCK_DGRAM),
                port(socket.SOCK_STREAM, "0.0.0.0" if n == 0 else "127.0.0.1"),
            )
            for n in range(count)
        ]
    finally:
        for s in held:
            s.close()


class Node:
    """A founder's `mantle serve` process. A thread reads what it says into its log and keeps the
    facts the check waits on: where it bound, that it enlisted, its volume's segment, its chunks'
    room as last asked, its orderly stop, and its end."""

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
                elif words[:3] == ["basis", "volume.1.segment_bytes", "="] and len(words) > 3:
                    fact = ("segment", int(words[3]))
                elif len(words) == 8 and words[0] == "chunks":
                    fact = ("chunks", tuple(int(w) for w in words[1:]))
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

    def free(self):
        """The bytes of the node's volume's free segments, asked now (`chunks`, its last word)."""
        with self.changed:
            self.facts.pop("chunks", None)
        self.process.stdin.write(b"chunks\n")
        self.process.stdin.flush()
        return self.until("chunks")[-1]

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



def probed(mantle, volume):
    """The block and segment a volume in the directory VOLUME is laid out in, as `mantle disk
    probe` says a node derives them from its device."""
    found = {}
    for line in said(run([mantle, "disk", "probe", volume])).splitlines():
        words = line.split()
        if len(words) > 2 and words[0] in ("block", "segment") and words[2] == "bytes:":
            found[words[0]] = int(words[1])
    if set(found) != {"block", "segment"}:
        raise Failed(f"mantle disk probe {volume} said no block and segment: {found}")
    return found["block"], found["segment"]


def session(mantle, root, logs):
    """The cell of three, its doors asked as a user asks them. Each founder's volume holds what the
    check writes (`segments`), as `mantle disk probe` says it is laid out, which each node then
    says of its own volume; its free segments, asked of each node once it enlisted and once the
    doors' writes are done, hold the model to what the volume took."""
    volumes = [root / f"volume-{n}" for n in range(1, FOUNDERS + 1)]
    planned = puts()
    sized = []
    for volume in volumes:
        volume.mkdir(parents=True)
        block, segment = probed(mantle, volume)
        need = segments(planned, block, segment)
        size = volume_bytes(need, block, segment)
        print(
            f"{volume.name}: {need} segments of {segment} bytes, {CLEANER_RESERVE} the cleaner's, "
            f"for {len(planned)} files put up to {ATTEMPTS} times each: {size} bytes"
        )
        sized.append((size, segment, need))
    addresses = free_addresses(len(volumes))
    out = root / "cell"
    args = ["init", out, "--max-nodes", len(volumes), "--max-volumes", len(volumes)]
    for n, ((quic, plane, s3), volume, (size, _, _)) in enumerate(zip(addresses, volumes, sized), 1):
        # The first founder's endpoint is named `localhost`, which init certifies and its
        # profiles name, `https://localhost:PORT`: every S3 client of the check dials it under TLS,
        # by a name that resolves to ::1 and 127.0.0.1 alike.
        name = ",name=localhost" if n == 1 else ""
        args += [
            "--node",
            f"quic={quic},plane={plane},s3={s3}{name},domain=rack-{n},volume={volume}:{size}",
        ]
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

    logs.mkdir(parents=True)
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
        free = []
        for node, (_, segment, need) in zip(nodes, sized):
            if node.until("segment") != segment:
                raise Failed(f"{node.name} lays its volume out in segments of {node.facts['segment']}, not {segment}")
            free.append(node.free())
            if free[-1] // segment < need:
                raise Failed(f"{node.name}'s volume has {free[-1] // segment} free segments, not the {need} the check needs")
        client = root / "client"
        print(said(run([mantle, "init", "client", client, "--cell", out])), end="")
        endpoint = endpoint_url(client / "s3" / "config")
        port = addresses[0][2].rsplit(":", 1)[1]
        if endpoint != f"https://localhost:{port}":
            raise Failed(f"the client's S3 profile names {endpoint}, not https://localhost:{port}")
        print(f"the S3 door's endpoint: {endpoint}, TLS under the endpoint's authority")
        doors(mantle, client, out / "s3", root / "objects")
        plain(mantle, client, addresses[1][2], root / "plain")
        # Nothing the check removed is freed within it, so what each volume took is what its free
        # segments fell by, which the model bounds.
        for node, (_, segment, need), before in zip(nodes, sized, free):
            took = (before - node.free()) // segment
            if took > need - CLEANER_RESERVE:
                raise Failed(f"{node.name}'s volume took {took} segments, past the {need - CLEANER_RESERVE} the model bounds")
            print(f"{node.name}'s volume took {took} segments of the {need - CLEANER_RESERVE} the model bounds")
        # Its peers serving on, a founder stops in order: its ranges handed over, exiting 0.
        status = nodes[0].stop()
        if status != 0:
            raise Failed(f"{nodes[0].name} stopped in order and exited {status}")
        print(f"{nodes[0].name} stopped in order, exit 0")
    except BaseException:
        print(f"each node's whole output is under {logs}", file=sys.stderr)
        raise
    finally:
        for node in nodes:
            node.end()


# The doors a client's object commands ask the cell through (`mantle --door`).
DOORS = ("native", "s3")


def one(door):
    """The largest object one request through DOOR carries: a native run, which the node takes
    whole, or a byte short of the S3 door's threshold, one PutObject."""
    return RUN if door == "native" else CLI_PART - 1


def unit(door):
    """What a copy through DOOR commits at a time: a run, or a part."""
    return RUN if door == "native" else CLI_PART


def resumed(door):
    """The length of the copy through DOOR the check kills and runs again: its first unit, which
    commits before the kill; its second, in flight then and never whole, since the feed stops a
    piece and a pipe's buffer past the first (pipe(7): 65,536 bytes on Linux), far short of a unit;
    and a byte, a third unit the killed copy never began."""
    return 2 * unit(door) + 1


def puts():
    """Every file the check puts into the founders' volumes, by length, for `segments`: through each
    door an object of `one` and one a byte longer, from a file and from standard input; a multipart
    upload of S3's least part and a last part of a byte; the killed copy's committed unit and the
    one in flight, then the copy run again; and the AWS CLI's and the plain-HTTP leg's objects of
    `one` and a byte longer through the S3 door."""
    out = []
    for door in DOORS:
        for length in (one(door), one(door) + 1):
            out += files(door, length) * 2
        out += [LEAST_PART, 1]
        out += [unit(door)] + cut(resumed(door), unit(door))
    for length in (one("s3"), one("s3") + 1):
        out += files("s3", length) * 2
    return out


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
    of the open upload and where the copy run again goes on; its runs or parts are each `unit`.

    Standard input is fed in the client's own pieces (PIECE), and no piece follows once a run or a
    part is said committed, so the copy holds at most its unit and a piece past it when it is
    killed (`resumed`)."""
    units = "runs" if door == "native" else "parts"
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
            for start in range(0, len(data), PIECE):
                if fed.is_set():
                    return
                copying.stdin.write(data[start : start + PIECE])
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

    # The cell holds the open upload the killed copy left, its parts what it said committed, each
    # the door's unit.
    bucket, key = at[len("s3://"):].split("/", 1)
    uploads = asked("multipart", "uploads", f"s3://{bucket}")
    open_ids = [w[1] for w in (l.split() for l in uploads.splitlines()) if len(w) == 3 and w[2] == key]
    if len(open_ids) != 1:
        raise Failed(f"multipart uploads lists {len(open_ids)} open uploads of {key}: {uploads}")
    parts = asked("multipart", "parts", at, open_ids[0])
    sizes = [int(l.split()[1]) for l in parts.splitlines()]
    if len(sizes) != runs or sum(sizes) != offset:
        raise Failed(f"the killed copy said {runs} {units}, {offset} bytes durable; the cell lists {parts}")
    if set(sizes) != {unit(door)}:
        raise Failed(f"the killed copy's {units} are {sizes} bytes, not the door's {unit(door)}")

    again = run(command, input=data)
    lines = again.stderr.decode(errors="replace").splitlines()
    first = next((n for n, l in enumerate(lines) if l.startswith(f"{at}: resuming upload ")), None)
    if first is None or not lines[first].endswith(f" at {offset} bytes, {runs} {units}"):
        raise Failed(f"the copy run again did not resume at {offset} bytes, {runs} {units}: {lines}")
    total = len(cut(len(data), unit(door)))
    after = [n for n, _ in committed(lines[first:], at)]
    if after != list(range(runs + 1, total + 1)):
        raise Failed(f"the resumed copy committed {after}, not {runs + 1} to {total}")
    same(f"cp of {at}, resumed, through {door}", run([mantle, "--client", client, "--door", door, "cp", at, "-"]).stdout, data)
    if door == "native":
        etag = hashlib.md5(data).hexdigest()
    else:
        pieces = cut(len(data), unit(door))
        starts = [sum(pieces[:n]) for n in range(len(pieces))]
        digests = b"".join(hashlib.md5(data[a : a + n]).digest() for a, n in zip(starts, pieces))
        etag = hashlib.md5(digests).hexdigest() + f"-{len(pieces)}"
    head = asked("head", at).splitlines()
    if f"size {len(data)}" not in head or f'etag "{etag}"' not in head:
        raise Failed(f"head of {at} through {door}: {head}, not etag {etag}")
    print(f"{at}: killed after {runs} {units}, {offset} bytes; resumed there, {runs + 1} to {total}")


def commands(mantle, client, door, dir):
    """Every object command of the client through DOOR, into a bucket of its name: `mb`; `cp` from a
    file of `one` bytes, the most one request carries, and of a byte more, the least past it, and
    back to a file; from standard input of the same two, and back to standard output; `head`; `ls`
    of a prefix's keys and of the bucket's prefixes; a multipart upload's every step, its first part
    S3's least and its last a byte, read back, and another aborted; a copy killed and resumed. The
    objects it put, by key."""
    s3 = f"s3://{door}"

    def asked(*args, input=None, ok=True):
        return run([mantle, "--client", client, "--door", door, *args], input=input, ok=ok)

    def file(name, data):
        path = dir / f"{door}-{name}"
        path.write_bytes(data)
        return path

    objects = {}
    if f"made {s3}" not in said(asked("mb", s3)):
        raise Failed(f"mantle --door {door} mb said no bucket made")
    for key, size in (("from/file-one", one(door)), ("from/file-past", one(door) + 1)):
        data = content(f"{s3}/{key}", size)
        asked("cp", file(key.replace("/", "-"), data), f"{s3}/{key}")
        back = dir / f"{door}-back"
        asked("cp", f"{s3}/{key}", back)
        same(f"mantle --door {door} cp of {key} to a file", back.read_bytes(), data)
        objects[key] = data
    for key, size in (("from/stdin-one", one(door)), ("from/stdin-past", one(door) + 1)):
        data = content(f"{s3}/{key}", size)
        asked("cp", "-", f"{s3}/{key}", input=data)
        same(f"mantle --door {door} cp of {key} to standard output", asked("cp", f"{s3}/{key}", "-").stdout, data)
        objects[key] = data
    head = said(asked("head", f"{s3}/from/stdin-past")).splitlines()
    if f"size {len(objects['from/stdin-past'])}" not in head:
        raise Failed(f"mantle --door {door} head: {head}")
    keys = said(asked("ls", f"{s3}/from/"))
    for key, data in objects.items():
        if not any(l.split()[1:] == [str(len(data)), key] for l in keys.splitlines()):
            raise Failed(f"mantle --door {door} ls lists no {len(data)} {key}: {keys}")
    if "PRE from/" not in said(asked("ls", s3)).splitlines():
        raise Failed(f"mantle --door {door} ls lists no prefix from/")

    # A multipart upload's steps: S3's least part from a file, the last a byte from standard input,
    # listed, completed and read back; another aborted.
    upload = said(asked("multipart", "create", f"{s3}/parts")).strip()
    first, last = content(f"{s3}/parts#1", LEAST_PART), content(f"{s3}/parts#2", 1)
    one_put = said(asked("multipart", "put", f"{s3}/parts", upload, "1", file("part-1", first)))
    two_put = said(
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
    etags = [f"{n}={line.split()[1]}" for n, line in ((1, one_put), (2, two_put))]
    completed = said(asked("multipart", "complete", f"{s3}/parts", upload, *etags))
    if '-2"' not in completed:
        raise Failed(f"mantle --door {door} multipart complete: {completed}")
    same(f"mantle --door {door}: a multipart upload", asked("cp", f"{s3}/parts", "-").stdout, first + last)
    objects["parts"] = first + last
    aborted = said(asked("multipart", "create", f"{s3}/aborted")).strip()
    asked("multipart", "abort", f"{s3}/aborted", aborted)
    if aborted in said(asked("multipart", "uploads", s3)):
        raise Failed(f"mantle --door {door} multipart uploads lists an aborted upload")
    data = content(f"{s3}/resumed", resumed(door))
    resume(mantle, client, door, f"{s3}/resumed", data)
    objects["resumed"] = data
    print(
        f"{door} door: mb, cp from and to a file and standard input of the most one request carries "
        "and a byte more, a copy killed and resumed, head, ls, every multipart step"
    )
    return objects


def doors(mantle, client, profile, dir):
    """The client's object commands through each door, and the S3 door's AWS CLI, each object read
    back through the other door and compared byte for byte."""
    dir.mkdir()
    aws = stock_client(profile)
    print(said(aws("--version")), end="")
    put = {door: commands(mantle, client, door, dir) for door in DOORS}
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
        through(mantle, client, door, "cp", f"s3://{other}/from/file-past", back)
        same(f"--door {door} cp of {other}/from/file-past to a file", back.read_bytes(), put[other]["from/file-past"])
    for listing in (["ls"], ["ls", "s3://native/from/"], ["ls", "s3://s3/"], ["multipart", "uploads", "s3://s3"]):
        native, s3 = (said(through(mantle, client, d, *listing)) for d in DOORS)
        if native != s3:
            raise Failed(f"{' '.join(listing)}: the native door says\n{native}the S3 door says\n{s3}")
    for key in ("from/file-one", "parts", "resumed"):
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
    """The stock client: a bucket, an object of the most one PutObject carries below the CLI's
    multipart threshold and one a byte longer, at it, up and back, listed and headed, then read back
    through both of the client's doors; what each door PUT read back by the AWS CLI; and its bucket
    removed."""
    aws("s3", "mb", "s3://stock")
    objects = {key: content(f"s3://stock/{key}", size) for key, size in (("one", one("s3")), ("past", one("s3") + 1))}
    for key, data in objects.items():
        path = dir / key
        path.write_bytes(data)
        aws("s3", "cp", "--no-progress", path, f"s3://stock/{key}")
    listed = said(aws("s3", "ls", "s3://stock/"))
    for key, data in objects.items():
        if not any(line.split()[-2:] == [str(len(data)), key] for line in listed.splitlines()):
            raise Failed(f"aws s3 ls lists no {len(data)} {key}: {listed}")
    length = said(
        aws("s3api", "head-object", "--bucket", "stock", "--key", "past",
            "--query", "ContentLength", "--output", "text")
    ).strip()
    if length != str(len(objects["past"])):
        raise Failed(f"aws s3api head-object: ContentLength {length}, not {len(objects['past'])}")
    for key, data in objects.items():
        back = dir / f"{key}.back"
        aws("s3", "cp", "--no-progress", f"s3://stock/{key}", back)
        same(f"aws s3 cp of {key}", back.read_bytes(), data)
        for door in DOORS:
            got = through(mantle, client, door, "cp", f"s3://stock/{key}", "-").stdout
            same(f"--door {door} cp of stock/{key}", got, data)
    for door in DOORS:
        for key in ("from/file-one", "parts", "resumed"):
            back = dir / "aws.back"
            aws("s3", "cp", "--no-progress", f"s3://{door}/{key}", back)
            same(f"aws s3 cp of {door}/{key}", back.read_bytes(), put[door][key])
    for key in objects:
        aws("s3", "rm", f"s3://stock/{key}")
    aws("s3", "rb", "s3://stock")
    print("the AWS CLI: s3 mb, cp below and at the multipart threshold, ls, s3api head-object, rm, "
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


def endpoint_url(config):
    """The `endpoint_url` of the `[profile mantle]` a config file states."""
    inside = False
    for line in config.read_text().splitlines():
        if line.startswith("["):
            inside = line.strip() == "[profile mantle]"
        elif inside and not line[:1].isspace() and line.split("=", 1)[0].strip() == "endpoint_url":
            return line.split("=", 1)[1].strip()
    raise Failed(f"{config} names no endpoint_url")


def plain(mantle, client, s3, dir):
    """The S3 door over plain HTTP, as a loopback endpoint serves it: a copy of the client whose S3
    profile names the founder listening at S3, `http://S3`, asks mb, cp from a file of the most one
    PutObject carries and from standard input of a byte more, a multipart upload, up and back, ls,
    head, rm and rb, each object read back through the native door."""
    copy = dir / "client"
    shutil.copytree(client, copy, ignore=shutil.ignore_patterns("journal"))
    config = copy / "s3" / "config"
    lines = [
        f"endpoint_url = http://{s3}" if l.split("=", 1)[0].strip() == "endpoint_url" else l
        for l in config.read_text().splitlines()
        if l.split("=", 1)[0].strip() != "ca_bundle"
    ]
    config.write_text("\n".join(lines) + "\n")
    if endpoint_url(config) != f"http://{s3}":
        raise Failed(f"{config} names {endpoint_url(config)}, not http://{s3}")

    def asked(*args, input=None, ok=True):
        return run([mantle, "--client", copy, "--door", "s3", *args], input=input, ok=ok)

    if "made s3://plain" not in said(asked("mb", "s3://plain")):
        raise Failed("mantle --door s3 mb over plain HTTP said no bucket made")
    objects = {key: content(f"s3://plain/{key}", size) for key, size in (("file", one("s3")), ("stdin", one("s3") + 1))}
    source = dir / "from-file"
    source.write_bytes(objects["file"])
    asked("cp", source, "s3://plain/file")
    back = dir / "file.back"
    asked("cp", "s3://plain/file", back)
    same("cp over plain HTTP to a file", back.read_bytes(), objects["file"])
    asked("cp", "-", "s3://plain/stdin", input=objects["stdin"])
    same("cp over plain HTTP to standard output", asked("cp", "s3://plain/stdin", "-").stdout, objects["stdin"])
    for key, data in objects.items():
        same(f"--door native cp of plain/{key}", through(mantle, client, "native", "cp", f"s3://plain/{key}", "-").stdout, data)
        if f"size {len(data)}" not in said(asked("head", f"s3://plain/{key}")).splitlines():
            raise Failed(f"head of plain/{key} over plain HTTP")
    keys = said(asked("ls", "s3://plain/"))
    for key, data in objects.items():
        if not any(l.split()[1:] == [str(len(data)), key] for l in keys.splitlines()):
            raise Failed(f"ls over plain HTTP lists no {len(data)} {key}: {keys}")
    for key in objects:
        asked("rm", f"s3://plain/{key}")
    if "removed s3://plain" not in said(asked("rb", "s3://plain")):
        raise Failed("rb over plain HTTP said no bucket removed")
    print(f"the S3 door over plain HTTP at http://{s3}: mb, cp from a file and standard input, up and "
          "back, ls, head, rm, rb; each object read back through the native door")


# The ways the transfer harness puts a large object (`mantle bench transfer --ways`): natively in
# runs, by the AWS CLI from its standard input, natively in parts and by boto3 in parts.
WAYS = ("native", "cli", "native-parts", "boto-parts")


def transfer(mantle, root, logs):
    """The binary's transfer harness at the scale of the plan's smoke, as CI's suite runs it
    (crates/mantle/tests/transfer.rs; research/76): the least of the small sets the owner agreed,
    1,000 objects (research/76 [4] f2); a large case of 20 MiB objects, three blocks and three of
    the AWS CLI's parts, the last short of S3's least part (docs/design/node.md), one object for
    each of the harness's four ways; and the harness's own seed. It exits non-zero when any check is
    not exact; what it and its nodes said is kept in LOGS."""
    out, err = logs / "transfer.out", logs / "transfer.err"
    with open(out, "wb") as o, open(err, "wb") as e:
        ran = subprocess.run(
            [str(mantle), "bench", "transfer", str(root), "--small", "1000", "--large", "20M",
             "--count", str(len(WAYS))],
            stdin=subprocess.DEVNULL,
            stdout=o,
            stderr=e,
        )
    print(out.read_text(errors="replace"), end="")
    if ran.returncode != 0:
        raise Failed(f"mantle bench transfer exited {ran.returncode}; its output and its nodes' are {out} and {err}")


def check(version, target, archive, logs):
    archive = Path(archive).resolve()
    logs = Path(logs).resolve()
    logs.mkdir(parents=True, exist_ok=True)
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
        session(mantle, work / "session", logs / "session")
        transfer(mantle, work / "transfer", logs)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    print(f"{name}: server and client through both doors, every byte as put")


def version():
    """The workspace's version, read from Cargo.toml's `[workspace.package]` without `tomllib`,
    which Python 3.10, Ubuntu 22.04's, lacks: the section's `version = "..."` line. Refused where
    the run is for a tag that does not name it."""
    inside, found = False, None
    for line in (ROOT / "Cargo.toml").read_text().splitlines():
        stripped = line.strip()
        if stripped.startswith("["):
            inside = stripped == "[workspace.package]"
        elif inside and stripped.split("=", 1)[0].strip() == "version":
            value = stripped.split("=", 1)[1].strip()
            if len(value) < 2 or value[0] != '"' or value[-1] != '"':
                raise Failed(f"Cargo.toml's [workspace.package] version is not a string: {value}")
            found = value[1:-1]
            break
    if not found:
        raise Failed("Cargo.toml states no [workspace.package] version")
    if os.environ.get("GITHUB_REF_TYPE") == "tag":
        tag = os.environ.get("GITHUB_REF_NAME", "")
        if tag != f"v{found}":
            raise Failed(f"the tag {tag} is not v{found}, the workspace's version")
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    steps = parser.add_subparsers(dest="step", required=True)
    steps.add_parser("version")
    made = steps.add_parser("package")
    made.add_argument("version")
    made.add_argument("target")
    made.add_argument("binary")
    made.add_argument("out")
    checked = steps.add_parser("check")
    checked.add_argument("version")
    checked.add_argument("target")
    checked.add_argument("archive")
    checked.add_argument(
        "--logs", required=True, help="where each process's whole output is kept, for a failure's artifact"
    )
    args = parser.parse_args()
    # Each line as it is said, interleaved with the commands' own in the job's log.
    sys.stdout.reconfigure(line_buffering=True)
    try:
        if args.step == "version":
            print(version())
        elif args.step == "package":
            package(args.version, args.target, args.binary, args.out)
        else:
            check(args.version, args.target, args.archive, args.logs)
    except Failed as e:
        print(f"release: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
