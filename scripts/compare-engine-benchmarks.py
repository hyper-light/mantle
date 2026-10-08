#!/usr/bin/env python3
"""Run alternating, reproducible Mantle/RocksDB comparisons; keep every raw round."""
import argparse
import csv
import hashlib
import json
import os
import pathlib
import platform
import re
import shlex
import shutil
import subprocess
import tempfile
import time


ROOT = pathlib.Path(__file__).resolve().parent.parent


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def command_output(command, cwd=None):
    try:
        result = subprocess.run(command, cwd=cwd, capture_output=True, text=True)
    except OSError as error:
        return {"command": command, "exit_code": None, "stdout": "", "stderr": str(error)}
    return {"command": command, "exit_code": result.returncode,
            "stdout": result.stdout, "stderr": result.stderr}


def source_record(path):
    path = path.resolve()
    revision = command_output(["git", "rev-parse", "HEAD"], path)
    status = command_output(["git", "status", "--porcelain=v1"], path)
    diff = subprocess.run(["git", "diff", "--binary", "HEAD"], cwd=path,
                          capture_output=True, check=True).stdout
    untracked = subprocess.check_output(
        ["git", "ls-files", "--others", "--exclude-standard", "-z"], cwd=path)
    files = {}
    for name in untracked.decode().split("\0"):
        if name and (path / name).is_file():
            files[name] = sha256(path / name)
    dirty = hashlib.sha256(diff + json.dumps(files, sort_keys=True).encode()).hexdigest()
    benchmark_sources = {}
    for name in ["crates/engine/benches/shard_db.rs",
                 "crates/engine/benches/support/rocks_workload.rs"]:
        if (path / name).is_file():
            benchmark_sources[name] = sha256(path / name)
    return {"path": str(path), "revision": revision, "status": status,
            "tracked_diff_sha256": hashlib.sha256(diff).hexdigest(),
            "untracked_sha256": files, "dirty_content_sha256": dirty,
            "benchmark_sources_sha256": benchmark_sources}


def host_record():
    commands = [["uptime"]]
    if platform.system() == "Darwin":
        commands += [["vm_stat"], ["sysctl", "hw.memsize", "hw.logicalcpu"]]
    elif platform.system() == "Linux":
        commands += [["cat", "/proc/meminfo"], ["cat", "/proc/vmstat"]]
    return {"utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "platform": platform.platform(), "logical_cpus": os.cpu_count(),
            "snapshots": [command_output(command) for command in commands]}


def process_tree(pid):
    linux = platform.system() == "Linux"
    fields = "pid=,ppid=,rss=,nlwp=" if linux else "pid=,ppid=,rss="
    result = command_output(["ps", "-axo", fields])
    if result["exit_code"] != 0:
        return None, None
    processes = {}
    for line in result["stdout"].splitlines():
        try:
            values = [int(value) for value in line.split()]
            processes[values[0]] = values[1:]
        except (ValueError, IndexError):
            continue
    selected = {pid}
    while True:
        children = {child for child, values in processes.items() if values[0] in selected}
        more = children - selected
        if not more:
            break
        selected.update(more)
    rss = sum(processes[child][1] for child in selected if child in processes)
    if linux:
        threads = sum(processes[child][2] for child in selected if child in processes)
    else:
        threads = 0
        for child in selected:
            rows = command_output(["ps", "-M", "-p", str(child), "-o", "pid="])
            if rows["exit_code"] != 0:
                return rss * 1024, None
            threads += sum(bool(line.strip()) for line in rows["stdout"].splitlines())
    return rss * 1024, threads


def parse_metrics(raw, engine):
    metrics = {}
    current = None
    for line in raw.splitlines():
        if engine == "mantle":
            match = re.match(r"(fillrandom|readrandom|seekrandom) \d+ (\d+) ([\d.]+)", line)
            if match:
                metrics.setdefault(match[1], {}).update(ops_per_sec=int(match[2]),
                                                       micros_per_op=float(match[3]))
            match = re.match(r"(fillrandom|readrandom|seekrandom) latency_us (.*)", line)
            if match:
                metrics.setdefault(match[1], {})["latency_us"] = {
                    name: float(value) for name, value in re.findall(
                        r"(p50|p99\.99|p99\.9|p99|max) ([\d.]+)", match[2])}
        else:
            match = re.match(r"(fillrandom|readrandom|seekrandom)\s+:\s+([\d.]+) micros/op (\d+) ops/sec", line)
            if match:
                current = match[1]
                metrics[current] = {"ops_per_sec": int(match[3]),
                                    "micros_per_op": float(match[2]), "latency_us": {}}
            if current and line.startswith("Percentiles:"):
                metrics[current]["latency_us"].update({
                    name.lower(): float(value) for name, value in re.findall(
                        r"(P50|P99\.99|P99\.9|P99): ([\d.]+)", line)})
            if current and line.startswith("Min:"):
                match = re.search(r"Max: ([\d.]+)", line)
                if match:
                    metrics[current]["latency_us"]["max"] = float(match[1])
    return metrics


def parse_phase_accounts(raw):
    accounts = {}
    fields = {"process_cpu_ns/op": "cpu_ns_per_op", "user_ns/op": "user_ns_per_op",
              "system_ns/op": "system_ns_per_op", "instructions/op": "instructions_per_op",
              "cycles/op": "cycles_per_op"}
    for line in raw.splitlines():
        if " process_cpu_ns/op " not in line:
            continue
        phase = line.split(" process_cpu_ns/op ", 1)[0]
        account = {fields[name]: None if value == "unavailable" else float(value)
                   for name, value in re.findall(
                       r"(process_cpu_ns/op|user_ns/op|system_ns/op|instructions/op|cycles/op) (\S+)", line)}
        account["scope"] = "whole-process-including-issuer-and-device-workers"
        accounts[phase] = account
    return accounts


def run_one(command, directory, engine):
    # wait4 returns the kernel's whole-process resource account, including all its
    # threads. On macOS ru_maxrss is bytes; Linux reports KiB (getrusage(2)).
    before = host_record()
    start = time.monotonic()
    with (directory / "raw.txt").open("w") as output, (directory / "resources.csv").open("w") as samples:
        writer = csv.writer(samples)
        writer.writerow(["elapsed_seconds", "tree_rss_bytes", "tree_threads"])
        process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT)
        peak_rss = peak_threads = None
        try:
            while True:
                done, status, resources = os.wait4(process.pid, os.WNOHANG)
                if done:
                    code = os.waitstatus_to_exitcode(status)
                    process.returncode = code
                    break
                rss, threads = process_tree(process.pid)
                if rss is not None:
                    peak_rss = max(peak_rss or 0, rss)
                if threads is not None:
                    peak_threads = max(peak_threads or 0, threads)
                writer.writerow([time.monotonic() - start, rss, threads])
                time.sleep(0.25)
        except BaseException:
            process.terminate()
            process.wait()
            raise
    raw = (directory / "raw.txt").read_text()
    record = {"command": command, "shell_command": shlex.join(command),
              "resource_method": "wait4/getrusage, all process threads",
              "process_user_seconds": resources.ru_utime,
              "process_system_seconds": resources.ru_stime,
              "process_peak_rss_bytes": resources.ru_maxrss * (1024 if platform.system() == "Linux" else 1),
              "process_minor_faults": resources.ru_minflt,
              "process_major_faults": resources.ru_majflt,
              "process_voluntary_context_switches": resources.ru_nvcsw,
              "process_involuntary_context_switches": resources.ru_nivcsw,
              "exit_code": code,
              "wall_seconds": time.monotonic() - start,
              "sampled_peak_tree_rss_bytes": peak_rss,
              "sampled_peak_tree_threads": peak_threads,
              "sampling_limitation": "Missing samples mean the OS denied ps or the process exited before a sample.",
              "host_before": before, "host_after": host_record(),
              "metrics": parse_metrics(raw, engine)}
    if engine == "mantle":
        record["phase_process_accounting"] = parse_phase_accounts(raw)
        record["phase_process_accounting_lines"] = [line for line in raw.splitlines()
                                                      if " process_cpu_ns/op " in line]
        record["maintenance_seconds"] = [float(value) for value in re.findall(
            r"^maintain (?:after-read )?([\d.]+)s$", raw, re.M)]
        record["memory_and_filters"] = [line for line in raw.splitlines()
                                         if line.startswith(("workload ", "memory ", "filter budget ", "filters:", "workers "))]
    else:
        record["maintenance_seconds"] = None
        record["maintenance_limitation"] = (
            "db_bench does not report drain elapsed time. waitforcompaction also sleeps "
            "5 seconds unconditionally before waiting; do not infer drain throughput from wall time.")
    (directory / "run.json").write_text(json.dumps(record, indent=2) + "\n")
    if code:
        raise RuntimeError(f"{engine} exited {code}; preserved {directory / 'raw.txt'}")
    if set(record["metrics"]) != {"fillrandom", "readrandom", "seekrandom"}:
        raise RuntimeError(f"{engine} did not report every workload; preserved {directory / 'raw.txt'}")
    return record


def labelled(value):
    label, separator, name = value.partition("=")
    if not separator or not re.fullmatch(r"[A-Za-z0-9_-]+", label):
        raise argparse.ArgumentTypeError("expected LABEL=PATH, with a simple unique label")
    return label, pathlib.Path(name).resolve()


def positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mantle-binary", type=labelled, action="append", required=True)
    parser.add_argument("--mantle-source", type=labelled, action="append", default=[])
    parser.add_argument("--rocksdb-binary", type=pathlib.Path)
    parser.add_argument("--rocksdb-source", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--single-configuration", action="store_true",
                        help="run only the supplied Mantle configurations, with identical flags")
    parser.add_argument("--rounds", type=positive, default=3)
    parser.add_argument("--num", type=positive, default=10000000)
    parser.add_argument("--reads", type=positive, default=1000000)
    parser.add_argument("--seek-nexts", type=positive, default=10)
    parser.add_argument("--seed", type=positive, default=301)
    parser.add_argument("--cache-mib", type=positive, default=256)
    parser.add_argument("--write-budget-mib", type=positive, default=64,
                        help="Mantle's issued-write budget; both engines' memtables remain 64 MiB")
    parser.add_argument("--issuer-depth", type=positive, default=16)
    parser.add_argument("--runs-in-flight", type=positive, default=16)
    parser.add_argument("--bloom-bits", type=positive, default=24,
                        help="24 rounds today's observed Mantle 23.96–25.88 bits/input-key; actual budgets differ")
    parser.add_argument("--background-jobs", type=positive, default=2)
    parser.add_argument("--rocksdb-write-buffers", type=positive, default=2)
    parser.add_argument("--maintenance", choices=["drain", "deferred"], default="drain")
    parser.add_argument("--attribute", action="store_true",
                        help="separate attribution experiment; instrumentation changes ordinary timing")
    args = parser.parse_args()
    if not args.single_configuration and (not args.rocksdb_binary or not args.rocksdb_source):
        parser.error("RocksDB comparison requires --rocksdb-binary and --rocksdb-source")
    if platform.system() not in {"Darwin", "Linux"}:
        parser.error("resource collection currently supports macOS and Linux")
    if args.seed + args.rounds + 3 >= 2**63:
        parser.error("seed and round offsets must fit RocksDB's signed 64-bit seed")
    labels = [label for label, _ in args.mantle_binary]
    if len(labels) != len(set(labels)) or "rocksdb" in labels:
        parser.error("Mantle labels must be unique and cannot be rocksdb")
    # Capture dirty state before creating output, so an output inside the checkout
    # does not recursively become part of its own source fingerprint.
    sources = dict(args.mantle_source)
    metadata = {"arguments": vars(args), "source_relationship":
                "Source paths are declared inputs; binary SHA-256 pins the executed artifact.",
                "sources": {label: source_record(sources.get(label, ROOT)) for label in labels},
                "limitations": [
                    "Configured caches are not equal total memory or CPU budgets.",
                    "Mantle uses its owner, issuer and device workers; RocksDB uses a driver and background jobs. CPU/peak RSS from wait4 includes all process threads; sampled process-tree RSS/thread counts are approximate.",
                    "Keys and operation streams match one-thread RocksDB 11.8.1 with one database and batch size 1. Mantle values are repeated v; RocksDB values are generated; compression is disabled.",
                    "Mantle tails time API calls only; RocksDB histograms include harness work between completions and use microsecond buckets. Throughput includes each driver's loop.",
                    "Mantle phase CPU/instruction/cycle accounts are whole-process deltas, including issuer/device workers and overlapping maintenance. Boundary syscalls are outside API timers. Maintenance is normalized per input put; unsupported counters are null. RocksDB has no equivalent phase accounts in this workflow.",
                    "Bloom 24 is a measured nominal comparison, not equal actual filter memory. Mantle reports bytes and bits per input key; RocksDB sizes filters per SST entry.",
                    "Drain mode: Mantle maintains and lands issued writes; RocksDB waitforcompaction sleeps 5 seconds and waits for background work without flushing the active memtable. Its drain duration is unavailable. Deferred mode moves drain after reads/seeks; phases can still overlap background work.",
                    "The OS page cache is shared and is not cleared between runs. Every valid slow round is retained."]}
    if not args.single_configuration:
        metadata["sources"]["rocksdb"] = source_record(args.rocksdb_source)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    binaries = output / "binaries"
    binaries.mkdir()
    configurations = []
    for label, original in args.mantle_binary:
        destination = binaries / label
        shutil.copy2(original, destination)
        configurations.append((label, "mantle", destination))
    if not args.single_configuration:
        destination = binaries / "rocksdb"
        shutil.copy2(args.rocksdb_binary.resolve(), destination)
        configurations.append(("rocksdb", "rocksdb", destination))
    metadata["binaries"] = {label: {"path": str(binary), "sha256": sha256(binary)}
                            for label, _, binary in configurations}
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2, default=str) + "\n")
    results = []
    for round_number in range(1, args.rounds + 1):
        order = configurations if round_number % 2 else list(reversed(configurations))
        seed = args.seed + round_number - 1
        for label, engine, binary in order:
            directory = output / f"round-{round_number}-{label}"
            directory.mkdir()
            with tempfile.TemporaryDirectory(prefix="db-", dir=directory) as database:
                if engine == "mantle":
                    command = [str(binary), database, str(args.num), "8", "buffered",
                               str(args.reads), str(args.cache_mib),
                               "attribute" if args.attribute else "none",
                               str(args.issuer_depth), str(args.runs_in_flight),
                               str(args.reads), str(args.seek_nexts), "0", "0", "50", "50",
                               str(args.write_budget_mib), "0", "0", "0",
                               f"--rocks-seed={seed}", f"--maintenance={args.maintenance}"]
                else:
                    drain = ",waitforcompaction"
                    phases = ("fillrandom" + drain + ",readrandom,seekrandom" if args.maintenance == "drain"
                              else "fillrandom,readrandom,seekrandom" + drain)
                    command = [str(binary), f"--benchmarks={phases}", f"--db={database}",
                               f"--num={args.num}", f"--reads={args.reads}",
                               f"--seek_nexts={args.seek_nexts}", f"--seed={seed}",
                               "--key_size=16", "--value_size=100", "--disable_wal=1",
                               "--compression_type=none", "--threads=1", "--batch_size=1",
                               "--histogram=1", "--use_direct_reads=0",
                               "--write_buffer_size=67108864",
                               f"--cache_size={args.cache_mib << 20}",
                               f"--bloom_bits={args.bloom_bits}",
                               f"--max_background_jobs={args.background_jobs}",
                               f"--max_write_buffer_number={args.rocksdb_write_buffers}",
                               "--cache_index_and_filter_blocks=0", "--row_cache_size=0"]
                print(f"round {round_number} {label}: {shlex.join(command)}", flush=True)
                result = run_one(command, directory, engine)
                result.update(round=round_number, label=label, seed=seed)
                results.append(result)
                (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    print(f"completed {len(results)} runs; results: {output / 'results.json'}")


if __name__ == "__main__":
    main()
