#!/usr/bin/env python3
"""The tables and the trial report of a `mantle bench doors` batch (scripts/doors-trials.sh),
regenerated from its raw outputs alone: python3 scripts/doors-tables.py DIR writes DIR/tables.md
and DIR/trial-report.md. Standard library only.

A cell is a machine x door (or comparator) x operation x size x condition x arm; a tail case's arm
carries -tail, its own cells. Its n is the transfers its cases measured; its quantiles are each
case's, from the harness's histogram, and a dash where the case's samples do not resolve them at
95% (measurement.md §5a: 6 samples for a median, 368 for a p99, 3,688 for a p99.9); across several
rounds a cell shows the median of its rounds' values, and a p99.9 only where every round resolved
one. The A/A band is A2's value against A1's, cell by cell, and summarized by condition."""

import json
import re
import statistics
import sys
from pathlib import Path

NAME = re.compile(r"^(?P<machine>box|mac|vm)_(?P<cond>[a-z0-9]+)_(?P<arm>[A-Za-z0-9-]+)_r(?P<round>[0-9]+)$")
MACHINES = ["box", "vm", "mac"]
SIZES = ["0B", "1B", "65535B", "64KiB", "65537B", "128KiB", "8MiB", "64MiB", "set1000", "set10000", "set100000"]
# The quantile each sample count resolves at 95% (measurement.md §5a).
P999_N = 3688
OPS = ["PUT", "PUT-STREAM", "GET", "GET-RANGE", "HEAD", "DELETE", "LIST", "MPU-CREATE", "MPU-PART",
       "MPU-LIST-PARTS", "MPU-COMPLETE", "MPU-ABORT"]
CONDS = ["quiet", "busy1x", "busy2x", "cpu1", "cpu2", "memfloor"]
ARMS = ["A1", "A2", "C", "minio", "garage", "seaweedfs", "rustfs"]
ARMS += [a + "-tail" for a in ARMS]
# The failure kinds a phase's note names (crates/mantle/src/bench_doors.rs `Failure::tag`).
NOTE_KINDS = ("excluded:", "failure:", "refusal:", "unanswered:")


def number(text):
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def phase(tokens):
    """A phase line's fields: its door, operation, size and condition, then its measures."""
    door, op, size, cond = tokens[1:5]
    fields = {"door": door, "op": op, "size": size, "cond": cond}
    i = 5
    while i < len(tokens):
        key = tokens[i]
        if key == "latency_us":
            fields["p50"], i = tokens[i + 2], i + 3
            fields["p50_lo"], fields["p50_hi"], i = tokens[i + 1], tokens[i + 2], i + 3
            for q in ("p99", "p99.9", "p99.99", "max"):
                fields[q], i = tokens[i + 1], i + 2
            continue
        if key == "lateness_us":
            fields["lateness_p99"], i = tokens[i + 2], i + 3
            continue
        fields[key] = tokens[i + 1] if i + 1 < len(tokens) else None
        i += 2
    return fields


def read_case(path):
    case = {"phases": [], "notes": [], "slopes": [], "handshakes": [], "verified": None, "controls": [],
            "harness": {}}
    for line in path.read_text(errors="replace").splitlines():
        tokens = line.split()
        if len(tokens) < 3 or tokens[0] != "doors":
            continue
        if len(tokens) > 5 and tokens[5] == "clients":
            case["phases"].append(phase(tokens))
        elif len(tokens) > 5 and tokens[5] in NOTE_KINDS:
            case["notes"].append({"door": tokens[1], "op": tokens[2], "size": tokens[3], "cond": tokens[4],
                                  "kind": tokens[5].rstrip(":"), "why": " ".join(tokens[6:])})
        elif tokens[2] == "slope" and "connections" in tokens:
            k = tokens.index("connections")
            case["slopes"].append({"door": tokens[1], "low": tokens[k + 1], "high": tokens[k + 2],
                                   "server_live": tokens[k + 4], "server_footprint": tokens[k + 6],
                                   "client_live": tokens[k + 8]})
        elif tokens[2] == "handshake":
            f = dict(zip(tokens[4::2], tokens[5::2]))
            k = tokens.index("latency_us")
            f.update({"door": tokens[1], "p50": tokens[k + 2], "max": tokens[k + 4]})
            case["handshakes"].append(f)
        elif tokens[1] == "verified":
            case["verified"] = " ".join(tokens[2:])
            case["verified_counts"] = dict(zip(tokens[2:12:2], tokens[3:13:2]))
        elif tokens[1] == "harness":
            case["harness"] = dict(zip(tokens[2::2], tokens[3::2]))
        elif tokens[1] == "control":
            case["controls"].append(" ".join(tokens[2:]))
    return case


def read_quic(raw, name):
    """A QUIC comparator case's runs, by label: secnetperf's and quinn-perf's results, hyper-quic's."""
    runs = {}
    for path in sorted(raw.glob(name + ".*.out")):
        label = path.name[len(name) + 1:-4]
        if label == "server":
            continue
        text = path.read_text(errors="replace")
        run = {}
        m = re.search(r"Result: ([0-9]+) RPS, Latency,us 0th: [0-9]+, 50th: ([0-9]+), 90th: ([0-9]+), 99th: ([0-9]+), "
                      r"99\.9th: ([0-9]+), 99\.99th: ([0-9]+).*Max: ([0-9]+)", text)
        if m:
            run = {"rps": m[1], "p50_us": m[2], "p99_us": m[4], "p99.9_us": m[5], "p99.99_us": m[6], "max_us": m[7]}
        found = re.findall(r"RPS: ([0-9.]+) \(([0-9]+) requests", text)
        m = found[-1] if found else None
        if m:
            run = {"rps": m[0], "requests": m[1]}
            tail = text[text.find("Stream metrics"):]
            for row in ("P50", "P90", "P100"):
                r = re.search(r"\b" + row + r"\s+[│|]\s+(\S+)\s+[│|]\s+(\S+)\s+[│|]\s+(\S+)", tail)
                if r:
                    run[row + "_up"], run[row + "_down"], run[row + "_fbl"] = r[1], r[2], r[3]
        m = re.search(r"open loop, over one RTT \(ms\): n=([0-9]+) p50=(\S+) \[(\S+)\] p99=(\S+) \[\S+\] "
                      r"p99\.9=(\S+) \[\S+\] p99\.99=(\S+) \[\S+\] max=(\S+) ms", text)
        if m:
            run = {"n": m[1], "over_p50_ms": m[2], "p50_95_ms": m[3], "over_p99_ms": m[4], "over_p99.9_ms": m[5],
                   "over_p99.99_ms": m[6], "over_max_ms": m[7]}
        if not run:
            last = [l for l in text.splitlines() if l.strip()]
            run = {"unread": (last[-1] if last else "empty")[:160]}
        runs[label] = run
    return runs


def read_host(path):
    out = {}
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        tokens = line.split()
        if tokens and tokens[0] in ("start", "end"):
            f = {"load": " ".join(tokens[4:7])}
            f.update(dict(zip(tokens[7::2], tokens[8::2])))
            out[tokens[0]] = f
    return out


def median(values):
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def show(value, digits=1):
    if value is None:
        return "-"
    if abs(value) >= 1000:
        return f"{value:,.0f}"
    return f"{value:.{digits}f}"


def per_op_cpu(p):
    user, system = number(p.get("user_ns/op")), number(p.get("system_ns/op"))
    return None if user is None or system is None else user + system


def main(top):
    raw = top / "raw"
    lines = (top / "plan.tsv").read_text().splitlines()
    header = lines[0].split("\t")
    plan = [dict(zip(header, l.split("\t"))) for l in lines[1:] if l.strip()]
    cases = {}
    for path in sorted(raw.glob("*.status")):
        name = path.name[:-len(".status")]
        m = NAME.match(name)
        if not m:
            continue
        status = path.read_text().strip()
        rusage = raw / (name + ".rusage")
        cases[name] = {
            "machine": m["machine"], "cond": m["cond"], "arm": m["arm"], "round": int(m["round"]), "status": status,
            "rusage": json.loads(rusage.read_text()) if rusage.exists() and rusage.read_text().strip() else {},
            "host": read_host(raw / (name + ".host")),
            "run": read_case(raw / (name + ".out")) if (raw / (name + ".out")).exists() else None,
            "quic": read_quic(raw, name) if m["arm"].startswith("quic-") else {},
        }

    # Every phase, keyed by its cell; a comparator's door is http@ARM, mantle's arms native/http.
    cells = {}
    for name, c in cases.items():
        if not c["run"]:
            continue
        for p in c["run"]["phases"]:
            key = (c["machine"], p["door"], p["op"], p["size"], p["cond"], c["arm"])
            cells.setdefault(key, []).append(p)

    out = []
    w = out.append
    w(f"# Tables: {top.name}\n")
    w("Regenerated by `python3 scripts/doors-tables.py` from the raw outputs in `raw/`. Latency in microseconds "
      "from each operation's intended start (open loop); a dash where the case's samples do not resolve the quantile "
      "at 95% (6 samples for a median, 368 for a p99, 3,688 for a p99.9) or the measure is not given. Several rounds: "
      "the median of the rounds' values, n summed; a p99.9 only where every round resolved one.\n")
    order = {s: i for i, s in enumerate(SIZES)}

    def sort_key(k):
        machine, door, op, size, cond, arm = k
        return (MACHINES.index(machine) if machine in MACHINES else 99, door.startswith("http@"), door,
                CONDS.index(cond) if cond in CONDS else 99, order.get(size, 99), OPS.index(op) if op in OPS else 99,
                ARMS.index(arm) if arm in ARMS else 99)

    def med(ps, field):
        return median(number(p.get(field)) for p in ps)

    def p999(ps):
        """A cell's p99.9: the median of its rounds' where every round resolved one on 3,688 or more."""
        values = [number(p.get("p99.9")) for p in ps]
        if any(v is None for v in values) or any(int(p.get("ops") or 0) < P999_N for p in ps):
            return None
        return median(values)

    def total(ps, field):
        return sum(int(number(p.get(field)) or 0) for p in ps)

    w("## Latency and cost per operation\n")
    w("ops/s is the phase's operations over its elapsed time; lateness p99 is how late an operation started "
      "after its intended start (open loop): where it is near the latency, the door ran at its capacity and the "
      "latency is mostly the queue's.\n")
    w("Arms A1 and A2 are the timed build (the A/A pair), C the counting build; comparators run the HTTP door "
      "with A1 as the harness; an arm's -tail cases ask 3,688 operations a sized phase. cpu = the servers' "
      "user+system ns/op (from /proc ticks); instr and cycles = `perf stat` user-space counts of the servers' "
      "processes over the phase; faults = minor faults/op; wire = the door's wire bytes/op (the native door's UDP "
      "payloads, the HTTP door's TCP octets); dgrams = the native door's datagrams/op. ref/unans/skip/diff are the "
      "operations refused, never answered, skipped (their put had failed) and read back differing; failed are the "
      "checks' failures.\n")
    w("| machine | door | op | size | cond | arm | n | ops/s | lateness p99 | p50 [95%] | p99 | p99.9 | p99.99 | max "
      "| cpu ns/op | instr/op | cycles/op | faults/op | wire B/op | dgrams/op | ref | unans | skip | diff | failed |")
    w("|" + "---|" * 25)
    for key in sorted(cells, key=sort_key):
        ps = cells[key]
        machine, door, op, size, cond, arm = key
        n = sum(int(p.get("ops") or 0) for p in ps)
        if len(ps) == 1:
            p50 = f"{ps[0]['p50']} [{ps[0]['p50_lo']}–{ps[0]['p50_hi']}]"
        else:
            p50 = show(med(ps, "p50"))
        q = {k: show(med(ps, k)) for k in ("p99", "p99.99", "max")}
        ms = sum(number(p.get("elapsed_ms")) or 0 for p in ps)
        rate = show(n / ms * 1000 if ms else None)
        w(f"| {machine} | {door} | {op} | {size} | {cond} | {arm} | {n} | {rate} | {show(med(ps, 'lateness_p99'))} | "
          f"{p50} | {q['p99']} | {show(p999(ps))} | {q['p99.99']} | {q['max']} | "
          f"{show(median(per_op_cpu(p) for p in ps), 0)} | {show(med(ps, 'instructions/op'), 0)} | "
          f"{show(med(ps, 'cycles/op'), 0)} | {show(med(ps, 'minor_faults/op'))} | {show(med(ps, 'wire_bytes/op'), 0)} | "
          f"{show(med(ps, 'datagrams/op'))} | {total(ps, 'refused')} | {total(ps, 'unanswered')} | "
          f"{total(ps, 'skipped')} | {total(ps, 'differed')} | {total(ps, 'failed')} |")

    w("\n## Counts per operation (the counting arms, C and C-tail)\n")
    w("The servers' allocator counts per operation across the cell's three `mantle serve` processes; their peak "
      "live heap over the phase above what they held as it began, over the most requests out at once (per request) "
      "and over the connections the phase held (per connection); the client's (mantle-client or the HTTP request "
      "writer) and the harness's own set-aside work; the servers' footprint; the harness's threads.\n")
    w("| machine | door | op | size | cond | arm | allocs/op | reallocs/op | moved/op | bytes/op | peak/request | "
      "peak/connection | client allocs/op | client bytes/op | harness allocs/op | harness peak B | footprint B | threads |")
    w("|" + "---|" * 18)
    for key in sorted((k for k in cells if k[5] in ("C", "C-tail")), key=sort_key):
        ps = cells[key]
        f = {k: show(med(ps, k)) for k in
             ("allocs/op", "reallocs/op", "moved/op", "bytes/op", "peak_live/request", "peak_live/connection",
              "client_allocs/op", "client_bytes/op", "harness_allocs/op", "harness_peak_live_bytes",
              "server_footprint_bytes", "threads")}
        w(f"| {' | '.join(key)} | {f['allocs/op']} | {f['reallocs/op']} | {f['moved/op']} | {f['bytes/op']} | "
          f"{f['peak_live/request']} | {f['peak_live/connection']} | {f['client_allocs/op']} | {f['client_bytes/op']} | "
          f"{f['harness_allocs/op']} | {f['harness_peak_live_bytes']} | {f['server_footprint_bytes']} | {f['threads']} |")

    # The A/A band, cell by cell: A2 against A1 (A2-tail against A1-tail), each measure both resolve.
    measures = ("ops/s", "p50", "p99", "p99.9", "instructions/op")

    def value(ps, measure):
        if measure == "ops/s":
            n = sum(int(p.get("ops") or 0) for p in ps)
            ms = sum(number(p.get("elapsed_ms")) or 0 for p in ps)
            return n / ms * 1000 if ms else None
        if measure == "p99.9":
            return p999(ps)
        return med(ps, measure)

    w("\n## A/A band, cell by cell (A2 against A1)\n")
    w("(A2 − A1) / A1 in percent for each measure both arms resolve; a dash where either does not. An effect in a "
      "cell under its band is not resolved by this batch.\n")
    w("| machine | door | op | size | cond | arms | " + " | ".join(measures) + " |")
    w("|" + "---|" * (6 + len(measures)))
    band = {}
    for key in sorted(cells, key=sort_key):
        arm = key[5]
        if arm not in ("A1", "A1-tail"):
            continue
        other = cells.get(key[:5] + (arm.replace("A1", "A2"),))
        if not other:
            continue
        row = []
        for measure in measures:
            a, b = value(cells[key], measure), value(other, measure)
            if a and b is not None:
                d = (b - a) / a * 100
                band.setdefault((key[0], key[4], measure), []).append(d)
                row.append(f"{d:+.1f}%")
            else:
                row.append("-")
        w(f"| {' | '.join(key[:5])} | {arm}/{arm.replace('A1', 'A2')} | " + " | ".join(row) + " |")

    w("\n## A/A band by condition\n")
    w("Per machine and condition, the median and the 90th percentile of the cells' absolute differences above.\n")
    w("| machine | cond | measure | cells | median abs diff % | p90 abs diff % | median signed diff % |")
    w("|---|---|---|---|---|---|---|")
    for (machine, cond, measure) in sorted(band, key=lambda k: (MACHINES.index(k[0]) if k[0] in MACHINES else 99,
                                                                CONDS.index(k[1]) if k[1] in CONDS else 99, k[2])):
        d = band[(machine, cond, measure)]
        absd = sorted(abs(x) for x in d)
        p90 = absd[min(len(absd) - 1, int(0.9 * len(absd)))]
        w(f"| {machine} | {cond} | {measure} | {len(d)} | {show(statistics.median(absd))} | {show(p90)} | "
          f"{show(statistics.median(d))} |")

    w("\n## Transfers behind each reported p99.9\n")
    resolved = [(k, ps) for k, ps in cells.items() if p999(ps) is not None]
    least = min((min(int(p.get("ops") or 0) for p in ps) for _, ps in resolved), default=None)
    w(f"Cells with a p99.9: {len(resolved)}; the fewest transfers behind one round's p99.9: {least} "
      f"(at least {P999_N} required, measurement.md §5a).\n")

    w("\n## Per connection: memory slope and handshakes\n")
    w("| case | door | connections | server live B/conn | server footprint B/conn | client live B/conn | handshakes | allocs/handshake | bytes/handshake | p50 us | max us |")
    w("|---|---|---|---|---|---|---|---|---|---|---|")
    for name in sorted(cases):
        run = cases[name]["run"]
        if not run:
            continue
        hs = {h["door"]: h for h in run["handshakes"]}
        for s in run["slopes"]:
            h = hs.get(s["door"], {})
            w(f"| {name} | {s['door']} | {s['low']}→{s['high']} | {s['server_live']} | {s['server_footprint']} | "
              f"{s['client_live']} | {h.get('handshakes', '-')} | {h.get('allocs/handshake', '-')} | "
              f"{h.get('bytes/handshake', '-')} | {h.get('p50', '-')} | {h.get('max', '-')} |")

    w("\n## The harness: its own peak and threads, and the run's outcome\n")
    w("| case | harness peak live B | threads | idle threads | verified |")
    w("|---|---|---|---|---|")
    for name in sorted(cases):
        run = cases[name]["run"]
        if not run or not (run["harness"] or run["verified"]):
            continue
        h = run["harness"]
        w(f"| {name} | {h.get('peak_live_bytes', '-')} | {h.get('threads', '-')} | {h.get('idle_threads', '-')} | "
          f"{run['verified'] or '-'} |")

    w("\n## QUIC: secnetperf, quinn-perf and bare hyper-quic\n")
    w("Loopback, server and client pinned together. secnetperf `-exec:lowlat -rstream:1 -runtime:5s` (latency per "
      "request in us); quinn-perf `--duration 5` (requests a second and per-stream upload/download durations); "
      "hyper-quic's geo_open_loop at 200 requests/s of 100 B, latency over one RTT in ms, through its relay (0 ms, "
      "and 500 ms one way under quiet).\n")
    w("| case | run | results |")
    w("|---|---|---|")
    for name in sorted(cases):
        for label, run in cases[name]["quic"].items():
            w(f"| {name} | {label} | {' '.join(f'{k} {v}' for k, v in run.items())} |")

    w("\n## Host per case\n")
    w("| case | status | wall s | load start | load end | MemAvailable GiB start | PSI io some10 start→end | rusage user s | system s | maxrss MiB | majflt |")
    w("|---|---|---|---|---|---|---|---|---|---|---|")
    for name in sorted(cases):
        c = cases[name]
        h, r = c["host"], c["rusage"]
        st, en = h.get("start", {}), h.get("end", {})
        mem = number(st.get("memavailable_kib"))
        rss = r.get("maxrss", r.get("maxrss_kib")) or 0
        rss_mib = rss / 1048576 if r.get("maxrss_unit") == "B" else rss / 1024
        w(f"| {name} | {c['status'].split()[0]} | {r.get('wall_s', '-')} | {st.get('load', '-')} | {en.get('load', '-')} | "
          f"{show(mem / 1048576 if mem else None)} | {st.get('psi_io_some10', '-')}→{en.get('psi_io_some10', '-')} | "
          f"{show(r.get('user_s'))} | {show(r.get('system_s'))} | {show(rss_mib)} | {r.get('majflt', '-')} |")

    compare = Path("/home/adalundhe/Projects/mantle-compare/COMPARATORS.md")
    if compare.exists():
        text = compare.read_text()
        w("\n## Comparator configurations (from mantle-compare/COMPARATORS.md)\n")
        start = text.find("| Comparator |")
        w(text[start:text.find("\n\n", start)].strip())
        w("")
        for heading in re.findall(r"^### .*$", text, re.M):
            w(f"- {heading[4:]}")

    (top / "tables.md").write_text("\n".join(out) + "\n")

    # The trial report: every planned case completed, failed with its reason, or excluded with its reason.
    rep = [f"# Trial report: {top.name}\n"]
    tally = {"completed": 0, "failed": 0, "excluded": 0, "not run": 0}
    rows = []
    for p in plan:
        kind, cond, arm, rnd = p["kind"], p["condition"], p["arm"], p["round"]
        machine = p.get("machine", "box")
        label = {"quic": "quic-" + arm, "tail": arm + "-tail"}.get(kind, arm)
        name = f"{machine}_{cond}_{label}_r{rnd}"
        c = cases.get(name)
        if not c:
            tally["not run"] += 1
            rows.append(f"| {p['seq']} | {name} | not run | |")
            continue
        state, _, rest = c["status"].partition(" ")
        tally[state] = tally.get(state, 0) + 1
        secs, _, why = rest.partition(" ")
        detail = why
        if state == "completed" and c["run"]:
            notes = c["run"]["notes"]
            excl = sorted({f"{n['door']} {n['op']} {n['size']}: {n['why']}" for n in notes if n["kind"] == "excluded"})
            fail = sorted({f"{n['door']} {n['op']} {n['size']} {n['kind']}: {n['why']}" for n in notes
                           if n["kind"] != "excluded"})
            detail = f"{secs} s; verified {c['run']['verified']}"
            if excl:
                detail += f"; {len(excl)} phases excluded by the server: " + "; ".join(excl)
            if fail:
                detail += f"; {len(fail)} phases with failures: " + "; ".join(fail)
        elif state == "failed" and c["run"]:
            done = len(c["run"]["phases"])
            detail = f"{secs} s, {done} phases measured before it ended; {why}"
        rows.append(f"| {p['seq']} | {name} | {state} | {detail} |")
    rep.append(f"Planned {len(plan)}: completed {tally['completed']}, failed {tally['failed']}, "
               f"excluded {tally['excluded']}, not run {tally['not run']}.\n")
    rep.append("| seq | case | state | detail |")
    rep.append("|---|---|---|---|")
    rep.extend(rows)
    (top / "trial-report.md").write_text("\n".join(rep) + "\n")
    print(f"wrote {top / 'tables.md'} and {top / 'trial-report.md'}: {tally}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: doors-tables.py BATCH-DIR")
    main(Path(sys.argv[1]))
