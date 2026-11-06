#!/usr/bin/env python3
"""The tables and the trial report of a `mantle bench doors` batch (scripts/doors-trials.sh),
regenerated from its raw outputs alone: python3 scripts/doors-tables.py DIR writes DIR/tables.md
and DIR/trial-report.md. Standard library only.

A cell is a door (or comparator) x operation x size x condition x arm. Its n is the transfers its
cases measured; its quantiles are each case's, from the harness's histogram, and a dash where the
case's samples do not resolve them at 95% (measurement.md §5a: 6 samples for a median, 368 for a
p99, 3,688 for a p99.9); across several rounds a cell shows the median of its rounds' values. The
A/A band is A2's value against A1's, cell by cell."""

import json
import re
import statistics
import sys
from pathlib import Path

NAME = re.compile(r"^box_(?P<cond>[a-z0-9]+)_(?P<arm>[A-Za-z0-9-]+)_r(?P<round>[0-9]+)$")
SIZES = ["0B", "1B", "65535B", "64KiB", "65537B", "128KiB", "8MiB", "64MiB", "set10000", "set100000"]
OPS = ["PUT", "PUT-STREAM", "GET", "GET-RANGE", "HEAD", "DELETE", "LIST", "MPU-CREATE", "MPU-PART",
       "MPU-LIST-PARTS", "MPU-COMPLETE", "MPU-ABORT"]
CONDS = ["quiet", "busy1x", "busy2x", "cpu1", "cpu2", "memfloor"]
ARMS = ["A1", "A2", "C", "garage", "seaweedfs", "rustfs"]


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
    case = {"phases": [], "notes": [], "slopes": [], "handshakes": [], "verified": None, "controls": []}
    for line in path.read_text(errors="replace").splitlines():
        tokens = line.split()
        if len(tokens) < 3 or tokens[0] != "doors":
            continue
        if len(tokens) > 5 and tokens[5] == "clients":
            case["phases"].append(phase(tokens))
        elif len(tokens) > 5 and tokens[5] in ("excluded:", "failure:"):
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
        m = re.search(r"RPS: ([0-9.]+) \(([0-9]+) requests", text)
        if m:
            run = {"rps": m[1], "requests": m[2]}
            tail = text[text.find("Stream metrics"):]
            for row in ("P50", "P90", "P100"):
                r = re.search(r"\b" + row + r"\s+│\s+(\S+)\s+│\s+(\S+)\s+\|\s+(\S+)", tail)
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
    for path in sorted(raw.glob("box_*.status")):
        name = path.name[:-len(".status")]
        m = NAME.match(name)
        if not m:
            continue
        status = path.read_text().strip()
        rusage = raw / (name + ".rusage")
        cases[name] = {
            "cond": m["cond"], "arm": m["arm"], "round": int(m["round"]), "status": status,
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
            key = (p["door"], p["op"], p["size"], p["cond"], c["arm"])
            cells.setdefault(key, []).append(p)

    out = []
    w = out.append
    w(f"# Tables: {top.name}\n")
    w("Regenerated by `python3 scripts/doors-tables.py` from the raw outputs in `raw/`. Latency in microseconds "
      "from each operation's intended start (open loop); a dash where the case's samples do not resolve the quantile "
      "at 95% (6 samples for a median, 368 for a p99, 3,688 for a p99.9) or the measure is not given. Several rounds: "
      "the median of the rounds' values, n summed.\n")

    w("## Latency and cost per operation\n")
    w("Arm A1 and A2 are the timed build (the A/A pair), C the counting build; comparators run the HTTP door "
      "with A1 as the harness. cpu = the servers' user+system ns/op (from /proc ticks); instr and cycles = "
      "`perf stat` user-space counts of the servers' processes over the phase; faults = minor faults/op.\n")
    w("| door | op | size | cond | arm | n | p50 [95%] | p99 | p99.9 | p99.99 | max | cpu ns/op | instr/op | cycles/op | faults/op | failed |")
    w("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    order = {s: i for i, s in enumerate(SIZES)}

    def sort_key(k):
        door, op, size, cond, arm = k
        return (door.startswith("http@"), door, CONDS.index(cond) if cond in CONDS else 99,
                order.get(size, 99), OPS.index(op) if op in OPS else 99, ARMS.index(arm) if arm in ARMS else 99)

    for key in sorted(cells, key=sort_key):
        ps = cells[key]
        door, op, size, cond, arm = key
        n = sum(int(p.get("ops") or 0) for p in ps)
        if len(ps) == 1:
            p50 = f"{ps[0]['p50']} [{ps[0]['p50_lo']}–{ps[0]['p50_hi']}]"
        else:
            p50 = show(median(number(p["p50"]) for p in ps))
        q = {k: show(median(number(p[k]) for p in ps)) for k in ("p99", "p99.9", "p99.99", "max")}
        failed = sum(int(p.get("failed") or 0) for p in ps)
        w(f"| {door} | {op} | {size} | {cond} | {arm} | {n} | {p50} | {q['p99']} | {q['p99.9']} | {q['p99.99']} | "
          f"{q['max']} | {show(median(per_op_cpu(p) for p in ps), 0)} | "
          f"{show(median(number(p.get('instructions/op')) for p in ps), 0)} | "
          f"{show(median(number(p.get('cycles/op')) for p in ps), 0)} | "
          f"{show(median(number(p.get('minor_faults/op')) for p in ps))} | {failed} |")

    w("\n## Counts per operation (the counting arm, C)\n")
    w("The servers' allocator counts per operation across the cell's three `mantle serve` processes, and the "
      "harness thread's own beside its set-aside work.\n")
    w("| door | op | size | cond | allocs/op | reallocs/op | moved/op | bytes/op | peak_live/request | client allocs/op | client bytes/op |")
    w("|---|---|---|---|---|---|---|---|---|---|---|")
    for key in sorted((k for k in cells if k[4] == "C"), key=sort_key):
        ps = cells[key]
        f = {k: show(median(number(p.get(k)) for p in ps)) for k in
             ("allocs/op", "reallocs/op", "moved/op", "bytes/op", "peak_live/request", "client_allocs/op",
              "client_bytes/op")}
        w(f"| {key[0]} | {key[1]} | {key[2]} | {key[3]} | {f['allocs/op']} | {f['reallocs/op']} | {f['moved/op']} | "
          f"{f['bytes/op']} | {f['peak_live/request']} | {f['client_allocs/op']} | {f['client_bytes/op']} |")

    w("\n## A/A band (A2 against A1)\n")
    w("Per cell, (A2 − A1) / A1 of the median latency, of the p99 where both resolve it, and of the servers' "
      "instructions/op; then per condition the median and the 90th percentile of the absolute differences. An effect "
      "under this band is not resolved by this batch.\n")
    band = {}
    for key in cells:
        if key[4] != "A1":
            continue
        other = cells.get(key[:4] + ("A2",))
        if not other:
            continue
        for measure in ("p50", "p99", "instructions/op"):
            a = median(number(p.get(measure)) for p in cells[key])
            b = median(number(p.get(measure)) for p in other)
            if a and b is not None:
                band.setdefault((key[3], measure), []).append((b - a) / a * 100)
    w("| cond | measure | cells | median abs diff % | p90 abs diff % | median signed diff % |")
    w("|---|---|---|---|---|---|")
    for (cond, measure) in sorted(band, key=lambda k: (CONDS.index(k[0]) if k[0] in CONDS else 99, k[1])):
        d = band[(cond, measure)]
        absd = sorted(abs(x) for x in d)
        p90 = absd[min(len(absd) - 1, int(0.9 * len(absd)))]
        w(f"| {cond} | {measure} | {len(d)} | {show(statistics.median(absd))} | {show(p90)} | "
          f"{show(statistics.median(d))} |")

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
        w(f"| {name} | {c['status'].split()[0]} | {r.get('wall_s', '-')} | {st.get('load', '-')} | {en.get('load', '-')} | "
          f"{show(mem / 1048576 if mem else None)} | {st.get('psi_io_some10', '-')}→{en.get('psi_io_some10', '-')} | "
          f"{show(r.get('user_s'))} | {show(r.get('system_s'))} | {show((r.get('maxrss_kib') or 0) / 1024)} | "
          f"{r.get('majflt', '-')} |")

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
        name = f"box_{cond}_{'quic-' + arm if kind == 'quic' else arm}_r{rnd}"
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
            fail = sorted({f"{n['door']} {n['op']} {n['size']}: {n['why']}" for n in notes if n["kind"] == "failure"})
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
