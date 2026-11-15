#!/usr/bin/env python3
"""DIAGNOSTIC (diag-nfc2): reads a run's trace windows and says, per harness phase, what each
node's admission did and where requests waited."""
import sys, os, re, glob, collections, statistics

LIMIT, PHASE = 10, 11
PARK, UNPARK, START, END, REFUSE, LOST, BOUND_SLOW, ASK, ANSWER, DOOR_TAKEN = 20, 21, 22, 23, 24, 25, 26, 27, 28, 29
TURN, TURN_PART = 40, 41
ENTRY_OUT, ENTRY_ANSWERED = 31, 32
OP_START, OP_END, PAUSE = 80, 81, 82
CL_AGAIN, CL_SLOWED = 91, 92

def q(xs, p):
    if not xs:
        return float('nan')
    xs = sorted(xs)
    i = min(len(xs) - 1, max(0, int(round(p * (len(xs) - 1)))))
    return xs[i]

def load(d):
    procs = collections.defaultdict(lambda: {'events': [], 'notes': [], 'heads': []})
    for f in sorted(glob.glob(os.path.join(d, 'trace-*.txt'))):
        m = re.match(r'trace-(\d+)-(\d+)\.txt', os.path.basename(f))
        pid = int(m.group(1))
        p = procs[pid]
        with open(f) as fh:
            head = fh.readline().strip()
            p['heads'].append(head)
            m = re.search(r'at (\d+) label \S+ events \d+ dropped \d+ turns (\d+) busy_ns (\d+) wait_ns (\d+) status utime (\d+) stime (\d+)', head)
            if m:
                p.setdefault('windows', []).append(tuple(int(x) for x in m.groups()))
            for line in fh:
                if line.startswith('e '):
                    _, t, k, a, b, c = line.split()
                    p['events'].append((int(t), int(k), int(a), int(b), int(c)))
                elif line.startswith('n '):
                    _, t, rest = line.rstrip('\n').split(' ', 2)
                    p['notes'].append((int(t), rest))
    for p in procs.values():
        p['events'].sort()
        p['label'] = 'phase' if any('label phase' in h for h in p['heads']) else 'node'
    return procs

def main(d, only=None):
    procs = load(d)
    harness = [pid for pid, p in procs.items() if p['label'] == 'phase']
    nodes = sorted(pid for pid, p in procs.items() if p['label'] == 'node')
    if not harness:
        print('no harness trace'); return
    h = procs[harness[0]]
    phases = []
    for t, text in h['notes']:
        m = re.match(r'phase (\d+) door "(\w+)" kind (\S+) clients (\d+) ops (\d+)\.\.(\d+) elapsed_ns (\d+)', text)
        if m:
            k, door, kind, clients, a, b, el = m.groups()
            phases.append(dict(k=int(k), door=door, kind=kind, clients=int(clients), ops=int(b) - int(a), end=t, start=t - int(el)))
    t0 = phases[0]['start'] if phases else 0
    for ph in phases:
        if only and ph['k'] not in only:
            continue
        s, e = ph['start'], ph['end']
        el = (e - s) / 1e9
        print(f"\n=== phase {ph['k']} {ph['door']} {ph['kind']} clients {ph['clients']} ops {ph['ops']} elapsed {el:.1f}s rate {ph['ops']/el:.1f}/s  (t={((s-t0)/1e9):.1f}s)")
        ops = [ev for ev in h['events'] if s <= ev[0] <= e + 1]
        lat = [ev[3] / 1e6 for ev in ops if ev[1] == OP_END]
        outc = collections.Counter(ev[4] for ev in ops if ev[1] == OP_END)
        pauses = [ev[3] / 1e6 for ev in ops if ev[1] == PAUSE]
        cl_again = [ev[3] / 1e6 for ev in ops if ev[1] == CL_AGAIN]
        cl_slow = [ev[3] / 1e6 for ev in ops if ev[1] == CL_SLOWED]
        if lat:
            print(f"  harness: ops {len(lat)} p50 {q(lat,.5):.1f}ms p90 {q(lat,.9):.1f} p99 {q(lat,.99):.1f} max {max(lat):.1f}  outcomes {dict(outc)}")
        if pauses or cl_again or cl_slow:
            print(f"  waits: harness-slowed {len(pauses)} sum {sum(pauses):.0f}ms; client-again {len(cl_again)} sum {sum(cl_again):.0f}ms; client-slowed {len(cl_slow)} sum {sum(cl_slow):.0f}ms max {max(cl_slow) if cl_slow else 0:.0f}ms")
        # slow ops' times
        slow = [(ev[0], ev[3] / 1e6, ev[4]) for ev in ops if ev[1] == OP_END and ev[3] > 1e9]
        if slow:
            print(f"  slow ops (>1s): {len(slow)}; first ends at +{(slow[0][0]-s)/1e9:.1f}s: " + ", ".join(f"+{(t-s)/1e9:.1f}s {l:.0f}ms o{o}" for t, l, o in slow[:8]))
        for i, pid in enumerate(nodes):
            evs = [ev for ev in procs[pid]['events'] if s <= ev[0] <= e]
            if not evs:
                continue
            c = collections.Counter(ev[1] for ev in evs)
            starts = [ev for ev in evs if ev[1] == START]
            adm = sum(1 for ev in starts if ev[4] & 1)
            slowd = sum(1 for ev in starts if not ev[4] & 1)
            run_max = max(((ev[4] >> 8) & 0xffffff) for ev in starts) if starts else 0
            ends = [ev[3] / 1e6 for ev in evs if ev[1] == END]
            unp = [ev for ev in evs if ev[1] == UNPARK]
            unp_w = [ev[3] / 1e6 for ev in unp]
            over = sum(1 for ev in unp if ev[4] & 1)
            parks = [ev for ev in evs if ev[1] == PARK]
            park_max = max((ev[3] for ev in parks if ev[3] != 2**64 - 1), default=0)
            park_full = sum(1 for ev in parks if ev[3] == 2**64 - 1)
            refl = sum(1 for ev in evs if ev[1] == REFUSE and ev[2] == 1)
            refb = sum(1 for ev in evs if ev[1] == REFUSE and ev[2] == 2)
            door = [ev[2] / 1e6 for ev in evs if ev[1] == DOOR_TAKEN]
            turns = [ev[2] / 1e6 for ev in evs if ev[1] == TURN]
            parts = collections.defaultdict(float)
            for ev in evs:
                if ev[1] == TURN_PART:
                    parts[ev[2]] += ev[3] / 1e6
            print(f"  node{i} pid {pid}: started {adm} slowdown-at-start {slowd} run_max {run_max} ended {len(ends)} p50 {q(ends,.5):.1f}ms p99 {q(ends,.99):.1f} max {max(ends) if ends else 0:.1f}")
            if parks or unp:
                print(f"     parked {len(parks)} (full {park_full}) max_len {park_max}; unparked {len(unp)} over {over}; wait p50 {q(unp_w,.5):.1f}ms p99 {q(unp_w,.99):.1f} max {max(unp_w) if unp_w else 0:.1f}")
            if door:
                print(f"     door waits {len(door)} p50 {q(door,.5):.2f}ms p99 {q(door,.99):.1f} max {max(door):.1f}")
            print(f"     refused limit {refl} bytes {refb}; bound-slowdown {c[BOUND_SLOW]} lost {c[LOST]}")
            if turns:
                print(f"     long turns {len(turns)} sum {sum(turns):.0f}ms max {max(turns):.1f}ms parts " + " ".join(f"{k}:{v:.0f}" for k, v in sorted(parts.items())))
            lims = [ev for ev in evs if ev[1] == LIMIT]
            phs = [ev for ev in evs if ev[1] == PHASE]
            if lims or phs:
                print(f"     admission phases {len(phs)} limit changes {len(lims)}")
                for ev in sorted(lims + phs)[:40]:
                    rel = (ev[0] - s) / 1e9
                    if ev[1] == LIMIT:
                        print(f"       +{rel:7.2f}s LIMIT {ev[2]} -> {ev[3]} (base {ev[4]})")
                    else:
                        L, lat_sum, flags = ev[2], ev[3], ev[4]
                        ta = flags & 1; desc = (flags >> 1) & 0x7f; phase = (flags >> 8) & 0xff; limit = flags >> 16
                        n = 267
                        thr = n / (L / 1e9) if L else 0
                        mean = lat_sum / n / 1e6
                        print(f"       +{rel:7.2f}s PHASE len {L/1e6:.0f}ms X {thr:.1f}/s R {mean:.1f}ms power {thr/mean if mean else 0:.2f} turned_away {ta} descent {desc} phase {phase} limit {limit}")
        # each node's entries per range: commands an entry, those left waiting, and its time out
        for i, pid in enumerate(nodes):
            outs = {}
            per = collections.defaultdict(lambda: {'n': 0, 'cmds': [], 'left': [], 'lat': []})
            for ev in procs[pid]['events']:
                if not (s <= ev[0] <= e):
                    continue
                if ev[1] == ENTRY_OUT:
                    outs[ev[2]] = ev[0]
                    r = per[ev[2]]; r['n'] += 1; r['cmds'].append(ev[3]); r['left'].append(ev[4])
                elif ev[1] == ENTRY_ANSWERED and ev[2] in outs:
                    per[ev[2]]['lat'].append((ev[0] - outs.pop(ev[2])) / 1e6)
            for g, r in sorted(per.items()):
                if r['n'] < 5:
                    continue
                print(f"  node{i} range {g:08x}: entries {r['n']} commands/entry mean {sum(r['cmds'])/len(r['cmds']):.1f} max {max(r['cmds'])}; waiting at send p50 {q(r['left'],.5)} max {max(r['left'])}; out p50 {q(r['lat'],.5):.2f}ms p90 {q(r['lat'],.9):.2f}")
        # each node's log over the phase, from the counts' notes around it
        for i, pid in enumerate(nodes):
            logs = [(t, txt) for t, txt in procs[pid]['notes'] if txt.startswith('log frames')]
            b = [x for x in logs if x[0] <= s + 1]
            a = [x for x in logs if x[0] >= e]
            if a and b:
                def kv(txt):
                    f = txt.split()[1:]
                    return {f[j]: int(f[j+1]) for j in range(0, len(f) - 1, 2) if f[j+1].isdigit()}
                k0, k1 = kv(b[-1][1]), kv(a[0][1])
                d = {k: k1.get(k, 0) - k0.get(k, 0) for k in k1}
                fr = max(1, d.get('frames', 0))
                fl = d.get('flush_count', 0)
                print(f"  node{i} log: frames {d.get('frames',0)} updates {d.get('updates',0)} ({d.get('updates',0)/fr:.1f}/frame) flushes {d.get('flushes',0)} durable_writes {d.get('durable_writes',0)} flushed_confirmations {d.get('flushed_confirmations',0)} mean flush {d.get('flush_sum_ns',0)/max(1,fl)/1e6:.2f}ms; since open p50 flush {k1.get('flush_p50_ns',0)/1e6:.2f}ms commit_wait p50 {k1.get('commit_wait_p50_ns',0)/1e6:.2f}ms")
        # each job's stages: from its start, each ask to its answer and the gaps between
        stages = collections.defaultdict(list)
        for i, pid in enumerate(nodes):
            jobs = collections.defaultdict(list)
            for ev in procs[pid]['events']:
                if s <= ev[0] <= e and ev[1] in (START, END, ASK, ANSWER):
                    jobs[ev[2]].append(ev)
            for jid, evs in jobs.items():
                if not evs or evs[0][1] != START or evs[-1][1] != END:
                    continue
                t_prev = evs[0][0]
                asked = {}
                for ev in evs[1:]:
                    if ev[1] == ASK:
                        name = {1: 'cmd', 2: 'read', 3: 'chunk', 10: 'cmd', 11: 'Block', 12: 'File', 13: 'Name'}.get(ev[3], str(ev[3]))
                        asked[(ev[3] if ev[3] < 10 else 1, ev[4])] = (ev[0], name)
                        if len(asked) == 1:
                            stages['gap before ' + name].append((ev[0] - t_prev) / 1e6)
                    elif ev[1] == ANSWER:
                        k = (ev[3], ev[4])
                        if k in asked:
                            t_a, name = asked.pop(k)
                            stages['ask ' + name].append((ev[0] - t_a) / 1e6)
                            if not asked:
                                t_prev = ev[0]
                    elif ev[1] == END:
                        stages['last answer to end'].append((ev[0] - t_prev) / 1e6)
                        stages['job total'].append(ev[3] / 1e6)
        if stages:
            print("  job stages (ms): " + "; ".join(f"{k} n {len(v)} p50 {q(v,.5):.2f} p90 {q(v,.9):.2f}" for k, v in sorted(stages.items())))
        # each node's loop and CPU over the phase: the windows dumped at its counts around it
        for i, pid in enumerate(nodes):
            ws = sorted(procs[pid].get('windows', []))
            # the window that ends just after the phase end holds the phase's turns
            after = [w for w in ws if w[0] >= e]
            before = [w for w in ws if w[0] <= s + 1]
            if after and before:
                w1, w0 = after[0], before[-1]
                el_s = (w1[0] - w0[0]) / 1e9
                ticks = (w1[4] + w1[5]) - (w0[4] + w0[5])
                cpu = ticks / 100.0
                print(f"  node{i} loop: turns {w1[1]} busy {w1[2]/1e6:.0f}ms wait {w1[3]/1e6:.0f}ms (loop busy {100*w1[2]/max(1,w1[2]+w1[3]):.0f}%); process CPU {cpu:.2f}s over {el_s:.2f}s = {cpu/el_s if el_s else 0:.2f} cores")
        # node notes near the phase end
        for pid in nodes:
            ns = [(t, txt) for t, txt in procs[pid]['notes'] if e - 5e9 <= t <= e + 5e9]
            for t, txt in ns[-1:]:
                print(f"  state pid {pid}: {txt}")

if __name__ == '__main__':
    only = set(int(x) for x in sys.argv[2:]) if len(sys.argv) > 2 else None
    main(sys.argv[1], only)
