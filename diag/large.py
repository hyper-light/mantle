#!/usr/bin/env python3
"""DIAGNOSTIC (diag-nfc2): where each large PUT's time went, from the nodes' traces: its time from
start to end split by what it had out at each moment: nothing (its body coming and being sealed),
chunks (to the volumes, with or without a command), or commands alone (Block, File, Name)."""
import sys, glob, os, re, collections

START, END, ASK, ANSWER = 22, 23, 27, 28

def main(d, least_ms=50.0):
    for f_pid in sorted({re.match(r'trace-(\d+)-', os.path.basename(f)).group(1) for f in glob.glob(os.path.join(d, 'trace-*.txt'))}):
        evs = []
        for f in sorted(glob.glob(os.path.join(d, f'trace-{f_pid}-*.txt'))):
            for line in open(f):
                if line.startswith('e '):
                    _, t, k, a, b, c = line.split()
                    k = int(k)
                    if k in (START, END, ASK, ANSWER):
                        evs.append((int(t), k, int(a), int(b), int(c)))
        evs.sort()
        jobs = collections.defaultdict(list)
        for ev in evs:
            jobs[ev[2]].append(ev)
        for jid, je in jobs.items():
            if not je or je[0][1] != START or je[-1][1] != END:
                continue
            total = (je[-1][0] - je[0][0]) / 1e6
            if total < least_ms:
                continue
            out = {}  # (kind, ask) -> (t, name)
            split = collections.Counter()
            names = {1: 'cmd', 2: 'read', 3: 'chunk', 11: 'Block', 12: 'File', 13: 'Name'}
            t_prev = je[0][0]
            most_chunks = 0
            chunk_lat, cmd_lat = collections.defaultdict(list), []
            for ev in je[1:]:
                kinds = [n for (_, n) in out.values()]
                state = 'chunks' if 'chunk' in kinds else ('commands' if any(n in ('Block', 'File', 'Name', 'cmd') for n in kinds) else ('read' if 'read' in kinds else 'body/seal'))
                split[state] += (ev[0] - t_prev) / 1e6
                t_prev = ev[0]
                if ev[1] == ASK:
                    key = (ev[3] if ev[3] < 10 else 1, ev[4])
                    out[key] = (ev[0], names.get(ev[3], str(ev[3])))
                    most_chunks = max(most_chunks, sum(1 for (_, n) in out.values() if n == 'chunk'))
                elif ev[1] == ANSWER:
                    key = (ev[3], ev[4])
                    if key in out:
                        t0, n = out.pop(key)
                        (chunk_lat['chunk'] if n == 'chunk' else cmd_lat).append((ev[0] - t0) / 1e6)
            def med(xs):
                xs = sorted(xs)
                return xs[len(xs) // 2] if xs else float('nan')
            cl = chunk_lat['chunk']
            print(f"pid {f_pid} job {jid}: {total:.0f} ms; " + ", ".join(f"{k} {v:.0f} ms ({100*v/total:.0f}%)" for k, v in split.most_common()) +
                  f"; chunks {len(cl)} median {med(cl):.1f} ms most at once {most_chunks}; commands {len(cmd_lat)} median {med(cmd_lat):.1f} ms")

if __name__ == '__main__':
    main(sys.argv[1], float(sys.argv[2]) if len(sys.argv) > 2 else 50.0)
