#!/usr/bin/env python3
"""DIAGNOSTIC (diag-nfc2): records when kiln's headroom pacer freezes and thaws each of this
user's kiln run cgroups, on CLOCK_MONOTONIC (the trace's clock), every 20 ms, until killed or
until `--for` seconds pass. Writes only to its output file, which should be on tmpfs, so it is
not itself a writer the pacer stops."""
import glob, os, sys, time

out = sys.argv[1]
until = time.monotonic() + float(sys.argv[2]) if len(sys.argv) > 2 else float('inf')
root = '/sys/fs/cgroup/user.slice/user-%d.slice/user@%d.service' % (os.getuid(), os.getuid())
state = {}
with open(out, 'a', buffering=1) as f:
    f.write(f"start {time.monotonic_ns()}\n")
    while time.monotonic() < until:
        now = time.monotonic_ns()
        seen = set()
        for path in glob.glob(root + '/app.slice/**/cgroup.freeze', recursive=True):
            seen.add(path)
            try:
                v = open(path).read().strip()
            except OSError:
                continue
            if state.get(path) != v:
                if path in state or v == '1':
                    f.write(f"{now} {v} {os.path.dirname(path)[len(root):]}\n")
                state[path] = v
        for path in list(state):
            if path not in seen:
                del state[path]
        time.sleep(0.02)
