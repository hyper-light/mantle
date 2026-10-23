# 64. Starting and stopping a node

Status: research note, 2026-10-21. For node.md §1.4–§1.7: how `mantle serve` stops when its host
asks it to, how it reports a start that fails, and what a closing connection owes its peer. The
decisions this note supports are in node.md §1.7.

## The question

`mantle node` runs as a child of its test harness: it learns its peers' addresses from standard
input, and stops when that input closes. `mantle serve` runs under a service manager or an
orchestrator, which stop it with a signal and expect it to be gone within a stated time, and to
which a start that fails must say why. Three facts decide its shape: how an orchestrator stops a
process, how a process tells why it failed to start, and what QUIC requires of a connection that
closes while the process that holds it goes away.

## Sources

### Kubernetes: how a pod's containers are stopped

Kubernetes documentation, "Pod Lifecycle", §"Termination of Pods" (kubernetes.io, read
2026-10-21):

- "kubelet makes requests to the container runtime to attempt to stop the containers in the pod by
  first sending a TERM (aka. SIGTERM) signal, with a grace period timeout, to the main process in
  each container."
- "The kubelet triggers the container runtime to send a TERM signal to process 1 inside each
  container."
- "The default `terminationGracePeriodSeconds` setting is 30 seconds."
- "When the grace period expires, if there is still any container running in the Pod, the kubelet
  triggers forcible shutdown. The container runtime sends `SIGKILL` to any processes still running
  in any container in the Pod."
- A `preStop` hook runs before the TERM, inside the same grace period ("If the `preStop` hook is
  still running after the grace period expires, the kubelet requests a small, one-off grace period
  extension of 2 seconds").

### sysexits(3): exit codes for failures

FreeBSD's manual page for sysexits(3) (man.freebsd.org, read 2026-10-21) lists `EX_USAGE` (64)
through `EX_CONFIG` (78), and says of the interface: "This interface has been deprecated and is
retained only for compatibility." "Its use is discouraged." "This interface is not portable." "The
choice of an appropriate exit value is often ambiguous." Windows has no such convention.

### RFC 9000 §10.2: an immediate close

- "An endpoint sends a CONNECTION_CLOSE frame (Section 19.19) to terminate the connection
  immediately."
- "The closing and draining connection states exist to ensure that connections close cleanly and
  that delayed or reordered packets are properly discarded. These states SHOULD persist for at
  least three times the current PTO interval as defined in [QUIC-RECOVERY]."
- "Endpoints that have some alternative means to ensure that late-arriving packets do not induce a
  response, such as those that are able to close the UDP socket, MAY end these states earlier to
  allow for faster resource recovery. Servers that retain an open socket for accepting new
  connections SHOULD NOT end the closing or draining state early."
- §10.2.1: "An endpoint in the closing state sends a packet containing a CONNECTION_CLOSE frame in
  response to any incoming packet that it attributes to the connection."

### The Raft dissertation §3.10: leadership transfer

Ongaro's dissertation, §3.10, as research 06 §A1.3 reads it: the leader stops taking new client
requests, brings the target's log up to its own, and sends it `TimeoutNow`, upon which the target
campaigns at once; a transfer that has not completed after about an election timeout is abandoned
and the leader takes requests again. hyper-durable exposes it as `Replica::transfer`.

### The memory a process may use

Linux's cgroup v2 documentation (docs.kernel.org, "Control Group v2", Memory Interface Files, read
2026-10-21):

- `memory.max`: "The default is “max”. Memory usage hard limit. This is the main mechanism to
  limit memory usage of a cgroup. If a cgroup’s memory usage reaches this limit and can’t be
  reduced, the OOM killer is invoked in the cgroup."
- `memory.high`: "The default is “max”. Memory usage throttle limit. If a cgroup’s usage goes over
  the high boundary, the processes of the cgroup are throttled and put under heavy reclaim
  pressure. Going over the high limit never invokes the OOM killer and under extreme conditions the
  limit may be breached."

A container's memory limit in a pod is its cgroup's `memory.max`. hyper-rt reads the tightest
`memory.max` (v1's `memory.limit_in_bytes`) up the process's cgroup path and a finite `RLIMIT_AS` or
`RLIMIT_DATA`, beside the machine's physical memory (`hyper_rt::machine::facts::MemoryFacts`);
`memory.high` throttles and never kills, so it bounds no admission, and a Windows job's limit is
owed.

### How long a scheduler lets a thread wait to run

hyper-rt's calibration (`Calibration::measure`) stops each probe at Wilks' 59 samples, a 95 %/95 %
bound on its p99 (hyper-raft `docs/runtime.md` §10.2), so its wall budget only bounds a host too
starved to schedule the probe 59 times. A runnable thread waits at most about one scheduling slice
of each thread ahead of it:

- Linux (`kernel/sched/fair.c`, v6.12): "Minimal preemption granularity for CPU-bound tasks:
  (default: 0.75 msec * (1 + ilog(ncpus)), units: nanoseconds)", `sysctl_sched_base_slice =
  750000ULL`, the factor taken over `min_t(unsigned int, num_online_cpus(), 8)` CPUs: at most 3 ms.
- macOS (XNU `osfmk/kern/sched_prim.c`): `#define DEFAULT_PREEMPTION_RATE 100 /* (1/s) */` and
  `std_quantum_us = (1000 * 1000) / default_preemption_rate`: 10 ms.
- Windows: Microsoft documents the quantum's setting, not its length ("This interval is specified
  as a range because threads can be preempted and processor time is not precisely determined",
  `Win32PrioritySeparation`, learn.microsoft.com, read 2026-10-22): by default a server takes
  "longer, fixed-length processor intervals" and a client "shorter, variable intervals" (the same
  page), and "Quantum durations are typically longer on a server" ("CPU Analysis",
  learn.microsoft.com, read 2026-10-22). A Microsoft engineer measured the clock interval at
  15.625 ms with ClockRes, wrote that "The quantum that the scheduler assigns to a thread is a
  multiple of the tick interval", and gave a foreground thread's default as "a quantum of 18 or 6
  tick intervals", "about 94 milliseconds" ("Know Thy Tick", Microsoft's embedded blog, 2006,
  archived on learn.microsoft.com, read 2026-10-22). No Microsoft page gives a server's quantum
  in intervals.

## What follows for mantle

1. **Stop on TERM within the grace period.** `mantle serve` takes SIGTERM and SIGINT (Ctrl-C on
   Windows, its console close and shutdown) as an orderly stop (hyper-rt `signal::subscribe`), and
   a second as a stop now. The orderly stop's steps (node.md §1.7) run within a bound the operator
   states beside the orchestrator's grace period, and a stop past it ends as a crash would, which
   is safe because nothing is acknowledged before it is durable.
2. **Hand a range's lead over before stopping.** For each range it leads, the node transfers the
   lead to a member whose log matches its own (§3.10) and waits until that member shows it leads,
   so the cell elects nothing after a node leaves on purpose; one that does not take it within an
   election is left to elect as after a crash.
3. **Close connections and keep the closing state.** The stopping node closes its connections and
   keeps answering their late packets for three probe timeouts before it exits (§10.2: it holds
   the socket its peers know), so its peers learn of the close at once rather than at their idle
   timeout. A client, which closes its socket as it ends, sends its close once (§10.2's "MAY end
   these states earlier"): a peer that did not receive it learns of the close only at its idle
   timeout, and refuses the client's next connection under the same identity while the old one
   holds its place (hyper-transport's admission), which the client waits out (`Client::object`).
4. **Failures are typed, not numbered.** A start that fails exits non-zero with a typed error that
   names the step that failed and why (the configuration line, the key file and its mode, the
   device another process holds, the address in use); sysexits is not used, since its own page
   deprecates it and Windows has no counterpart.
5. **Memory is the operator's, or the OS's on the operator's behalf.** A stated ceiling governs;
   absent one, a limit the OS sets for the process (cgroup v2, a Windows job), which in an
   orchestrator is the operator's statement; absent both, physical memory. node.md §1.4's floor,
   the least that admits one unit of every class of work, is what the node refuses to start below,
   and what remains past it sizes the volumes' indexes and the engines' resident metadata.
6. **A probe's budget is its samples at a quantum each.** hyper-rt's probes stop at 59 samples,
   and each waits at most about one quantum behind a runnable thread ahead of it, so the budget is
   59 of the platform's quanta: 3 ms on Linux, 10 ms on macOS, Microsoft's measured 6 intervals of
   15.625 ms on Windows. On a starved Windows server, whose quantum Microsoft gives only as
   longer, the wake probe keeps fewer wakes than its floor and reports them as quick, which each
   shard's own wakes replace within their first few dozen (hyper-rt `machine::wake`); it refuses
   only a host on which it measured none.
7. **A restart leaves no session behind.** The dissertation's sessions expire by count or by the
   log's time, and a restarted client registers anew (research 06 §A1.8); a range's table then
   holds its owner's old sessions until they expire, and a node that restarts more often than the
   lifetime finds the table full. A registration that names its owner, the owner's run (a node's
   run record, raised at every start before anything else) and its place among the run's
   registrations lets the range end the owner's last session as a later registration arrives:
   the process that held it has ended, since one directory admits one process, or let it go
   holding nothing under it. The same registration delivered again is answered its session, and
   an earlier one is refused, so a registration delivered late ends no session whose commands
   may be out, which a command asked again under a new session would apply twice. The sessions a
   range holds are then the identities the cell's configuration pins, each at most one, and no
   expiry by time is needed for them.
