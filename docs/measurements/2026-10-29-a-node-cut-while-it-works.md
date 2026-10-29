# A node cut while it works

**Question.** Why did CI's busy job (linux-x86_64-2cpu-busy: two CPUs beside two busy loops, a
debug build) fail the native suite (`crates/mantle/tests/native.rs`) on four runs in a row from
18:36 on 2026-10-28, after six passes? Runs 40746651744 (5fee021c), 40762704095 (b928eb56),
40794052095 (750a1525) and 40802695796 (9cbddcf4) failed one of two ways: a read-back
`mantle cp s3://resumed/<key> -` ended "the exchange ended unanswered (Stalled)", or the `changed`
copy of `a_copy_killed_at_a_run_boundary_or_mid_run_resumes_and_a_changed_source_is_refused`,
expected to end in `BadDigest`, was refused `InsufficientStorage` at its fourth run. b928eb56 is
913abd1a with the roll's stop mended and none of the chunk store's changes since 913abd1a, the
other three carry them.

**Reproduced, two arms.** A scratch branch (`diag/native-get`, never merged; run 40813935158) ran
the suite whole on six of GitHub's busy runners, CI's own conditions, with this tree's
`crates/chunk` at 913abd1a and at 9cbddcf4, each arm built once and run in turn, every process
tracing to a file of its own under `MANTLE_DIAG_DIR`: each client exchange opened and how it ended,
each node's range reads asked, each ask held 2 s or more, and every chunk ask's wait. A runner's
rounds ended at its first failure; runner 2 ran its six.

| Arm | Suite runs | Passed | Read-back cut `Stalled` | `changed` refused `InsufficientStorage` |
|---|---|---|---|---|
| 9cbddcf4 | 10 | 8 | 2 | 0 |
| `crates/chunk` at 913abd1a | 10 | 7 | 1 | 2 |

Both fail both ways at like rates: neither the cleaning pass's plan (fb57d055) nor the read that
finds a moved record again (d5eadd39) is the cause. A second branch (`diag/native-space`, run
40816091299) ran 9cbddcf4's suite six times on each of six runners, every round run, each node's
volumes traced as they changed: 27 of 36 passed, 7 read-backs were cut `Stalled`, 2 copies refused
`InsufficientStorage`. The fastest runner, 341 to 363 s a round, passed all six.

**A get cut while its node read.** A node sends a get's head as soon as the object's current version
is read, then asks the file's header, its extents and the block's row of their ranges, and the
block's chunk of a volume, before the body's first byte; it asks the chunk as one span of the
block's wanted bytes, 8 MiB here, whole, and has nothing to send until all of it is in. The node's
transport judges each of those asks at the idle timeout, 3.07 s. The client judges the answer by its
bytes, a datagram's worth each period, and its period is at least that same idle timeout, the node's
serve period (gateway.md §6). Runner 5's failing read-back, by the traces of the client and of the
node it asked:

| ms after the get was asked | what the traces said |
|---|---|
| 0 | the client asks the get of node `a19ede20` (the head before it answered in 30 ms) |
| 1 to 26 | the node asks the file's header, extents and block row, each answered at once |
| 3,071 | the node's chunk ask of a peer answers, 3,039 ms after it was asked |
| 3,080 | the client cuts the get `Stalled`: its period brought less than a datagram |

The node lost to its client by 9 ms, and would have lost by any margin: an ask of a peer that takes
most of the node's own period leaves the node no time to answer within the client's. Runner 1's
base-arm failure was cut at the same point, 3,088 ms after the get was asked, its node having asked
the get's reads 8 to 45 ms after it.

**A run cut while its node wrote, and the space it held.** A native run is cut the same way where the
node, holding the run whole, writes its chunks and commits its Block, File and Name commands for
longer than the client's period: on these runners a run took 5.3 to 9.2 s to answer. The client
asks the durable offset again and sends the run again, its period doubled after the cut, and the
second send commits; the chunks the first made stay on every node, uncommitted. The suite's volumes
are 256 MiB each, 19 segments of 8,404,992 bytes once laid out. In the round of the second branch
that `changed` failed (runner 6, round 1):

| Copy | Its runs cut `Stalled` with the request whole, and sent again | Full chunks a node held of it, beyond its committed runs |
|---|---|---|
| `file` (three runs and 77 bytes) | the first, after 6,148 ms | 1 |
| `boundary`, before its kill | the first, after 6,147 ms | |
| `boundary`, resumed | the third, after 6,147 ms | 2 |
| `mid`, before and after its kill | none | 0 |
| `changed`, before its kill and resumed | none | 0 |

Each node then held 20 chunks where the committed runs are 17, 143,766,832 bytes live, 16 segments
sealed, 2 open and 1 free, the cleaner's reserve, and the fourth run of `changed` was refused
`InsufficientStorage` 2,147 ms after it was asked. The other round refused (runner 2, round 5) held
the same: 20 chunks, the same live bytes, the reserve free, four exchanges cut with their requests
whole.

**Cause.** A client cannot tell a node at work from a stuck one. The node states nothing while its
own asks run, and the client gives it the same period the node gives each of those asks; a get's
first body byte waits on several of them in turn, a run's answer on its chunks' writes and three
commits. Some of the node's waits have no bound at all: a read paused for its range's leader is
asked with no deadline. A machine slow enough to stretch one ask to a period, a debug build on two
CPUs beside two busy loops here, cuts working nodes: the reads fail, and the runs sent again leave
their first chunks behind until the cell is full.

Raising the client's period or asking again quiets this without mending it (CLAUDE.md §3); the
client's floor for a fresh exchange, and its asking again, were tried and reverted (b9b2a5b8,
docs/measurements/2026-10-28-transfer-scale.md). What mends it is a design decision, with the
evidence each rests on:

- The node states its progress. A served exchange's owner says it is still at work, at least once
  a period, while its own asks move, and the asker's judgement counts it; the node's own bound
  ends the work in a typed refusal. Amazon S3 does this for CompleteMultipartUpload: "After Amazon
  S3 begins processing the request, it sends an HTTP response header that specifies a 200 OK
  response. While processing is in progress, Amazon S3 periodically sends white space characters
  to keep the connection from timing out" (Amazon S3 API Reference, CompleteMultipartUpload). It
  needs hyper-transport, the transport mantle shares with focal and slates.
- The deadline is propagated. The client's request carries its time left, the node gives its own
  asks what remains less its own share, and answers a typed refusal when the time is out: "Rather
  than inventing a deadline when sending RPCs to backends, servers should employ deadline
  propagation" (Beyer et al., *Site Reliability Engineering*, O'Reilly 2016, ch. 22); "In these
  cases where your server also acts as a client you would want to honor the deadline set by the
  original client" (gRPC, Deadlines guide). The command line asks with no deadline today, so one
  must be derived for it.
- The bounds compose. The client gives a node the node's own derived bound between its answer's
  bytes, the sum of the asks it makes in turn and their attempts, and the node holds itself to it.
  mantle alone, no change on the wire; a stuck node is found only after that bound, tens of
  seconds rather than a period.

Apart from the decision, a get's node could pass each sealed segment on as it arrives and verifies,
where it now waits for the block's whole span: the client would hear the bytes moving from the peer
through the node as they move, and the first byte would come after a segment rather than a block.
