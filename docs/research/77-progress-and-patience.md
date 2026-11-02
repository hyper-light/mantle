# 77. How long an asker waits: stated progress, carried deadlines, hedged slowness

Status: research note, 2026-10-29. For the waits between a client and a node and between nodes
(node.md §3.8, §5.4; gateway.md §6; `vendor/hyper-transport/src/progress.rs`,
`crates/client/src/lib.rs`, `crates/node/src/node.rs`). Sources [1] to [6], [9] and [10] were read
from the vendor references, standards and papers named, each quoted where it was read; [7] and [8]
are quoted from node.md §3.8 and research/04 §A9.1, where they were read.

## The question

A client gives a node a period floored at the node's serve period, the idle timeout of 3.07 s
(gateway.md §6), and the node judges each of its own asks of its peers by that same timeout. A
get's first body byte waits on several such asks in turn and an upload run's answer on its chunks'
writes and three commits, so on a machine slow enough to stretch one ask to a period the client
cuts a node that is working: on CI's busy runner a node's chunk read from a peer took 3,039 ms and
its client cut the get at 3,080 ms, and runs cut while their node wrote were sent again, their
first chunks left on full volumes (measurements/2026-10-29-a-node-cut-while-it-works.md). With no
period at all, a client waited two hours on a node that had stopped answering
(measurements/2026-10-28-transfer-scale.md). node.md §3.8 asks each wait to cover "the remote
side's measured service time", which a sudden slowing outruns. The question is how an asker tells a
server at work from one that is not, and what bounds the work itself, from one laptop to a cell
serving agents by the billion.

## Sources

[1] Amazon S3 API Reference, CompleteMultipartUpload,
https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html (read
2026-10-28): "The processing of a CompleteMultipartUpload request could take several minutes to
finalize. After Amazon S3 begins processing the request, it sends an HTTP response header that
specifies a `200 OK` response. While processing is in progress, Amazon S3 periodically sends white
space characters to keep the connection from timing out. A request could fail after the initial
`200 OK` response has been sent."

[2] RFC 2518, https://www.rfc-editor.org/rfc/rfc2518.txt (read 2026-10-29), §10.1 "102
Processing": "The 102 (Processing) status code is an interim response"; "This status code SHOULD
only be sent when the server has a reasonable expectation" that the request will take significant
time; "if a method is taking longer than 20 seconds (a reasonable, but arbitrary value) to process
the server SHOULD return a 102 (Processing) response"; "The server MUST send a final response after
the request has been completed."

[3] Beyer, Jones, Petoff and Murphy (eds.), *Site Reliability Engineering*, O'Reilly 2016, ch. 22
"Addressing Cascading Failures", https://sre.google/sre-book/addressing-cascading-failures/ (read
2026-10-28), "Deadline propagation": "Rather than inventing a deadline when sending RPCs to
backends, servers should employ deadline propagation." "With deadline propagation, a deadline is
set high in the stack (e.g., in the frontend)." "The tree of RPCs emanating from an initial request
will all have the same absolute deadline."

[4] gRPC, Deadlines guide, https://grpc.io/docs/guides/deadlines/ (read 2026-10-28): "In these
cases where your server also acts as a client you would want to honor the deadline set by the
original client." "To address this gRPC converts the deadline to a timeout from which the already
elapsed time is already deducted."

[5] Marc Brooker, "Timeouts, retries, and backoff with jitter", Amazon Builders' Library,
https://builder.aws.com/content/3EumjoZascWd1oZiEgL8ORlv3qE/timeouts-retries-and-backoff-with-jitter
(read 2026-10-29): "So at Amazon, when we make one service call another service, we choose an
acceptable rate of false timeouts (such as 0.1%)." "Then, we look at the corresponding latency
percentile on the downstream service (p99.9 in this example)." "If each layer retries
independently, the load on the database will increase 243x, making it unlikely to ever recover."
"We have found that we can mitigate this risk by limiting retries locally using a token bucket."
"Jitter adds some amount of randomness to the backoff to spread the retries around in time."

[6] Armon Dadgar, James Phillips and Jon Currey, "Lifeguard: Local Health Awareness for More
Accurate Failure Detection", arXiv 1707.00788 (2017, revised 2018), https://arxiv.org/abs/1707.00788
(read 2026-10-29), abstract: "slow message processing can cause SWIM to mark healthy members as
failed", a "false positive failure detection" that Lifeguard, "showing that it drastically reduces
the rate of false positives", answers with each member's awareness of its own health.

[7] RFC 9000 §10.1, as node.md §3.8 reads it: a connection lives while acknowledgements arrive, and
its idle timeout is "at least three probe timeouts".

[8] Jeffrey Dean and Luiz André Barroso, "The Tail at Scale", CACM 56(2), 2013, as quoted in
research/04 §A9.1: hedged requests sent after "the 95th-percentile expected latency"; "Permitting
more aggressive use of hedged requests with moderate resource consumption requires faster
cancellation of requests."

[9] botocore, Config reference, https://docs.aws.amazon.com/botocore/latest/reference/config.html
(read 2026-10-31): "`read_timeout` (float or int) – The time in seconds till a timeout exception is
thrown when attempting to read from a connection. The default is 60 seconds."

[10] AWS CLI v2, global options, https://docs.aws.amazon.com/cli/latest/reference/index.html (read
2026-10-31): "`--cli-read-timeout` (int) The maximum socket read time in seconds. If the value is set
to 0, the socket read will be blocking and not timeout. The default value is 60 seconds."

## Findings

F1. A server whose work can outlast its client's patience says that it is still at work, and the
final answer, success or failure, follows: S3 by bytes inside an answer already begun [1], HTTP by
an interim response before the final one [2]. The statement is what keeps the request alive, not the
client's guess at how long the work takes.

F2. A server that asks others on a client's behalf honors that client's deadline: one absolute
deadline for the whole tree of requests [3], carried as the time remaining at each hop so that no
clock is compared across machines [4]. Work past the deadline is work nobody waits for.

F3. Where a timeout stands between services, it is set from the downstream's measured latency at
the percentile that matches an acceptable rate of false timeouts, p99.9 for 0.1% [5]. Retries at
every layer multiply load, 243 times over five layers of three attempts [5], so they are limited by
a local token bucket and spread with jitter [5].

F4. A slow copy is answered by asking another after the 95th-percentile latency, not by cutting the
first [8], and the loser is cancelled fast [8]; node.md §5.4 already takes this for chunk reads.

F5. A judge that is itself slow takes healthy peers for failed [6]: a waiting side starved of CPU must
not count its own lateness against the peer it waits on.

## What it means for mantle

D1. Whether a server is at work is stated, not inferred from latency (F1, F5): while a served
exchange's owner has work moving for it, it says so at a beat the asker's judgement counts, and an
asker cuts only an exchange whose statements stopped or a connection that died ([7]). A server stating
nothing, a lost request among them, is given up within a period of its last statement, as one that
never made one is. This needs hyper-transport, which mantle shares with focal and slates: proposed to
its maintainers 2026-10-29, reviewed 2026-10-31 (below), and built that day on hyper-raft branch
`progress-notes` and in mantle's node on branch `progress-notes`; it lands on the owner's word.

D2. Every request carries its deadline as time remaining, and every node asks its peers within what
remains less its own share (F2; node.md §5.4: "A request's deadline is its client's patience");
past it, the node stops the work and frees what it holds. A request with no deadline, the command
line's, is bounded by its server's own bound: no wait in a node is without one (CLAUDE.md rule 2).

D3. Latency decides hedging, never liveness (F3, F4): a chunk read past its measured quantile is
asked of another copy (node.md §5.4), and a timeout that remains is set from the measured p99.9.

D4. A request sent again with its identity joins the work it began or is answered from what that
work made, and work abandoned frees what it wrote at once (node.md §5.4's identities), so a retry
never repeats work or leaves chunks behind; retries are held by a token bucket with jitter (F3).

## D1's design, as reviewed

Proposed to hyper-transport's maintainers on 2026-10-29 and reviewed by hyper-raft's agent on
2026-10-31, which found no hyper-quic blocker; it awaits the owner's word.

- **Wire.** A unidirectional control stream on each connection, which QUIC gives natively, carries
  notes, each the varint ids of every exchange vouched for since the last: ordered, and a few bytes
  a note. An interim reply prefix, HTTP's 1xx [2], was the other way: it covers only the asking
  phase, before a reply's prefix, while the case traced most is a get paused between blocks after
  its body began.
- **Serving side.** The owner vouches for a served exchange only when its own work for it advanced:
  one of its asks completed, or a byte of its reply was made. Never on a timer, which would make a
  note a keepalive that hides a stuck server. The transport sends at most a note a beat on each
  connection, naming only exchanges served, not yet replied whole and not refused.
- **Asking side.** A note is progress in either phase: asking, the window of what the asker has
  had starts again; answering, the window counts as moved, so a body paused mid-way is judged by
  the server's statement and not the clock. Silence past the bound is suspicion: an exchange whose
  notes stop is cut at the end of the window its last note began, within a period of it, and a
  dead connection is found by the idle timeout as now.
- **Beat.** A third of the asker's period, so two lost notes still leave margin, as RFC 9000's
  idle timeout is three probe timeouts [7] and hyper-liveness's silent periods are counted.
- **Period.** Both sides derive it from what both know rather than a field in the request: one
  less thing carried, and nothing to get wrong across a restart.
- **A fresh id for each attempt.** An exchange asked again after a cut is a new exchange with a new
  id, and a note naming one its asker gave up is ignored, so a late note never revives an orphaned
  attempt (the runs cut and sent again on CI's busy runner, measurements/2026-10-29).
- **The owner's bound.** The owner's work is bounded (CLAUDE.md rule 2): past its bound it answers
  a typed refusal, which ends its notes, so no note keeps a stuck job alive; with D2 the bound is
  the time its request has left.

What it would close, as traced: a get cut 9 ms after the chunk it waited on came (CI's busy runner,
measurements/2026-10-29); runs cut while their node wrote, their first chunks orphaned (the same);
and a 40 GiB put on the box whose chunk to a peer was cut `Lost(Stalled)` while that peer waited on
its own device, the block `Unplaced` and the request answered `503 SlowDown`
(measurements/2026-10-28-transfer-scale.md). Any server whose answer waits on its own asks would
use it, focal's seal and slates' included.

The notes were built that day, and on CI's busy runner the node also vouches for a job as each piece
of a chunk it sends a peer goes (measurements/2026-10-29). On the box they did not close the 20 GiB
puts: all three nodes keep their volumes on one file system whose flushes stalled 5 to 6 s, a
receiver whose device holds a chunk answers nothing and says nothing, and behind it the serving
node's own volume refused `Busy` and its transport `Lost(Budget)` (measurements/2026-10-28).

## D2's design, as the owner chose it

On 2026-11-01 the owner chose that a node waits as long as its client would: "Yes, wait as long as
the client would", each request carrying how long its client will wait, 60 s if it does not say, and
a node whose disk is still busy with a write saying so. This changes D1's rule, which vouched only
for work that advanced, so hyper-transport's maintainers review it and the owner approves it before
it lands.

- **What carries it.** A native request's head carries the time its client will still wait, in
  nanoseconds counted from its sending, 0 for none: gRPC's deadline "converted to a timeout from
  which the already elapsed time is already deducted" [4], so no clock is compared across machines.
  A chunk's put or get a node asks of a peer for a request carries what remains of the request's
  time when it is asked.
- **A request that carries none.** An S3 request, and one whose client gives no deadline, is given
  the patience of the stock S3 clients, 60 s between bytes, botocore's and the AWS CLI's default
  read timeout [9, 10]: a node working for it longer works for a client that has gone.
- **What waits within it.** While its request has time left: a chunk refused for want of room,
  `Busy` at a volume's full writer queue or `Lost(Budget)` at the transport, is asked again of the
  same volume once a put there answers or a transfer ends, not sent on to the next volume or ended
  `Unplaced`; a peer's chunk whose put its device holds, submitted and not yet durable, is vouched
  for each beat; and a job is vouched for each beat while an ask of its own is held, out on an
  exchange its transport still keeps (by its server's statements or its moving bytes) or in flight
  at its own device. A server whose loop stops says nothing, a peer that dies is found by the
  transport as before, and a device that never answers is given up at the deadline.
- **Past it.** The job stops: what it asked is let go, its chunks are left to the cleaner (D4's
  prompt freeing is not built), and its request is answered `503 SlowDown` if its client is there.
- **What it changes of D1.** A statement that work is held, by the node's own device or by a peer's
  that says so, counts as one that it moved. The deadline is what makes it safe: it ends, so no
  statement keeps a stuck job alive past the time its client would wait.

Built on mantle branch `carried-deadlines` (node.md §3.8): the native request head and a chunk's
put and get heads carry the time left; a job's patience is its client's deadline or 60 s past its
work's last move, the earlier; a job whose own work is held, an ask of its own out or a chunk
waiting for room, ends `503 SlowDown` past it and vouches each beat within it, but a get whose head
has gone out, its reply begun, is told nothing more past it, its chunk reads given up by its
transport once their peers say nothing, and a job waiting on its own client is the transport's to
judge; a node vouches each beat for a peer's chunk its volume holds within the time the chunk's
head carried and never past the stock patience; a chunk refused `Busy` or
`Lost(Budget)` is asked again of the same volume when room frees (a chunk answered or a volume's put
answered), not at its own refusal. It replaces D1's vouch on a volume's answered put, which the held
rule includes.

## D2's design, as reviewed

Quiver, hyper-transport's maintainer, reviewed D2 on 2026-11-01 and found it sound: proving liveness
only while real work is in flight, the client's patience the hard bound. Its three server-side cases
and its one requirement of the asker, as the branch meets them:

- **Vouches bottom out in real work.** A node vouches for a job only while an ask of its own is out
  or a chunk of it waits for room, and for a peer's chunk only while its device holds the put,
  submitted and unanswered. A job's patience runs from its work's last movement, an ask answered or
  a piece of a chunk read or sent, never a note it hears, so no statement extends another and no
  cycle keeps work alive; a chunk's put or get asks no one.
- **A failed write is no work held.** A volume answers a write or flush that fails at once and
  fences itself (chunk-store.md §4; Rebello et al., ATC 2020), and the put leaves what the node
  vouches for with that answer: the vouch is gated on the I/O in flight, not on "not yet durable".
- **A cap on the carried time.** A job carries at most the stock patience past its work's last
  movement, so a node now waits on a chunk at most that long whatever its head says. A client's own
  deadline needs no cap: a job's patience is its deadline or 60 s past its last movement, the
  earlier.
- **The asker bounds itself.** A native client with a deadline checks it before opening an
  exchange and while reading its reply, and races each source or sink wait against that deadline
  on its own clock whatever its node says (`mantle_client`'s attempt). Queued transport events
  cannot outrank an expired deadline. A node ends a held job at
  its own bound on its own clock whatever its peers say. A request with no deadline is bounded
  locally by silence, the stock patience without bytes or notes, and a node's notes keep it while
  its work is held. Telling held from moved in the note itself would let the asker apply the
  patience to held stretches on its own clock; that is a hyper-transport change, put to its
  maintainers.

The review also found two places that broke the rule: a get past its patience was still vouched for
while a chunk read was out, though its client had stopped waiting, and a get still reading its
version was exempt from `SlowDown` before its reply began. The first is now told nothing more and
the second ends `SlowDown`, as every other job does.

Regression checks cover the GET job before and after its reply begins, the chunk's capped wait,
and the native client's deadline while a loopback node continues sending working notes. They also
cover a deadline already expired on a connected client (previously a transport configuration
refusal) and a GET whose sink stops taking bytes (previously able to finish after its deadline).
