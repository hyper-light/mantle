# 77. How long an asker waits: stated progress, carried deadlines, hedged slowness

Status: research note, 2026-10-29. For the waits between a client and a node and between nodes
(node.md §3.8, §5.4; gateway.md §6; `vendor/hyper-transport/src/progress.rs`,
`crates/client/src/lib.rs`, `crates/node/src/node.rs`). Sources [1] to [6] were read from the
vendor references, standards and papers named, each quoted where it was read; [7] and [8] are
quoted from node.md §3.8 and research/04 §A9.1, where they were read.

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
nothing, a lost request among them, is found within two beats. This needs hyper-transport, which
mantle shares with focal and slates; proposed to its maintainers 2026-10-29, awaiting the owner's
word.

D2. Every request carries its deadline as time remaining, and every node asks its peers within what
remains less its own share (F2; node.md §5.4: "A request's deadline is its client's patience");
past it, the node stops the work and frees what it holds. A request with no deadline, the command
line's, is bounded by its server's own bound: no wait in a node is without one (CLAUDE.md rule 2).

D3. Latency decides hedging, never liveness (F3, F4): a chunk read past its measured quantile is
asked of another copy (node.md §5.4), and a timeout that remains is set from the measured p99.9.

D4. A request sent again with its identity joins the work it began or is answered from what that
work made, and work abandoned frees what it wrote at once (node.md §5.4's identities), so a retry
never repeats work or leaves chunks behind; retries are held by a token bucket with jitter (F3).
