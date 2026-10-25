# A client's first operation on a loaded host

**Question.** Why did `a_cell_of_one_made_by_init_serves_from_what_init_wrote` (tests/serve.rs)
fail once in the gates on a68e915a with "quiet for 1s with nothing moved", when it passes alone and
in CI? The harness (crates/mantle/tests/support/cell.rs) fails a wait once nothing has moved for
its quiet period: the longest any node or client states it may wait, and never less than one
second, the retransmission timeout RFC 6298 §2.1 gives a round trip before any is measured.

**What failed.** The cell of one had enlisted; its writer client had begun its first operation, a
CreateBucket, and told nothing yet. Every range was led at term 1, the Bucket range at commit 1,
so the create had not committed; the node, with no peers, stated no patience and could not say it
was held by its device (`Node::held` is its liveness stream's own proof, which a node with no peer
keeps none of); its log's longest flush had been 168 ms. A second passed with nothing moved. The
gates ran the serve suite's ten tests at once beside a multipart test of their own, other
sessions' builds and a hyper-quic suite; the host's load averages were 7.7, 12.6 and 14.6.

**The client's own timing.** A client's first exchange is unmeasured: its probe timeout is RFC 9002
§6.2.1's at the initial estimates, `333 ms + max(4 × 166.5 ms, 1 ms) + 25 ms = 1.024 s`
(`mantle_node::bounds::initial_pto`), past the harness's one second, so a first flight lost is
sent again only after the harness has given up. A connection refused before the node took the
request is asked again after a pause between half and all of that probe timeout. Once it has
measured an exchange, `Client::patience` states no less than the idle timeout, three probe
timeouts at the initial estimates (RFC 9000 §10.1), 3.07 s; before, it states nothing.

**Measured.** The serve suite's eleven tests, two suites at once (22 cells on this Mac, an Apple
M5 Max of 18 cores and 128 GiB, 2026-10-25), the harness timing each object client from its start
to its first operation's answer (a scratch timer on the s3-multipart tree, not landed): 470 first
operations.

| | median | p90 | p99 | max |
|---|---|---|---|---|
| client open and learn | 1.0 ms | 1.4 ms | — | 24.5 ms |
| first operation | 27 ms | 580 ms | 2,001 ms | 4,460 ms |

Every test passed in both suites; a first operation's commits are movements the harness sees, so
only a first operation whose first commit comes after a quiet second fails, as the cell of one's
did.

**What changes.** A client counts in the harness from its start at its unmeasured patience, the
idle timeout, until it tells a measured one (`Cell::client_begins`; the cluster harness likewise
for its workload's clients). The one-second floor stays for waits no client is in.
