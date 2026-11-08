# 79. Learned link liveness: no fixed timeout on a link, and links that outlast a held process

Status: research note, 2026-11-07. For the owner's word on `mantle_protocol::bounds::idle_timeout`
("Why 3.072s? That's an arbitrary number. We don't support that. Learned and data-derived methods
only. Also yes, research longer lived links.") and the trace behind it,
docs/measurements/2026-11-06-node-test-holds.md [25]. Sources [1] to [9] and [21] to [23] are
standards, kernel and library references, read on 2026-11-07 at the addresses given at the end;
[4], [10], [11], [13], [15] to [19] are papers read from the copies named there. [12], [14] and
[20] could not be fetched (a paywall, a server that refused automated reading, one that did not
answer) and are quoted from the works named beside them. [24] and [25] are hyper-raft's and
mantle's own documents and code, read at the commits named. Every claim below carries its source's
number, a file and line of the code, or the run that measured it. Nothing was built or run for this
note.

## The question

Every QUIC connection mantle makes, node to node and client to node, advertises a
`max_idle_timeout` of 3.072 s (`crates/protocol/src/bounds.rs:81-83`, set at `bounds.rs:212`,
advertised at `vendor/hyper-transport/src/endpoint.rs:207, 220`). The number is three times
1.024 s, and 1.024 s is 333 ms plus four times 166.5 ms plus 25 ms (`bounds.rs:73-78`; the test at
`bounds.rs:415-419`). The 333 ms is QUIC's `kInitialRtt`, "the initial RTT [that] SHOULD be set to
333 milliseconds" when "no previous RTT is available", chosen so that handshakes start "with a PTO
of 1 second, as recommended for TCP's initial RTO" [2 §6.2.2]. That 1 s rests on a 2009 survey in
which "the round-trip times of more than 97.5% of the connections observed ... were less than 1
second" [5 App. A]. The factor of three is RFC 9000's floor: "endpoints MUST increase the idle
timeout period to be at least three times the current Probe Timeout (PTO)" [1 §10.1]. The 25 ms is
the default `max_ack_delay` [1 §18.2]. So 3.072 s is the least idle timeout RFC 9000 allows,
computed for a path about which nothing is yet known, then kept for the connection's whole life on
every path. On loopback the handshake measures a round trip of 395 µs (measured:
`vendor/hyper-transport/src/progress.rs:58-59`), for which the RFC's own floor would be about 79 ms
(derived: 3 × (0.395 + 1 + 25) ms by [2 §6.2.1]). The number is neither the RFC's value, since the
RFC gives none, nor a measurement of anything.

It decides a great deal. hyper-quic kills a connection silent for longer than it, or than three
current probe timeouts if that is longer (`vendor/hyper-quic/src/connection/mod.rs:3136`), with
`TimedOut` and nothing sent (`mod.rs:1825-1826`; RFC 9000's "silently closed" [1 §10.1]). The node
then drops the datagram plane's keys for that peer (`crates/node/src/node.rs:2016-2041`), so its
liveness detector can no longer hear the peer until a new connection is made, and every exchange on
the connection ends `Lost(Closed)`. Thirteen more rows of the inventory below are derived from it.

The trace measured what that does on a host that holds a process still [25]. On the box, pinned to
two CPUs beside four busy loops while another agent's 100M bench pressed the host's memory (its zram
swap at 17:43: 32.9 GiB of 122.9 GiB in use, swapping in at 551 MB/s), the node test failed twice
in six rounds. A probe that holds one node with SIGSTOP reproduced it exactly: held 2 s, the test
passed; held 4 s and 5 s, a read ended `Lost(Closed)`. In the 4 s hold a peer lost its link to the
held node `TimedOut` about 3.1 s in; the held node, continued about 4.1 s in, timed out the client's
connection and its link to the third node on its first turn, before reading the datagrams waiting
in its sockets; the
client's own idle timer ran out 4.65 s in. A diagnostic driver that read the sockets before firing
timers changed none of the probes' outcomes, because the client's own timer still ran out first
(all measured: [25], the probes of 2026-11-06 on the box).

So there are two questions. Which bounds on mantle's link, liveness, retry and reconnect path are
fixed, and how can each be learned from data instead; and how can a link outlast a host that holds
its process still for seconds, for tens of seconds, or for good.

## What mantle runs with today

Every fixed time constant, and every count that sets a time, on the path, in mantle and in the
crates it vendors from hyper-raft (snapshot `0a182ae`, `vendor/*/SNAPSHOT`). "Derived" means
computed from rows above it; "cited" names the source the value is taken from; "none" means no
source or measurement gives the value.

| # | Constant | Where | Value | What it decides | Basis today |
|---|---|---|---|---|---|
| 1 | `INITIAL_RTT` | `crates/protocol/src/bounds.rs:18` | 333 ms | The round trip assumed before any sample; the base of rows 5 and 6 | Cited: `kInitialRtt` [2 §6.2.2], RFC 6298's 1 s initial RTO over three [5 (2.1)], from a 2009 survey of paths [5 App. A]. A prior for an unknown path, not a measure of this one |
| 2 | `GRANULARITY` | `bounds.rs:20` | 1 ms | The least a PTO's variance term adds | Cited: the recommended `kGranularity` [2 §6.1.2], which [2 App. A.2] calls "a system-dependent value". Not measured here |
| 3 | `MAX_ACK_DELAY` | `bounds.rs:23` | 25 ms | Added to every PTO mantle computes (rows 5, 6; `measured_pto`, `bounds.rs:67-69`) | Cited: the default [1 §18.2], which "SHOULD include the receiver's expected delays in alarms firing" [1 §18.2]. Not measured |
| 4 | `IDLE_PTOS` | `bounds.rs:26` | 3 | The idle timeout in probe timeouts | Cited: [1 §10.1]'s floor, "at least three times the current Probe Timeout". Applied to the initial PTO, not the current one |
| 5 | `initial_pto()` | `bounds.rs:73-78` | 1.024 s | The first pause of the redial, range-ask, client-round and name-lookup backoffs (`node.rs:992, 1102, 3230, 3602`; `crates/client/src/lib.rs:1014`; `crates/node/src/names.rs:64, 137`) | Derived from rows 1 to 3 by [2 §6.2.1]. [2 §6.2.1] sets `max_ack_delay` to 0 in the Initial and Handshake spaces, the only ones before a sample, where this adds 25 ms |
| 6 | `idle_timeout()` | `bounds.rs:81-83`, set `bounds.rs:212`, advertised `vendor/hyper-transport/src/endpoint.rs:207, 220` | 3.072 s | QUIC's `max_idle_timeout` from every node and client: when a silent connection is discarded | Derived: [1 §10.1]'s floor at the estimates before any sample, kept for the connection's life. `docs/design/node.md:1611-1614` says it "is derived from the path, never fixed" |
| 7 | `serve_period` | `bounds.rs:211` | 3.072 s | How long an exchange a peer opened may move nothing before its server refuses it; admission's quiet period (`endpoint.rs:936`); the notes' beat (row 27) | None of its own: "as long as a connection may stay silent" (`bounds.rs:209-210`) |
| 8 | `keep_alive` | `bounds.rs:215` | 1.536 s | The PING cadence of a connection with nothing else to send | None: half of row 6. The field's own contract is "from the path's measured NAT lifetime" (`endpoint.rs:98`); [6 §3.5] asks for none more often than every 15 s |
| 9 | `backed_off()` | `bounds.rs:87-89` | Doubles, at most 3.072 s | Every later pause of the backoffs in row 5 | Doubling cited [5 (5.5)]. The cap: none; it is row 6, where [5 (2.5)] allows a cap only "provided it is at least 60 seconds" |
| 10 | `STOCK_PATIENCE` | `bounds.rs:31` | 60 s | How long a request stating no patience is waited on: a node's job (`crates/node/src/node/host.rs:110-117`), a peer's held chunk (`node.rs:4059-4068`), a client's exchange at most (`client/src/lib.rs:1777-1778`), the S3 listener's wait for an octet (`crates/node/src/listener/conn.rs:44-47`) | Cited: botocore's `read_timeout` and the AWS CLI's `--cli-read-timeout` defaults (research/77 [9, 10]); the owner's D2 |
| 11 | A node's own asks' period | `crates/node/src/node.rs:2299, 3096, 3167, 3389, 3766` | 3.072 s | The period at which a node's snapshot, chunk put, chunk get, command and read to a peer are judged | None beyond row 6 |
| 12 | The turn's longest sleep | `node.rs:1553-1555` | 3.072 s | How long a node's turn sleeps with nothing due | "Every wait has a bound" (`node.rs:1553`); the value is row 6's |
| 13 | The vouch beat | `node.rs:1538-1539`; `host.rs:2957-2961` | 1.024 s | How often a held chunk or job is said to be at work | Derived: row 5, "the transport's notes' beat of a third of the serve period" (`host.rs:2957-2959`) |
| 14 | `Status::reconnect` | `node.rs:1205-1210` | Row 6 plus the peer's backoff | How long a link may be silent, by the node's own law | Derived from rows 6 and 9 |
| 15 | The leaderless budget | `node.rs:3620-3628` | Row 6 times the nodes | How long a step waits without a leader where nothing judges | Derived from row 6 |
| 16 | Name lookups | `names.rs:64, 72, 137, 148` | Row 5, doubled to row 6 | When an unresolved peer name is looked up again | Derived from rows 5 and 9 |
| 17 | The status saying | `crates/node/src/process.rs:1890-1899` | Row 7 | How often a node says its transfers moved | Derived from row 7 |
| 18 | `ELECTION_TICK`, `HEARTBEAT_TICK` | `crates/node/src/derive.rs:38-41` | 10 and 2 ticks | A range's Raft tick settings, kept though ranges elect by suspicion | Cited: focal's (node.md §2.4). They time nothing |
| 19 | `PERSISTENT_CONGESTION_THRESHOLD` | `crates/node/src/client.rs:146` | 3 PTOs | The least period a measured exchange gets | Cited [2 §7.6.1] |
| 20 | The node client's patience | `crates/node/src/client.rs:540-553` | At least row 6 | The longest it says it waits without asking | Derived from row 6 |
| 21 | `ATTEMPTS` | `client/src/lib.rs:75-77` | 3 | Asks of a request refused before any node took it | Cited: the AWS SDKs' standard retry mode (research/73) |
| 22 | `QUOTA`, `SLOWED_RETRY`, `UNRETRIED_REFUND` | `client/src/lib.rs:79-87` | 500, 5, 1 | The retry token bucket | Cited: botocore's `quota.py` (research/77 [5], D4) |
| 23 | `SLOWED_BASE`, `SLOWED_MOST` | `client/src/lib.rs:89-93` | 1 s, 20 s | A slowed request's full-jitter backoff | Cited: botocore's standard mode (research/73) |
| 24 | `PERSISTENT_CONGESTION_THRESHOLD` | `client/src/lib.rs:100-102` | 3 PTOs | The least period a measured exchange gets | Cited [2 §7.6.1] |
| 25 | The period's floor `served` | `client/src/lib.rs:1782` | Row 6 | The least period an exchange gets once measured | None beyond row 6 |
| 26 | `Limits::validate` | `vendor/hyper-transport/src/endpoint.rs:190-191` | Refuses 0 | That every endpoint has an idle timeout | None; [1 §18.2] lets both ends advertise 0 |
| 27 | `NOTES_A_PERIOD` | `endpoint.rs:344-347`; the beat `endpoint.rs:4116-4122` | 3, a beat of 1.024 s | The most notes a connection sends a period | Derived from [1 §10.1]'s three PTOs (research/77 D1, reviewed); the path's loss is not measured |
| 28 | The least period | `endpoint.rs:1480-1486` | More than the peer's `max_ack_delay` | Refuses a period a live peer could fail | Cited [1 §13.2.1] |
| 29 | `probes_within` | `vendor/hyper-transport/src/charge.rs:97-113`; `endpoint.rs:2053, 2065, 2089` | Two a doubling from 1 ms to row 6 | The loss probes a peer may send unread: the room a socket keeps for them | Derived from [2 §6.2.1, §6.2.4] and row 6 |
| 30 | `K_GRANULARITY` | `vendor/hyper-transport/src/credit.rs:69` | 1 ms | Row 29's first doubling | Cited [2 §6.1.2] |
| 31 | The refused records' lifetime | `endpoint.rs:705-724`, from `vendor/hyper-quic/src/config/mod.rs:359` | 15 s | How long a refused attempt's copy is ignored | None: quinn's default `retry_token_lifetime` |
| 32 | `MAX_DOUBLINGS` | `vendor/hyper-transport/src/timing.rs:8-11` | 63 | How far abandoned exchanges double a peer's expected time | Derived: the arithmetic's limit (focal's cap of six excluded) |
| 33 | `max_idle_timeout` default | `vendor/hyper-quic/src/config/transport.rs:410` | 30 s | quinn's default, which mantle replaces with row 6 | None: upstream's |
| 34 | `initial_rtt` | `config/transport.rs:418` | 333 ms | The round trip before a sample | Cited [2 §6.2.2] |
| 35 | `TIMER_GRANULARITY` | `vendor/hyper-quic/src/lib.rs:404` | 1 ms | The PTO's least variance term | Cited [2 §6.1.2]; "system-dependent" [2 App. A.2] |
| 36 | The idle timer | `connection/mod.rs:3127-3142`; negotiation `mod.rs:6012-6019, 5210-5211`; advertised `transport_parameters.rs:162` | The larger of row 6 and 3 current PTOs | Kills a silent connection `TimedOut` with nothing sent (`mod.rs:1825-1826`) | Cited [1 §10.1, §18.2], on the current PTO as the RFC says |
| 37 | `persistent_congestion_threshold` | `config/transport.rs:427` | 3 | Persistent congestion | Cited [2 §7.6.1] |
| 38 | `MAX_BACKOFF_EXPONENT` | `connection/mod.rs:5847-5849` | 16 | The most a PTO doubles: probes at most 2^16 PTOs apart | None: upstream's, "prevents overflow" |
| 39 | `PATH_DEGRADING_PROBE_TIMEOUTS` | `connection/mod.rs:5840-5845` | 4 PTOs | When `Event::Degrading` is told | Cited: quiche's default. mantle ignores the event (`node.rs:1943-1945`) |
| 40 | The held Initial | `vendor/hyper-quic/src/endpoint.rs:897-905` | 3 initial PTOs, 2.997 s | How long a server holds an Initial it cannot yet place | Derived from [2 §6.2.1] at row 34 |
| 41 | `HANDSHAKE_COPY_BURST` | `config/transport.rs:12-15` | 35 ms | How far a handshake copy trails its original | Cited: Jiang and Schulzrinne's trace (hyper-raft `docs/research/burst-loss.md` §2) |
| 42 | Closing and draining | [1 §10.2]; awaited by a node's stop (`docs/design/node.md:767-769`) | 3 PTOs | How long a closed connection lingers | Cited [1 §10.2] |
| 43 | `UNANSWERED_NS` | `vendor/hyper-liveness/src/pair.rs:19-23` | 3 s | The most a stream to a peer never heard from backs off to | Cited [6 §3.1.3] |
| 44 | `PHI_PER_MILLION` | `vendor/hyper-timing/src/link.rs:90-92` | 15 ppm | Clock drift in the detection bound and the window's cap | Cited: RFC 5905 §7.2, as hyper-timing reads it |
| 45 | RFC 9002's gains | `vendor/hyper-timing/src/lib.rs:72-77` | 1/8, 1/4, 4 | `ExchangeRtt`, under the exchange tails of `vendor/hyper-transport/src/timing.rs` | Cited [2 §5.3, §6.2.1]; Jacobson's estimator [4 App. A] |
| 46 | `INITIAL_RTO_NS` | `vendor/hyper-swim/src/detector.rs:262-266` | 1 s | A SWIM pair's timer before a round trip | Cited [5 (2.1)]. mantle links no part of hyper-swim (rows 46 to 49) |
| 47 | `MINIMUM_RTO_NS` | `detector.rs:268-277` | 1 s | That timer's floor until the pool has measured the host's stalls | Cited [5 (2.4)] |
| 48 | `MAXIMUM_RTO_NS` | `detector.rs:279-281` | 60 s | That timer's cap when backed off | Cited [5 (2.5)] |
| 49 | `RTO_ROUND_TRIPS` and the gains | `detector.rs:209-212, 283-290` | 3; 1/8, 1/4, 4 | A path far from the pool's; the timer's estimator | Cited [5 (2.2), (2.3)] |

Already learned, and so not in the table: hyper-liveness's interval `η` and margin `α` for each node
pair, configured from the pair's measured latenesses, the owner's timer lateness `G`, the mean
flush, the fleet's failure history and the measured election cost [24 §2.2, §2.8]; the clock's
resolution, from `clock_getres` (`node.rs:947`; `vendor/hyper-rt/src/machine/clock.rs:30-45`); the
election's spread, from the voters' measured paths [24 §2.3]; a redial's first pause, the probe
timeout its last connection's handshake measured (`node.rs:1991-1997`); and a client's slowest
measured exchange (`client/src/lib.rs:1776-1800`).

Rows 7 to 9, 11, 12, 14 to 17, 20, 25, 27 and 29 hang off row 6. Rows 1 to 4 are QUIC's rules for
retransmitting before anything is known, and the RFC that gives them says liveness timers are
another matter:
"determining an interval between keep-alive messages ... or determining if a remote endpoint has
responded to a request to perform an action, typically operate over longer timescales than
congestion control and therefore are not covered in this section" [6 §3.1.1].

## What the sources establish

**A timeout fixed in advance is the poorest detector, and QUIC's idle timer is one.** Chen, Toueg
and Aguilera analyse "a simple failure detection algorithm, commonly used in practice": on each
heartbeat the receiver "starts a timer with a fixed timeout value TO; if the timer expires before q
receives a newer heartbeat message from p, then q starts suspecting p" [11 §1.2.1]. It has two
faults: "the probability of a premature timeout on m_i also depends on the heartbeat m_{i-1} that
precedes m_i", and "the worst-case detection time for this algorithm is the maximum message delay
plus TO", which is "impractical because, in many systems, the maximum message delay is orders of
magnitude larger than the average" [11 §1.2.1]. QUIC's idle timer is that algorithm, restarted at
each packet received (`connection/mod.rs:3087-3088, 3127-3142`), with TO the negotiated value.
Jacobson showed the same of a timeout fixed as a multiple of the mean: TCP's suggested `β = 2`
"can adapt to loads of at most 30%", because "R and the variation in R increase quickly with load"
and "scale like (1 − ρ)^−1"; estimating the variation instead "essentially eliminates spurious
retransmissions" [4 §2]. Even his own multiplier came from data: the revised paper moved the
timeout from R + 2V to R + 4V after "spurious retransmissions during connection startup" were
observed on slow links [4 note 21]. Falcon's authors put it flatly: "there may not be a perfect
timeout value", and "while adaptive timeouts ... might seem promising, adaptation requires time;
thus, if system responsiveness changes rapidly (e.g., from bursty load), one does not obtain" a
reliable detector [18 §2.2]. SWIM's suspicion timeout is "prespecified" and "effectively trades off
an increase in failure detection time for a reduction in frequency of false failure detections"
[16 §4.2].

**The RFCs fix no idle timeout; they fix a floor and leave the value, and whether to have one, to
the application.** The idle timeout applies only "if a max_idle_timeout is specified by either
endpoint", and the effective value is "the minimum of the two advertised values (or the sole
advertised value, if only one endpoint advertises a non-zero value)" [1 §10.1]; "idle timeout is
disabled when both endpoints omit this transport parameter or specify a value of 0" [1 §18.2]. The
three PTOs are a floor relative to "the current Probe Timeout", there so that "multiple probes [can]
be sent and lost, prior to idle timeout" [1 §10.1], and "the total length of time over which
consecutive PTOs expire is limited by the idle timeout" [2 §6.2.1]. RFC 9000 asks applications to
decide: "Application protocols that use QUIC SHOULD provide guidance on when deferring an idle
timeout is appropriate" [1 §10.1.2]. RFC 9308 says the same: "an application therefore should be
able to configure its own maximum value", and "applications that provide their own keep-alive
mechanisms will therefore keep a QUIC connection alive" [3 §10]; and it warns that "timeouts
shorter than 30 seconds can make it harder to handle transient network interruptions, such as
Virtual Machine (VM) migration" [3 §3.2], which is a host holding a process still. Transport
parameters cannot follow a learned value: they are "authenticated declarations" made "during
connection establishment" [1 §7.4], so a value set per peer is fixed at the handshake and is
anyway the lesser of the two ends' [1 §10.1].

Two other rules matter once a connection may stay silent. A peer that lost a connection's state, a
restart or a crash, can say so at once: "A crash or outage might result in peers continuing to send
data to an endpoint that is unable to properly continue the connection", and a stateless reset makes
"the peer ... immediately end the connection" [1 §10.3]. A live endpoint acknowledges within its
`max_ack_delay`, "an explicit contract: an endpoint promises to never intentionally delay
acknowledgments", and "if it does, any excess accrues to the RTT estimate" [1 §13.2.1]. RFC 9002
folds a peer's scheduling stalls into the path: delays beyond `max_ack_delay` are "attributed to
unintentional but potentially repeating delays, such as scheduler latency at the peer", and "are
considered effectively part of path delay" [2 §5.3].

**Keep-alives are for middlebox state, and the sources bound their cadence from below.** "Though
REQ-5 in [RFC4787] recommends a 2-minute timeout interval, experience shows that sending packets
every 30 seconds is necessary to prevent the majority of middleboxes from losing state for UDP
flows" [1 §10.1.2]; RFC 4787 requires that "a NAT UDP mapping timer MUST NOT expire in less than
two minutes" and recommends "a default value of five minutes or more" [7 §4.3]. RFC 9308 adds that
"larger values are preferable if the deployment can consistently survive NAT rebinding or is known
to be in a controlled environment (e.g., data centers)", that "sending PING frames more frequently
than every 30 seconds over long idle periods may result in excessive unproductive traffic", and
that "applications should use keep-alives only in circumstances where continued communication is
highly likely", HTTP/3 "using keep-alives only when a request is outstanding" [3 §3.2]. RFC 8085:
keep-alives "are not recommended for general use", "SHOULD NOT [be transmitted] more frequently
than once every 15 seconds", should be jittered, and in "a controlled environment, the deployer
SHOULD investigate whether the target environment allows applications to use longer intervals" [6
§3.5]. A binding's lifetime can be probed by idling and testing [8 §4.6], with the warning that "for
many NAT devices, an absolute refresh interval cannot be determined; bindings might be closed more
quickly under heavy load" [8 §4.6]. On a Linux host the kernel's own flow table states its timeouts,
`nf_conntrack_udp_timeout` "default 30" and `nf_conntrack_udp_timeout_stream` "default 120" [9].
QUIC survives a rebinding by its connection IDs, but "this only helps if one endpoint maintains
availability at the address its peer uses and the peer is the one to send after the timeout occurs"
[3 §3.2], which is a node and its client.

**Retransmission timers are learned, and their fixed parts are priors and floors.** RFC 6298
estimates the timeout from samples, `SRTT + max(G, 4·RTTVAR)` with gains 1/8 and 1/4 [5 (2.2),
(2.3)], takes samples only from unambiguous exchanges (Karn's rule) [5 §3], doubles on each expiry
[5 (5.5)] and collapses on the next sample [5 §5]. Its fixed parts are labelled as such: 1 s before
any sample [5 (2.1)]; a 1 s floor "as a conservative approach, while at the same time acknowledging
that at some future point, research may show that a smaller minimum RTO is acceptable" [5 (2.4)];
"no requirement for the clock granularity G" [5 §4]; and any cap "at least 60 seconds" [5 (2.5)].
RFC 8085 asks a UDP application without a sample to be "as conservative (large) as possible" [6
§3.1.1], and one whose destination "does not send return traffic" to send "not more than one UDP
datagram every 3 seconds" [6 §3.1.3].

**Failure detectors are configured from measurement, and every record misses the next longest
stall.** Chen, Toueg and Aguilera replace the restarted timer by freshness points shifted from the
heartbeats' schedule, so that "the detection time is bounded and the bound does not depend on the
behavior of message delays and losses", `T_D ≤ δ + η` [11 Thm. 5]; configure `η` and `δ` from the
loss and the delay's mean and variance alone through the one-sided inequality [11 §5.1, Thm. 9];
estimate the expected arrival from the `n` most recent heartbeats where clocks are not synchronized
(NFD-E) [11 §6.3]; and adapt by "periodically reexecuting the configuration" [11 §8.1.1]. They note
where the model ends: bursts longer than the sending period break the heartbeats' independence, and
then one needs "a short-term component" and "a long-term component", taking "the most conservative
one" [11 §8.1.2]. Bertier, Marin and Sens combine Chen's estimate "with a dynamic estimation based
on Jacobson's estimation of the round-trip time", a detector with "no tuning parameter" that
"provides a shorter detection time, but generates more wrong suspicions" and proved "more sensitive
... (1) to message losses, and (2) to large fluctuations in the receiving time of heartbeats" (as
described in [13 §II-D, §V-E]; [12] could not be read). The φ accrual detector outputs
`φ = −log10(P_later(t_now − T_last))` from a normal fit to a fixed-size window of inter-arrivals
and leaves the threshold to the application, `Φ = 1` meaning a mistake "about 10 %" of the time,
`Φ = 2` about 1 % [13 §IV-A, §IV-B]; its mistake rate "improves as the window size increases"
[13 §V-D]. Satzger and others drop the normal fit: their suspicion "is computed using the empirical
distribution function applied to a list of stored heartbeat inter-arrival times" (as described in
[15]; [14] could not be read). SWIM sets its probe timeout from "an estimate of the distribution of
round-trip time within the network, e.g. an average or 99th percentile", with a protocol period
"at least three times the round-trip estimate" [16 §3.1].

hyper-raft's detector, which mantle runs, takes this line to its end [24 §2.2]: each heartbeat taken
ends one gap a freshness point judged, its lateness bounded by Cantelli's inequality over the whole
record's mean and variance, plus Rényi's record law, "the chance that the next is later than all `m`
before it is exactly `1/(m + 1)` whatever their distribution"; the interval and margin minimize the
time a group cannot commit, and a configuration whose bound on that time is one or more is refused.
Its traces
show why the record must be the whole history: "over consecutive blocks of `m` heartbeats, the
median block's deviation is 1–18 % of the run's on every run with a tail" (measured: hyper-raft's
heartbeat traces, [24 §2.6 item 3]). The record law is also the reason no learned timeout can be
safe for a link: a hold longer than every one the record holds comes with chance `1/(m + 1)`
whatever the holds' distribution, and a hold has no upper bound (a SIGSTOP lasts until a SIGCONT),
so a link closed at any finite learned bound is sometimes closed under a live peer. The rate can be
priced; it cannot be made zero.

**Nobody can tell a held process from a dead one by its messages; the held process can.** "It is
impossible to reliably distinguish a process that has crashed from one that is merely very slow"
[10 §9.1]. What can be had is eventual accuracy: a detector that "increases the length of its
timeout period" after each premature suspicion implements an eventually perfect detector in a
partially synchronous system, measuring time "by counting the number of steps that it takes" [10
§9.1, Fig. 10, Thm. 9.1], and in practice it suffices that accuracy holds "for periods that are
'long enough'" [10 §1].

Lifeguard found that "slow processing by the failure detector module itself is the primary cause of
the false positives that SWIM's Suspicion mechanism fails to suppress", since "an episode of slow
message processing at a given group member is likely to impact multiple of its interactions" [17
§IV]. It was evaluated by "pausing the sending and receiving of protocol messages at selected group
members for well defined periods of time" [17 §V-D] and cut false positives "in the range of 10x to
100x, and over 50x on average" (measured: [17 §VII]), with a saturating counter, a base probe
interval of 1 s and timeout of 500 ms, `S = 8` [17 §IV-A], "parameters that currently use
heuristically determined values" [17 §VII].

Falcon looks inside the layers instead: "if a process disappears from the OS's process table, it is
dead", and local timeouts "are better informed and shorter than end-to-end timeouts, as they
reflect local, more predictable behavior" [18 §2.3]. Its application spy "measures T_app-resp by the
CPU time consumed by the monitored application, not real time", which "avoids the case that the
enforcer declares an unresponsive application down when in fact the application is temporarily slow
because of load", and it "does not undermine any higher-level (human or application) deadlines since
those are expressed and enforced by Falcon's end-to-end timeout" [18 §4]. A process stopped by a
signal accrues no CPU time, so the spy never condemns it; the end-to-end timeout decides. To be
certain, Falcon kills what it suspects and confirms the kill before it reports a crash [18 §3.4,
§4]. Pigeon renounces killing ("Pigeon shall not kill") and exposes the uncertainty instead [19
§2.2]: a *stop*, where "the target process has stopped executing and lost its volatile state", is
certain and permanent; an *unreachability*, where "the target process may be operational, but the
client cannot reach it", is "potentially intermittent" and "abstracts a timeout due to, say, a
network partition or a slow process" [19 §3.1]. Each condition carries an expected duration, and
"each application has an unavailability threshold such that if the expected duration of the
condition is smaller, the application should wait; otherwise, it should start recovery" [19 §3.3].
Pigeon's own durations were measured on its testbed (2.8 s for a new path, 66 s for a router's
reboot; measured: [19 §4.4]). Albatross, by the same authors, enforces its verdicts in the network
and "categorizes processes as excluded or non-excluded and promises definitive answers only to
non-excluded processes" (as quoted in [20]).

The held process has facts no peer has. Its own wake comes late by the hold, which hyper-raft counts:
"a stop of the process or a frozen host inside a wait does count" in the timer lateness `G` [24
§2.4]. The kernel stamps each datagram as it arrives, "the reception time of the last packet passed
to the user", on Linux on `CLOCK_REALTIME` [21 socket(7)], carried to the shard's clock by
`vendor/hyper-rt/src/udp/carry.rs:1-29`; on macOS `SO_TIMESTAMP_MONOTONIC`
(`vendor/hyper-rt/src/udp/macos.rs:1-18`); on Windows there is none, "Windows stamps at the read"
[24, merge `e6b3372`]. With stamps, "a heartbeat that arrived before its freshness point is fresh
however late the process read it" [24 §2.4]; the gap the stamp removes was measured at p99.9 of 3.8
to 4.7 ms and up to 101 ms on macOS at load 17 to 32, 73 ms and up to 125 ms at load 41, and 0.09 to
0.15 ms and up to 22 ms on Linux (measured: hyper-raft's heartbeat traces, [24 §2.4]). The kernel
counts what it dropped while the process did not read, `SO_RXQ_OVFL`, "the number of packets
dropped by the socket since its creation" [21 socket(7)]. It names a stopped process, state `T`,
"Stopped (on a signal)" [21 proc_pid_stat(5)]; times a thread "spent waiting on a runqueue" [22
sched-stats]; and reports "the share of time in which at least some tasks are stalled" on memory or
I/O [22 psi]. Every clock mantle's timers run on counts a hold, and a suspend as well
(`vendor/hyper-rt/src/machine/clock.rs:1-5`): QUIC's timers run on the shard's clock
(`driver.rs:380-383`), not on std's `Instant`, for which "it is also not specified whether system
suspends count as elapsed time or not" [23]. So a peer sees one silence, but the held node can tell
afterwards that it was held, for how long, and often why.

## The design

Two clocks have two owners. A caller's deadline belongs to the caller and runs on the wall clock:
the caller was held too, and its patience is spent whether or not it ran. A judgment of a peer
belongs to the learned detector and counts only the time the judge was running, as Chandra and
Toueg's detector counts its own steps [10 §9.1] and Falcon counts the monitored process's CPU time
[18 §4]. The transport carries data and keeps neither. Each rule below follows from that. None is
built. Most of it changes vendored crates, which are hyper-raft's; the rest is mantle's `bounds.rs`,
node and client.

**L1. No idle timeout on any link.** Every node and client advertises `max_idle_timeout` 0, so the
negotiated timeout is none [1 §18.2], and no connection ends because it went quiet. A link ends on
evidence that its peer's run ended, or by a stated bound:

- the peer's close [1 §10.2];
- a stateless reset, the peer's word that it lost the connection's state [1 §10.3]. This needs
  hyper-transport's restartable endpoint (`vendor/hyper-transport/src/restart.rs:13-24`,
  `endpoint.rs:876-889`), which mantle does not use (no `RestartKey` or `new_restartable` in
  `crates/`): "an endpoint keyed afresh at each start drops such a packet, its connection ID one the
  new key does not know" (`restart.rs:18-20`), so today a restart is told only by the idle timeout;
- a newer authenticated connection of the same identity, which replaces an idle older one as
  admission does (`vendor/hyper-transport/src/admission.rs:9-19`);
- the liveness stream's word that the run ended, `Change::Restarted` or `Change::Stopped`
  (`vendor/hyper-liveness/src/lib.rs:253-270`);
- the cell's membership removing the peer;
- the transport's budget at its bound, which evicts the connection longest without a sign of life
  first: an eviction by a stated rule (CLAUDE.md §2), not a time. A link to a dead peer costs at
  most its stated connections' bytes (`bounds.rs:32-35, 340-381`), and the peers are bounded by
  what the node states (`bounds.rs:124-128`);
- the owner's own close: a stop, or a client done with a node, or one that waited a whole patience
  for a node that gave no sign of life and looks its name up again (`aa5cbf62`'s rule).

A dial keeps a bound of its own. hyper-quic bounds a handshake by the idle timer
(`connection/mod.rs:409-412`), so it needs a handshake bound apart from it: the dial ends
`Unreachable` once its handshake has gone unanswered for RFC 9002's persistent-congestion duration
at the path's estimate, three probe timeouts [2 §7.6.1], and L6's backoff takes over. Ending a dial
early loses nothing; ending a connection does. The alternative of an idle timeout set per peer from
the detector's bound is rejected: it is fixed at the handshake [1 §7.4] and bounded by the other
end's [1 §10.1], it is Chen's restarted timer again [11 §1.2.1], and it would close the link, with
the plane's keys (`node.rs:2028-2041`), at the moment the detector only suspects.

**L2. Suspicion moves work and closes nothing.** A suspicion (`Change::Suspected`) still starts
elections (hyper-raft L-2) and still ends a peer's chunk reads at once so they go to other copies
(`node.rs:1777-1781`). It no longer costs the link: the connection stays and the plane keeps its
keys, so the detector takes the held peer's first heartbeat after its resume with no handshake. A
suspicion also gathers the evidence L1 needs. While the peer is suspected, the node tests its link
with a PING, RFC 9000's "liveness testing" [1 §10.1.1], at the pauses of L6's backoff: an
acknowledgement says the peer runs again, a stateless reset says its run ended, and nothing says it
is still silent. It also looks a named peer's name up again, since a pod made again comes back at
another address (research/78). A trust after a suspicion sends a PING too, so the PTO backoff
resets at the next acknowledgement [2 §6.2.1] rather than at the next backed-off probe.

**L3. A judge keeps its own holds out of its judgments.** Five rules, each from a fact the judge
measures:

- *Read before timers.* The driver takes readiness and drains every socket before it fires any
  timer, and fires timers only at a clock read before a drain that left no socket holding
  datagrams: the rule the node already follows for its plane (`node.rs:1408-1426`) and hyper-raft
  for its members (`d712a5f`). Today it drains only sockets readied in an earlier turn, fires due
  timers (`vendor/hyper-rt-transport/src/driver.rs:558-567`) and only then takes readiness
  (`driver.rs:580`). Necessary, not sufficient: alone it changed no probe's outcome (measured: [25]).
- *Kernel stamps for QUIC.* The driver hands QUIC each datagram's read time, not its stamp ("the
  endpoint is told when each datagram was read ... the kernel's stamp only says what was queued
  when the drain began", `driver.rs:1017-1026`), so a receiver's own hold enters its round-trip
  samples and its progress judgments. RFC 9002 is right to put a *peer's* scheduling stall in the
  path [2 §5.3]; the receiver's own is not the path's. Stamps go to the RTT sample and to the
  progress judgment where the platform gives one; on Windows the other rules carry it.
- *Periods on running time.* A period that ran out while the judge's own wait was late past it is
  given again from the judge's resume. The judge knows the lateness exactly: the wake it asked for
  against when it woke, which the node already reads for its detector (`node.rs:1584-1590`).
  hyper-liveness does the same for its own flush stalls: "a wait at least as long as the margin,
  which ended after the peer was expected, gives the peer the margin again from its end" [24 §2.8,
  "A judge whose own proof stalls"]. A caller's deadline is not such a judgment and keeps the wall
  clock (above).
- *A socket that overflowed.* Datagrams the kernel dropped while the judge did not read are its own
  fault, and the kernel counts them [21 socket(7)]; hyper-rt already reads the count
  (`driver.rs:1028-1029`). A judge that finds drops after a late wake should judge no peer by a
  freshness point that passed inside its hold until it has read a datagram that arrived after its
  resume. Whether hyper-liveness needs this, or its stamps already suffice, is measured first (T4).
- *G.* Holds inside waits count in the timer lateness that floors every interval and margin [24
  §2.4], so a host that holds its processes often widens its nodes' margins: Lifeguard's direction
  [17 §IV], measured where Lifeguard counts.

**L4. What D2's deadline means on a link the detector trusts.** The owner chose that a node waits
"as long as the client would": each request carries its time left, 60 s when it says nothing
(research/77 D2; row 10). On a link the detector trusts, the peer's heartbeats are fresh, each the
proof of a recent durable flush (`vendor/hyper-liveness/src/lib.rs:15-27`): its process runs. An
exchange to it that moves nothing is then the peer's work, which the notes vouch for while it moves
or is held (research/77 D1, D2). So the asker waits until the carried deadline, and cuts sooner only
an exchange whose server is heard yet has neither moved nor vouched for it for the notes' bound:
a live server that is not at work on it. Silence never cuts an exchange; silence is the detector's.
On a link the detector suspects, an exchange that has an alternative moves at the suspicion (a chunk
read, `node.rs:1777-1781`, or a read the range can serve elsewhere once it elects); one that has
none waits for its deadline, the peer's typed refusal, or the end of the peer's run.

A client runs no detector, but its node's acknowledgements are one: a running endpoint acknowledges
within its `max_ack_delay` by contract [1 §13.2.1], so a client with data or a PING outstanding that
hears acknowledgements knows the node's process runs, and treats the exchange as above. Total
silence is Pigeon's unreachability [19 §3.1]: the node may be held, cut off or dead. The client then
waits until its deadline, or the stock patience if it gave none, and asks another node as well for
a request that is safe to ask twice (a get; a put with its identity joins its work, research/77 D4),
once the silence passes that node's measured response quantile: the hedging rule node.md §5.4 sets
for chunk reads, Dean and Barroso's 95th percentile until the headroom is measured, and research/77
D3, "latency decides hedging, never liveness". The first answer is taken and the other cancelled.
The idle-timeout floors of rows 20 and 25 go. The only time bounds left on a client's exchange are
its caller's deadline, the stock patience, and a hedge point from its own record.

The serving side follows the same rule for an asker it waits on, a client held mid-request: it waits
the asker's carried time left, or the stock patience, as the S3 listener already waits for an octet
(`listener/conn.rs:44-47`). Row 7 then has two parts with two bases: the serve-side wait (the asker's
time) and admission's quiet period, which is open (question 6).

**L5. Keep-alives only while a request is outstanding, at the path's middlebox lifetime.** A
connection with nothing outstanding sends no keep-alive [3 §3.2]: it has no idle timer to defer, and
a client that sends after a quiet spell rebinds through a NAT on its own [3 §3.2]. A connection with
an exchange outstanding and nothing else flowing sends PINGs at the path's measured middlebox
lifetime, jittered [6 §3.5]: on a Linux host whose flow table is loaded, its stated timeout read
from the kernel [9]; on a path measured by idling and testing [8 §4.6]; before either, the 30 s the
QUIC RFCs give for paths through a NAT [1 §10.1.2] [3 §3.2]; and never more often than every 15 s
[6 §3.5]. Within a cell the liveness stream and Raft's lanes keep their own flows busy. A sign that
a middlebox's state expired is itself a measurement: the plane still hears the peer while the QUIC
link's first packet after a quiet gap goes unanswered, which bounds that flow's lifetime from above.
Row 8 goes.

**L6. Reconnects: the measured probe timeout, doubled, capped by RFC 8085's rate, and redials on
evidence.** Every backoff of row 5 keeps RFC 6298's doubling from the probe timeout last measured to
the peer [5 (5.5)] and focal's equal jitter (`bounds.rs:95-102`). Its cap stops being row 6.
RFC 6298's rule for a cap, "at least 60 seconds" [5 (2.5)], is for TCP's retransmission timer. A
link test and a redial are UDP sends toward a destination that answers nothing, which RFC 8085
bounds by rate instead: such a sender "SHOULD NOT send more than one UDP datagram every 3 seconds"
[6 §3.1.3]. So the backoff doubles until its sends meet that rate and stays there: for a link test,
one PING, a pause of 3 s; for a redial, 3 s times the datagrams one unanswered dial sends. The
count is the dialer's own (hyper-quic counts what a connection sent); before one is counted it
follows from RFC 9002's schedule, an Initial and up to two datagrams at each probe timeout within
the dial's bound [2 §6.2.4]. No number is picked; question 4 asks whether the slower rate RFC 6298's
cap implies is preferred. With L1 a held peer needs no redial at all: its link is tested, and a
restarted peer answers the first test after it listens with a stateless reset, which ends the link
at once and starts the redial (L2). Other redials come at once rather than at a pause: a name that
resolves to another address (`names.rs`, research/78), a newer connection. A pause for want of a
leader is bounded by the patience the members stated, their expected election
(`node.rs:3620-3628`; `crates/node/src/client.rs:296-305`), rather than by row 6.

**L7. The first bound, before anything is measured.** Each quantity has a source that is not a
guess:

- a dial's first probe timeout: `kInitialRtt` [2 §6.2.2], the prior from RFC 6298's survey [5 App.
  A], or, for a peer reached before, the last connection's final smoothed RTT, which RFC 9002
  permits [2 §6.2.2]. hyper-quic already keeps a previous connection's measurements per address for
  Careful Resume (RFC 9959 §2.4, `vendor/hyper-quic/src/config/mod.rs:201-226`);
- after the handshake, the handshake's own round trip, `SRTT = R` and `RTTVAR = R/2` [2 §5.3], the
  first sample, which sets the probe timeout, the redial's first pause (`node.rs:1991-1997`) and the
  next dial's bound. It does not set a liveness bound: a handshake on a quiet host has not seen the
  host's stalls, whose variance a short record underestimates (measured: [24 §2.6 item 3]);
- the detector's first margin: the node's own evidence, its other links' latenesses, and with none
  its own stream's latenesses, how late its own host made its own sends, standing in for its peers'
  [24 §2.8, "Judged before its own evidence" and "Judged with no evidence of its own"];
- a client's first exchange: its caller's deadline, or the stock patience; "an unmeasured round gets
  the whole ceiling" (`vendor/hyper-timing/ORIGIN.md` change 6). No hedge until a response time is
  measured;
- a keep-alive's cadence: the host's stated flow timeout [9], else 30 s [1 §10.1.2] [3 §3.2], as L5.

**L8. What stays fixed, and why it may.** RFC 9002's and RFC 6298's gains are part of the
estimator's definition (rows 45, 49); RFC 6298 takes them from Jacobson and Karels [5 (2.3)], whose
multiplier of four replaced two once spurious retransmissions were observed [4 note 21].
`kPersistentCongestionThreshold` (rows 19, 24, 37) bounds a dial and a congestion response, not a
link. `kInitialRtt` (rows 1, 34) is used only for the first packet to an unknown peer. The botocore
constants (rows 21 to 23) and the stock patience (row 10) are what stock clients do, which mantle
matches by the owner's choice. The 3 s of RFC 8085 (row 43), the clock drift of RFC 5905 (row 44)
and the handshake copies' spacing (row 41) are cited. Rows 2, 3, 30 and 35 should become measured:
`kGranularity` is "system-dependent" [2 App. A.2], RFC 6298 asks nothing of `G` [5 §4], and
hyper-timing measures it already; the advertised `max_ack_delay` "SHOULD include the receiver's
expected delays in alarms firing" [1 §18.2], which is the endpoint's measured timer lateness. Row 38
is an overflow guard; with no idle timeout it also sets how far apart QUIC's own probes to a dead
peer grow, 2^16 probe timeouts, about 28 minutes at loopback's 26 ms (derived), beside L6's link
tests. Row 27 keeps its derivation from [1 §10.1] until the path's loss is measured. Rows 6, 8, 9,
20, 25 and 26 go. Rows 7, 11, 14, 15 and 16 take the bases L1, L4 and L6
give them. Rows 12, 13, 17, 27 and 29 are derived from rows 5 to 7 and need bases of their own once
those change: the turn's longest sleep can be the earliest deadline the node holds; a held vouch's
beat and a status line's cadence follow the notes' beat, which is open (question 6); row 29's horizon
becomes a dial's bound for a handshake and the asker's patience for an established connection.
Row 31 is quinn's default for a token's life, outside this note's path.

## A peer held 3 s, 30 s, and for good

What each part does under the design, for a node held still by its host while its peers and a
client run. Today any hold past 3.072 s fails, as the 4 s and 5 s probes did, and a 3 s hold sits
just inside that bound (measured: [25]).

| | Held 3 s | Held 30 s | Held for good |
|---|---|---|---|
| Its peers' detectors | Suspect it at the freshness point, within the bound each suspicion states [24 §2.8]; trust it again at its first heartbeat after resume | The same | Suspect it, and never trust it again |
| The ranges it leads | Elect another; it learns the higher term on resume | The same; it catches up by appends or a snapshot | Elect another; the cell's membership replaces it |
| Its QUIC links | Kept (L1); its peers' probes and link tests are read on resume before its timers fire (L3) | Kept; whatever its sockets dropped, QUIC sends again | Kept until the membership removes it or a budget evicts it; tested once every 3 s at most (L6), QUIC's own probes at most every 2^16 PTOs (row 38) |
| Plane keys and heartbeats | Kept; its heartbeats resume at once | Kept | Kept; its peers' streams still send it one each interval, since a stream "does not back off" from a suspected peer [24 §2.8] |
| A client's read through it, no deadline | Waits up to the stock patience and is answered after resume; asked of another node too once the silence passes the hedge point (L4) | The same; 30 s is within 60 s | Ends at 60 s, the caller's; the client closes the link and looks the name up again |
| A client's read with deadline `d` | Answered if `d` covers the hold and the work | Answered if `d` covers 30 s, else ends at `d` | Ends at `d` |
| Its own asks of peers | On resume, replies and notes are read before its timers fire; periods that ran out inside its hold are given again (L3) | The same | None |
| Its own judgment on resume | Peers fresh by their stamps; no suspicion | Where its sockets overflowed, no judgment until fresh arrivals (L3) | None |
| What it costs | Nothing beyond today's | Its sockets' queues stay full for the hold | At most its stated connections' bytes at each peer, until removed |

Without stamps (Windows) the 3 s and 30 s columns rest on read-before-timers and periods on running
time alone; how well they hold there is measured, not assumed (T3, M4).

## What to measure and test before any code changes

Each check runs real processes on real sockets, waits on facts, and asserts exactly. A hold is the
experiment's variable and ends on a fact, never on a sleep. A stop is a fact once the system says
so: `ps -o stat=` starts with `T` [21 proc_pid_stat(5)], as hyper-liveness's process tests already
check (`vendor/hyper-liveness/tests/processes.rs:2246-2275`).

- **M1. The holds the hosts impose.** Each node records its own wake lateness (`node.rs:1584-1590`)
  with PSI's memory and I/O shares [22 psi] and its threads' run-queue waits [22 sched-stats] around
  each, on the box under the trace's loads (zram swap, four and eight busy loops on two CPUs, a
  cgroup's `memory.high`), on this Mac under load, and in the KIND cell. Out comes the distribution
  of holds and their kind: stopped (no CPU, no run-queue wait), starved (run-queue wait), or swapped
  (memory stall). It is the data for the hedge points and for question 2.
- **M2. What a receiver's own hold does to QUIC now.** Each probe timeout and smoothed RTT before
  and after a hold, from read-time samples; the baseline for T3.
- **M3. Middlebox lifetimes.** The flow timeouts the box's and the KIND nodes' kernels state [9],
  and the binding lifetime on one client path through a NAT by idling and testing [8 §4.6].
- **M4. The detector under holds.** Under T1's holds on Linux, macOS and Windows: each suspicion of
  the held node, its stated bound against when it came, and every suspicion the held node made after
  its resume.
- **T1. Holds end to end.** In `crates/mantle/tests/node.rs`'s cell, the trace's probe stops a node
  at a chosen operation and releases it on facts: every live peer reports it suspected; for the
  longer hold, also its plane socket reports drops; for the hold for good, the client's exchange
  reaches its deadline. Assert: the read returns its object's exact bytes, or for good ends with the
  deadline's refusal at or after the deadline and not before; no connection between the held node
  and its peers or the client ended, by the links' `dials` and `ended` counters (`node.rs:501-506`)
  unchanged across the hold; every suspicion of the held node lies inside its hold; the held node
  made no suspicion of a live peer from a point inside its hold.
- **T2. The driver's order.** Hold a node until each peer's counters show datagrams sent to it after
  the stop was a fact, then release. Assert that every datagram whose stamp precedes the first timer
  the driver fired was handled before that timer, and that no exchange or connection was refused for
  a period whose bytes were waiting.
- **T3. Stamps in QUIC.** On Linux and macOS, hold a receiver while its peer's acknowledgements
  arrive. Assert that each round-trip sample taken from an acknowledgement queued in the hold equals
  its stamp less its packet's send time less the reported delay, exactly; on Windows, record what the
  samples are.
- **T4. An overflowed socket.** Hold a node until its plane socket's drop count rises, then release.
  Assert that it suspects no peer from a freshness point inside its hold.
- **T5. Periods on running time,** in hyper-sim's deterministic worlds: a judge frozen across a
  period's end with the peer's bytes in its queue refuses nothing; with the peer dead, the refusal
  comes exactly one period of the judge's running time after its resume.
- **T6. A run's end ends the link, a hold does not.** Kill a node with SIGKILL and start it again on
  a restart key: its peers' old connections end at the first packet answered by a verified stateless
  reset, with no timer, and the links are keyed again. Stop a node with SIGSTOP instead: no
  connection ends.
- **T7. The tests' and redials' rate.** Suspect a peer whose process is gone, and dial an address
  with no listener; count the datagrams each sends over a window: at most one every 3 s on average
  plus one attempt's worth [6 §3.1.3], with the pauses doubling from the measured probe timeout
  under equal jitter, the exact sequence of the node's seeded draws
  (`vendor/hyper-timing/src/election.rs:382-385`). Start the gone peer again on its restart key:
  the link ends at the first test after it listens, by a verified stateless reset, and the redial
  that follows connects.
- **T8. Keep-alive cadence.** A connection with nothing outstanding sends no PING over a window; one
  with an exchange outstanding and nothing else flowing sends them at the path's cadence, never more
  than one every 15 s [6 §3.5].
- **T9. Held for good stays bounded.** A peer held until its ranges have elected elsewhere and
  repair has replaced it: its links' bytes at each peer stay within `bounds::beside`'s stated bound
  (`bounds.rs:340-381`, measured by `crates/node/tests/transport_memory.rs`); its link's tests come
  at most one every 3 s and QUIC's own probes follow the doubling to row 38; every exchange to it
  ended at its deadline.

## Open questions for the owner

1. Should a link to a peer that never comes back ever be closed by time? The design keeps it until
   the cell removes the peer or the node's memory budget needs the room, and never on a timer.
2. When a node goes silent, should a client's read wait for it until its deadline (60 s if the
   caller gives none), or also ask another node once the silence passes what that node usually takes,
   and use whichever answers first?
3. Inside a cell, should a link send keep-alives at all before a middlebox's lifetime is measured,
   or start at the 30 s the QUIC RFCs give for paths through a NAT?
4. Should a suspected peer's link be tested as often as RFC 8085 allows toward a peer that answers
   nothing, one datagram every 3 s, so a restarted peer is found within 3 s of listening; or less
   often, at the 60 s RFC 6298 sets as the least cap on TCP's timer, which sends fewer datagrams to a
   dead host but finds a restart later?
5. Should a liveness stream go on sending a heartbeat each interval to a peer silent for a long
   time, or slow down as RFC 8085 asks of a sender that hears nothing back? That is hyper-raft's
   crate to change.
6. Two bounds were the serve period, or a third of it, and need a basis of their own once it no
   longer means "as long as a connection may stay silent": when a client's idle connection may be
   replaced by a newcomer's, and how often a server says it is at work. Should both go to
   hyper-transport's maintainers with this note?
7. The changes fall in hyper-raft's crates: no idle timeout and a handshake bound of its own in
   hyper-transport and hyper-quic, a restart key, kernel stamps into QUIC, the driver's order, and
   periods on running time. Should they go to hyper-raft's agent, with Quiver reviewing as for D2?
8. Windows gives no kernel receive stamp. Is a weaker guarantee there acceptable, the driver's order
   and periods on running time alone?
9. Should the test harness's quiet watch count time its own process was held still? The trace left
   this open beside the links.

## Sources

[1] J. Iyengar and M. Thomson (eds.), RFC 9000, "QUIC: A UDP-Based Multiplexed and Secure
Transport", May 2021, https://www.rfc-editor.org/rfc/rfc9000.txt, §7.4, §10.1, §10.1.1, §10.1.2,
§10.2, §10.3, §13.2.1, §18.2.

[2] J. Iyengar and I. Swett (eds.), RFC 9002, "QUIC Loss Detection and Congestion Control", May 2021,
https://www.rfc-editor.org/rfc/rfc9002.txt, §5.3, §6.1.2, §6.2, §6.2.1, §6.2.2, §6.2.4, §7.6.1,
App. A.2.

[3] M. Kühlewind and B. Trammell, RFC 9308, "Applicability of the QUIC Transport Protocol",
September 2022, https://www.rfc-editor.org/rfc/rfc9308.txt, §3.2, §10.

[4] V. Jacobson and M. J. Karels, "Congestion Avoidance and Control", November 1988, the revised
version of the paper in Proc. ACM SIGCOMM '88, https://ee.lbl.gov/papers/congavoid.pdf, §2, App. A,
note 21.

[5] V. Paxson, M. Allman, J. Chu and M. Sargent, RFC 6298, "Computing TCP's Retransmission Timer",
June 2011, https://www.rfc-editor.org/rfc/rfc6298.txt, (2.1) to (2.5), §3, §4, (5.5), §5, App. A.

[6] L. Eggert, G. Fairhurst and G. Shepherd, RFC 8085 (BCP 145), "UDP Usage Guidelines", March 2017,
https://www.rfc-editor.org/rfc/rfc8085.txt, §3.1.1, §3.1.3, §3.5.

[7] F. Audet and C. Jennings (eds.), RFC 4787 (BCP 127), "Network Address Translation (NAT)
Behavioral Requirements for Unicast UDP", January 2007, https://www.rfc-editor.org/rfc/rfc4787.txt,
§4.3, REQ-5.

[8] D. MacDonald and B. Lowekamp, RFC 5780, "NAT Behavior Discovery Using Session Traversal
Utilities for NAT (STUN)", May 2010, https://www.rfc-editor.org/rfc/rfc5780.txt, §4.6.

[9] Linux kernel documentation, "Netfilter Conntrack Sysfs variables",
`Documentation/networking/nf_conntrack-sysctl.rst` on the kernel's master branch,
https://raw.githubusercontent.com/torvalds/linux/master/Documentation/networking/nf_conntrack-sysctl.rst:
`nf_conntrack_udp_timeout`, `nf_conntrack_udp_timeout_stream`.

[10] T. D. Chandra and S. Toueg, "Unreliable Failure Detectors for Reliable Distributed Systems",
Journal of the ACM 43(2), March 1996, pp. 225–267, read from
https://cs.nyu.edu/~apanda/classes/sp25/papers/chandra-toueg.pdf, §1, §9.1, Fig. 10, Theorem 9.1.

[11] W. Chen, S. Toueg and M. K. Aguilera, "On the Quality of Service of Failure Detectors", IEEE
Transactions on Computers 51(5), May 2002, pp. 561–580, read from
https://www.microsoft.com/en-us/research/wp-content/uploads/2002/05/ieeetc02_fdqos.pdf, §1.2.1,
§1.2.2, §3.1, Theorem 5, §5.1, Theorem 9, §6.3, §8.1.1, §8.1.2.

[12] M. Bertier, O. Marin and P. Sens, "Implementation and Performance Evaluation of an Adaptable
Failure Detector", Proc. DSN 2002, pp. 354–363. Not read: HAL's copy (hal-01544787) refused
automated reading and the authors' LIP6 server did not answer. Quoted from [13] §II-D and §V-E.

[13] N. Hayashibara, X. Défago, R. Yared and T. Katayama, "The φ Accrual Failure Detector", Proc.
SRDS 2004, pp. 66–78, read as JAIST research report IS-RR-2004-010 (2004-05-10),
https://dspace02.jaist.ac.jp/dspace/bitstream/10119/4784/1/IS-RR-2004-010.pdf, §II-C, §II-D,
§IV-A, §IV-B, §V-D, §V-E.

[14] B. Satzger, A. Pietzowski, W. Trumler and T. Ungerer, "A New Adaptive Accrual Failure Detector
for Dependable Distributed Systems", Proc. ACM SAC 2007, pp. 551–555. Not read: ACM's copy refused
(HTTP 403). Quoted from [15].

[15] J. Seeger, R. A. Deshmukh, V. Sarafov and A. Bröring, "Dynamic IoT Choreographies: Managing
Discovery, Distribution, Failure and Reconfiguration", IEEE Pervasive Computing 18(1), 2019,
DOI 10.1109/MPRV.2019.2907003, read as arXiv:1803.03190, https://arxiv.org/pdf/1803.03190, the
section evaluating its failure detector against [13] and [14].

[16] A. Das, I. Gupta and A. Motivala, "SWIM: Scalable Weakly-consistent Infection-style Process
Group Membership Protocol", Proc. DSN 2002, read from
https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf, §3.1, §4.2.

[17] A. Dadgar, J. Phillips and J. Currey, "Lifeguard: Local Health Awareness for More Accurate
Failure Detection", DSN Workshops 2018, pp. 22–25, read as arXiv:1707.00788v2 (3 April 2018),
https://arxiv.org/pdf/1707.00788, §IV, §IV-A, §V-D, §VII.

[18] J. B. Leners, H. Wu, W.-L. Hung, M. K. Aguilera and M. Walfish, "Detecting Failures in
Distributed Systems with the Falcon Spy Network", Proc. SOSP 2011, pp. 279–294, read from
https://cs.utexas.edu/falcon/papers/falcon-sosp11.pdf, §2.2, §2.3, §3.4, §4.

[19] J. B. Leners, T. Gupta, M. K. Aguilera and M. Walfish, "Improving Availability in Distributed
Systems with Failure Informers" (Pigeon), Proc. NSDI 2013, pp. 427–441, read from
https://www.usenix.org/system/files/conference/nsdi13/nsdi13-final90.pdf, §2.2, §3.1, §3.2, §3.3,
§4.2, §4.4. The brief for this note dated Pigeon 2015; Pigeon is the NSDI 2013 paper, and the
authors' 2015 paper is [20].

[20] J. B. Leners, T. Gupta, M. K. Aguilera and M. Walfish, "Taming Uncertainty in Distributed
Systems with Help from the Network" (Albatross), Proc. EuroSys 2015. Not read: the authors' page and
Leners's dissertation refused automated reading. Quoted from A. Colyer's summary, The Morning Paper,
2015-05-05,
https://blog.acolyer.org/2015/05/05/taming-uncertainty-in-distributed-systems-with-help-from-the-network/.

[21] Linux man-pages: socket(7), `SO_TIMESTAMPNS` and `SO_RXQ_OVFL`,
https://man7.org/linux/man-pages/man7/socket.7.html; proc_pid_stat(5), the state field,
https://man7.org/linux/man-pages/man5/proc_pid_stat.5.html.

[22] Linux kernel documentation, "PSI - Pressure Stall Information"
(`Documentation/accounting/psi.rst`) and "Scheduler Statistics", `/proc/<pid>/schedstat`
(`Documentation/scheduler/sched-stats.rst`), the kernel's master branch on GitHub.

[23] The Rust standard library, `std::time::Instant`, "Underlying System calls",
https://doc.rust-lang.org/std/time/struct.Instant.html: `CLOCK_MONOTONIC` on Unix,
`CLOCK_UPTIME_RAW` on Darwin, `QueryPerformanceCounter` on Windows, and "it is also not specified
whether system suspends count as elapsed time or not". mantle's QUIC timers are not on it: the
driver gives QUIC instants on the shard's clock (`driver.rs:380-383`), which counts suspend
(`vendor/hyper-rt/src/machine/clock.rs:1-5`).

[24] hyper-raft, `docs/timing.md` and `docs/research/timing.md` at `0a182ae` (the snapshot every
vendored crate's `SNAPSHOT` names), §2.2, §2.3, §2.4, §2.6, §2.8, with the measurements their
heartbeat traces recorded (hyper-raft `docs/benchmarks.md`, "Heartbeat traces"); and the merge
`e6b3372`, "the detector model on starved hosts", whose account measured suspicions of live peers on
hyper-raft-e2e falling from 147, 216, 434 and 207 to 0, 20, 6 and 0 (macOS; Linux at 4, 2 and 1
CPUs).

[25] mantle, `docs/measurements/2026-11-06-node-test-holds.md` (main `cc720c05`, test binary SHA-256
`079c4a871d6422c8…`, the box, 2026-11-06), with `docs/research/77-progress-and-patience.md`,
`docs/research/78-a-cell-in-kubernetes.md` and `docs/design/node.md` §3.5, §3.8 and §5.4, and the code
at `e718cddb` cited by file and line above.
