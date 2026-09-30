# 25 — The node's runtime: async I/O, HTTP, QUIC, datagrams, fairness and pressure

**Status:** research input for `docs/design/node.md`, the design of the `mantle serve` process.
This is not a decision record; the decisions it supports are in the design.
**Compiled:** 2026-09-30.
**Scope:** the primary sources behind the parts of the node that no earlier note covers: how
tokio treats blocking and CPU-bound work and what its queues bound; what HTTP/1.1 and HTTP/2
require of a server around `Expect: 100-continue`, framing, flow control and resets, and what
hyper does by default; what S3 clients expect of a server that sheds load; how QUIC and quinn
bound a connection's memory and what they expose to change it; 0-RTT replay; unreliable
datagrams, UDP congestion and replay windows; deficit round robin; Lifeguard's correction to
SWIM; and Linux's memory-pressure signals. Earlier notes already cover the consensus stack
(07), slates' runtime and transport (08), cells, routing and S3's 503 contract (09 §3.5–§3.6,
§6.3), the operating-parameter models (11), the engine (12, 23, 24), SWIM and the φ detector
(06 §A8), and FoundationDB's simulation (06 §A5).

---

## 0. How to read this note

**Citation tags.** `[KEY §section]`. Crate documentation is cited by item path, source files
by path at the tag named. Earlier notes are cited as "note 07 §x".

**Quotes.** Quotes in "double quotes" are verbatim from the fetched text. Where the source
wraps a line, the break is replaced by one space. For DRR, text extraction dropped the fi/fl/ff
ligatures and the ≥ sign; both are restored and nothing else is changed.

**Evidence labels.**
- **primary**: a standard (RFC), vendor documentation, or a library's own documentation or
  source, read directly.
- *(no label)*: a peer-reviewed paper, checked against its text.
- **NON-PEER-REVIEWED**: preprints and blog posts.
- **ARCHIVED**: an AWS page no longer served at its URL, read from the Internet Archive.
- **DERIVED** / **INFERENCE**: reasoning in this note; the sources do not state it.
- **UNVERIFIED**: not confirmed against a primary text.

**Method.** Every source was fetched on 2026-09-30 (UTC) as raw text (RFC `.txt`, docs.rs
HTML, GitHub raw source, AWS markdown or HTML) and each quote was checked by machine against
that text with whitespace collapsed. docs.rs "latest" was tokio 1.53.1, quinn 0.11.12, rustls
0.23.45 and hyper 1.11.1 that day.

---

## Sources

| Key | Source | Label |
|---|---|---|
| TOKIO | tokio 1.53.1 documentation: crate root §"CPU-bound tasks and blocking code"; `tokio::task` §"Blocking and Yielding"; `task::spawn_blocking`; `task::block_in_place`; `task::coop`; `runtime::Builder::max_blocking_threads`; `sync::Semaphore`; `sync::mpsc::channel`. https://docs.rs/tokio/latest/tokio/ | primary |
| HYPER | hyper 1.11.1: `server::conn::http1::Builder`, `server::conn::http2::Builder` (docs.rs); source `src/body/incoming.rs`, `src/proto/h1/conn.rs`, `src/proto/h2/server.rs` at tag v1.11.1 | primary |
| H2 | h2: `src/proto/mod.rs`, `src/server.rs` (master), CHANGELOG 0.3.17; RUSTSEC-2023-0034; GHSA-8r5v-vm4m-4g25 / RUSTSEC-2024-0003 | primary |
| RAPID | NVD, CVE-2023-44487; GitHub advisory GHSA-qppj-fm5r-hxr3; S. McArthur, "hyper HTTP/2 Rapid Reset Attack: Unaffected", 2023-10-10, https://seanmonstar.com/blog/hyper-http2-rapid-reset-unaffected/ | primary; the blog NON-PEER-REVIEWED |
| RFC9110 | HTTP Semantics, §10.1.1, §10.2.3, §15.6.4 | primary |
| RFC9112 | HTTP/1.1, §6.3, §9.6, §11.2 | primary |
| RFC9113 | HTTP/2, §5.2.1, §6.5.2, §6.8, §8.1, §10.5, §10.5.1 | primary |
| S3-REDIRECT | Amazon S3 API Reference, "Redirects and 100-Continue" (RESTRedirect.html), Wayback capture 2026-03-09 | primary, ARCHIVED |
| S3-ERR | Amazon S3 API Reference, "List of error codes" (ErrorResponses.html), Wayback capture 2025-12-31 | primary, ARCHIVED |
| S3-PERF | Amazon S3 User Guide, "Best practices design patterns: optimizing Amazon S3 performance" and its design-patterns page | primary |
| SDK-RETRY | AWS SDKs and Tools Reference Guide, "Retry behavior", https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html | primary |
| RFC9000 | QUIC, §4.1, §4.6, §21.6–§21.9 | primary |
| QUINN | quinn 0.11.12: `TransportConfig`, `Connection`, `SendStream::set_priority` (docs.rs) | primary |
| RFC9001 | Using TLS to Secure QUIC, §9.2 | primary |
| RFC8446 | TLS 1.3, §2.3, §7.5, §8, Appendix E.5 | primary |
| RUSTLS | rustls 0.23.45: crate root, `ServerConfig::max_early_data_size`, `WebPkiClientVerifier` (docs.rs) | primary |
| RFC9221 | An Unreliable Datagram Extension to QUIC, §5, §5.2–§5.4 | primary |
| RFC8085 | UDP Usage Guidelines (BCP 145), §3.1, §3.1.3, §3.2 | primary |
| RFC4303 | IP Encapsulating Security Payload, §3.4.3 | primary |
| DRR | M. Shreedhar, G. Varghese. "Efficient Fair Queuing using Deficit Round Robin." SIGCOMM '95. https://conferences.sigcomm.org/sigcomm/1995/papers/shreedhar.pdf | peer-reviewed |
| LIFEGUARD | A. Dadgar, J. Phillips, J. Currey. "Lifeguard: Local Health Awareness for More Accurate Failure Detection." arXiv:1707.00788v2, 2018 | NON-PEER-REVIEWED |
| PSI | Linux kernel documentation, "PSI - Pressure Stall Information", https://docs.kernel.org/accounting/psi.html | primary |
| CGROUP2 | Linux kernel documentation, "Control Group v2", `memory.high`, https://docs.kernel.org/admin-guide/cgroup-v2.html | primary |

---

## 1. tokio: blocking, CPU-bound work, and what its queues bound

- Two kinds of threads: "code that spends a long time without reaching an .await will prevent
  other tasks from running. To combat this, Tokio provides two kinds of threads: Core threads
  and blocking threads." [TOKIO, crate root]
- "A blocking operation performed in a task running on a thread that is also running other
  tasks would block the entire thread, preventing other tasks from running." [TOKIO,
  `tokio::task` §Blocking and Yielding]
- CPU-bound work: "If your code is CPU-bound and you wish to limit the number of threads used to
  run it, you should use a separate thread pool dedicated to CPU bound tasks." [TOKIO, crate
  root]
- The blocking pool: "Tokio will spawn more blocking threads when they are requested through
  this function until the upper limit configured on the Builder is reached. After reaching the
  upper limit, the tasks are put in a queue." "When running many CPU-bound computations, a
  semaphore or some other synchronization primitive should be used to limit the number of
  computations executed in parallel." "Be aware that tasks spawned using spawn_blocking cannot
  be aborted because they are not async." [TOKIO, `spawn_blocking`]
- Its bound: `max_blocking_threads` — "The default value is 512." "If no idle thread is
  available and no more threads are allowed to be spawned, the task will remain in the queue
  until one of the busy threads pick it up. Note that since the queue does not apply any
  backpressure, it could potentially grow unbounded." The method also panics "if val is not
  larger than 0". [TOKIO, `Builder::max_blocking_threads`]
- `block_in_place`: "any other code running concurrently in the same task will be suspended
  during the call"; it "cannot be used within a current_thread runtime"; "Code running behind
  block_in_place cannot be cancelled." [TOKIO, `block_in_place`]
- Cooperative budget: "If a task runs for a long period of time without yielding back to the
  executor, it can starve other tasks waiting on that executor to execute them, or drive
  underlying resources." "Tokio has explicit yield points in a number of library functions,
  which force tasks to return to the executor periodically." [TOKIO, `task::coop`] The size of
  the budget is not stated in the documentation (**UNVERIFIED** here).
- `Semaphore`: "This Semaphore is fair, which means that permits are given out in the order they
  were requested. This fairness is also applied when acquire_many gets involved". Its
  `MAX_PERMITS` "is usize::MAX >> 3. Exceeding this limit typically results in a panic."
  [TOKIO, `sync::Semaphore`]
- `mpsc::channel`: "Creates a bounded mpsc channel for communicating between asynchronous tasks
  with backpressure." It "Panics if the buffer capacity is 0, or too large." [TOKIO,
  `sync::mpsc::channel`]

**For mantle (INFERENCE).** tokio's blocking pool is bounded in threads and unbounded in queue,
so it cannot be the bound CLAUDE.md rule 2 asks for; work handed to it needs a permit taken
first, and work that must be cancellable or must stop at shutdown does not belong in it. The
panicking constructors (a zero capacity, too many permits, a zero thread limit) are reachable
only from values the node computes, so each is checked before the call (CLAUDE.md rule 1).

## 2. HTTP/1.1 and HTTP/2 at the server

**`Expect: 100-continue`** [RFC9110 §10.1.1]:
- "an origin server MUST send either: * an immediate response with a final status code, if that
  status can be determined by examining just the method, target URI, and header fields, or * an
  immediate 100 (Continue) response to encourage the client to send the request content."
- "The origin server MUST NOT wait for the content before sending the 100 (Continue)
  response."
- "A client that sends a 100-continue expectation is not required to wait for any specific
  length of time; such a client MAY proceed to send the content even if it has not yet
  received a response."
- "A server that responds with a final status code before reading the entire request content
  SHOULD indicate whether it intends to close the connection ... or continue reading the request
  content."
- 417 is for other expectations: "A server that receives an Expect field value containing a
  member other than 100-continue MAY respond with a 417 (Expectation Failed) status code".

**hyper's handling** (source, v1.11.1; the documentation says nothing): the body's sender
"won't becoming ready until the `Body` has been polled for data once" (`body/incoming.rs`), and
the connection writes "the 100 Continue if not already responded" when the body is first read,
only while no response has started and only for requests after HTTP/1.0
(`proto/h1/conn.rs`); a handler that answers without reading the body makes it "skip sending the
100-continue". No automatic 100 was found in hyper's HTTP/2 server; h2 exposes
`send_informational` (**UNVERIFIED** whether hyper wires it). [HYPER]

**503 and Retry-After** [RFC9110]: "The 503 (Service Unavailable) status code indicates that the
server is currently unable to handle the request due to a temporary overload or scheduled
maintenance ... The server MAY send a Retry-After header field" (§15.6.4); "When sent with a
503 (Service Unavailable) response, Retry-After indicates how long the service is expected to
be unavailable to the client" (§10.2.3).

**Framing and smuggling** [RFC9112]: "If a message is received with both a Transfer-Encoding and
a Content-Length header field, the Transfer-Encoding overrides the Content-Length. Such a
message might indicate an attempt to perform request smuggling (Section 11.2) or response
splitting (Section 11.1) and ought to be handled as an error." (§6.3 item 3); "If the
unrecoverable error is in a request message, the server MUST respond with a 400 (Bad Request)
status code and then close the connection." (§6.3 item 5); "If the sender closes the connection
or the recipient times out before the indicated number of octets are received, the recipient
MUST consider the message to be incomplete and close the connection." (§6.3 item 6). §11.2 adds
no rule of its own beyond pointing to §6.3.

**HTTP/2 flow control and limits** [RFC9113]:
- "A receiver MAY choose to set any window size that it desires for each stream and for the
  entire connection. A sender MUST respect flow-control limits imposed by a receiver." "The
  initial value for the flow-control window is 65,535 octets for both new streams and the
  overall connection." (§5.2.1)
- `SETTINGS_MAX_CONCURRENT_STREAMS`: "Initially, there is no limit to this value. It is
  recommended that this value be no smaller than 100, so as to not unnecessarily limit
  parallelism." `SETTINGS_MAX_HEADER_LIST_SIZE`: "This advisory setting informs a peer of the
  maximum field section size that the sender is prepared to accept"; "The initial value of this
  setting is unlimited." (§6.5.2)
- "Implementations SHOULD track the use of these features and set limits on their use. An
  endpoint MAY treat activity that is suspicious as a connection error (Section 5.4.1) of type
  ENHANCE_YOUR_CALM." (§10.5) "A server that receives a larger field block than it is willing to
  handle can send an HTTP 431 (Request Header Fields Too Large) status code" (§10.5.1).

**Answering before the body, and closing** [RFC9113 §8.1, §6.8; RFC9112 §9.6]:
- "A server can send a complete response prior to the client sending an entire request if the
  response does not depend on any portion of the request that has not been sent and received.
  When this is true, a server MAY request that the client abort transmission of a request without
  error by sending a RST_STREAM with an error code of NO_ERROR after sending a complete response".
  (RFC 9113 §8.1)
- "A server that is attempting to gracefully shut down a connection SHOULD send an initial GOAWAY
  frame with the last stream identifier set to 2^31-1 and a NO_ERROR code." "After allowing time
  for any in-flight stream creation (at least one round-trip time), the server MAY send another
  GOAWAY frame with an updated last stream identifier." (RFC 9113 §6.8)
- "in a response, the same field indicates that the server is going to close this connection
  after the response message is complete." (RFC 9112 §9.6, of `Connection: close`)

**Rapid Reset.** "The HTTP/2 protocol allows a denial of service (server resource consumption)
because request cancellation can reset many streams quickly, as exploited in the wild in August
through October 2023." [RAPID, NVD] hyper's maintainer: "hyper is not affected. Especially if
you have h2 v0.3.18 or newer." "The way hyper handles frames, it will cancel out the stream
before ever making it available for handlers, so the cost is local." [RAPID, blog] The bound
came earlier: h2 0.3.17 added "`max_pending_accept_reset_streams(usize)`" after
RUSTSEC-2023-0034, where "the pending accept queue can grow in memory usage"; RUSTSEC-2024-0003
"limits the total number of internal error resets emitted by default before the connection is
closed" (h2 0.3.24 and 0.4.2). [H2]

**hyper's builders** [HYPER]:
- http1: `header_read_timeout` — "If a client does not transmit the entire header within this
  time, the connection is closed." "Panics if header_read_timeout is configured without a
  Timer." "Default is 30 seconds." `max_buf_size` — "Default is ~400kb." "The minimum value
  allowed is 8192. This method panics if the passed max is less than the minimum."
- http2: `max_concurrent_streams` — "Default is 200, but not part of the stability of hyper ...
  You are encouraged to set your own limit." `max_header_list_size` — "Default is currently
  16KB". `max_pending_accept_reset_streams` — "As of v0.4.0, it is 20."
  `max_local_error_reset_streams` — "If None is supplied, hyper will not apply any limit."
  `adaptive_window` — "Enabling this will override the limits set in initial_stream_window_size
  and initial_connection_window_size." Source defaults: 1 MiB stream and connection windows,
  adaptive window off (`proto/h2/server.rs`).

**For mantle (INFERENCE).** hyper's 100-continue behavior is what S3 needs, provided the
handler decides admission and authentication before it first reads the body. Every default above
is a library's choice, not a derivation, so the node sets each from its own budget; two of the
setters panic on values the node must therefore check first.

## 3. What S3 clients expect of a server that sheds load

- 100-continue: "configure your application to use 100-continues for PUT operations. When your
  application uses 100-continue, it does not send the request body until it receives an
  acknowledgement. If the message is rejected based on the headers, the body of the message is
  not sent." "Amazon S3 does recognize if your request contains an Expect: Continue and will
  respond with a provisional 100-continue status or a final status code." [S3-REDIRECT] The live
  PutObject page shows the header only in examples.
- Error codes [S3-ERR]: `RequestTimeout`, 400, "Your socket connection to the server was not
  read from or written to within the timeout period."; `SlowDown`, 503 Slow Down, "Please reduce
  your request rate."; `IncompleteBody`, 400, "You did not provide the number of bytes
  specified by the Content-Length HTTP header."; `ServiceUnavailable`, 503, "Service is unable
  to handle request."
- Rates [S3-PERF]: "your application can achieve at least 3,500 PUT/COPY/POST/DELETE or 5,500
  GET/HEAD requests per second per partitioned Amazon S3 prefix." "While Amazon S3 is scaling to
  your new higher request rate, you may see some 503 (Slow Down) errors." "If these errors
  occur, each AWS SDK implements automatic retry logic using exponential backoff."
- SDK retries [SDK-RETRY]: the page describes behavior that "requires opting in until it
  becomes the default behavior. Set `AWS_NEW_RETRIES_2026=true` ... Without this setting, your
  SDK uses pre-2026 retry behavior, which differs in backoff timing, retry quota costs, and
  service-specific defaults." Under it: "Standard mode retries failed requests using exponential
  backoff with jitter"; max attempts default 3, "one initial request and up to two retries";
  "delay = random(0, 1) × min(20,000 ms, base_delay × 2^retry)" with a base of "50 ms for
  transient errors or 1,000 ms for throttling errors"; a retry quota of 500 tokens, 14 per
  transient retry and 5 per throttling retry. `SlowDown` is a throttling error; "an HTTP 400 with
  the error code `RequestTimeout` is classified as transient and retried", as is "any HTTP 500,
  502, 503, or 504 without a recognized error code". "Some AWS services include an
  `x-amz-retry-after` header in error responses. The header value is a delay in
  milliseconds."

**For mantle (DERIVED).** A client that honors the standard mode sends at most three attempts,
and on a throttling error waits a uniformly random delay below 1 s, then below 2 s. A
`SlowDown` therefore sheds load only if the condition that caused it is measured in the same
units: a server that refuses for a condition lasting longer than the client's retries answers
the client's third attempt too, and that client's request fails. Whether S3 sends
`x-amz-retry-after` is **UNVERIFIED**.

## 4. QUIC connections: flow control, concurrency and memory

**The protocol** [RFC9000]:
- "QUIC employs a limit-based flow control scheme where a receiver advertises the limit of total
  bytes it is prepared to receive on a given stream or for the entire connection." "Subsequently,
  a receiver sends MAX_STREAM_DATA frames ... or MAX_DATA frames ... to the sender to advertise
  larger limits." (§4.1)
- "An endpoint limits the cumulative number of incoming streams a peer can open." (§4.6)
- Slowloris: "QUIC deployments SHOULD provide mitigations for the Slowloris attacks, such as
  increasing the maximum number of clients the server will allow, limiting the number of
  connections a single IP address is allowed to make, imposing restrictions on the minimum
  transfer speed a connection is allowed to have, and restricting the length of time an endpoint
  is allowed to stay connected." (§21.6)
- Reassembly: "The attack on receivers is mitigated if flow control windows correspond to
  available memory. However, some receivers will overcommit memory and advertise flow control
  offsets in the aggregate that exceed actual available memory." (§21.7)
- "implementations SHOULD track cost of processing relative to progress and treat excessive
  quantities of any non-productive packets as indicative of an attack." (§21.9)

**quinn** [QUINN]:
- `receive_window`: "Maximum number of bytes the peer may transmit across all streams of a
  connection before becoming blocked." "This should be set to at least the expected connection
  latency multiplied by the maximum desired throughput."
- `stream_receive_window`: "Setting this smaller than receive_window helps ensure that a single
  stream doesn't monopolize receive buffers".
- `send_window`: "Endpoints that wish to handle large numbers of connections robustly should take
  care to set this low enough to avoid memory exhaustion if every connection uses the entire
  window."
- `max_concurrent_bidi_streams`: "Worst-case memory use is directly proportional to
  max_concurrent_bidi_streams * stream_receive_window, with an upper bound proportional to
  receive_window."
- Changed on a live connection: `Connection::set_receive_window` ("See
  proto::TransportConfig::receive_window()") and `set_max_concurrent_bi_streams` ("Large counts
  increase both minimum and worst-case memory consumption.").
- `max_idle_timeout`: "Defaults to 30 seconds." `keep_alive_interval`: "None to disable, which is
  the default." `initial_mtu` "Must be at least 1200, which is the default"; `min_mtu`: "If the
  provided value is higher than what the network path actually supports, the result will be
  unpredictable and catastrophic packet loss".
- Priorities: "Every send stream has an initial priority of 0. Locally buffered data from streams
  with higher priority will be transmitted before data from streams with lower priority.
  Changing the priority of a stream with pending data may only take effect after that data has
  been transmitted." (`SendStream::set_priority`) `send_fairness`: "When enabled, connections
  schedule data from outgoing streams having the same priority in a round-robin fashion."
  "Higher priority streams always take precedence over lower priority streams."
- Datagrams: `send_datagram` — "data must both fit inside a single QUIC packet and be smaller
  than the maximum dictated by the peer." "Previously queued datagrams which are still unsent may
  be discarded to make space for this datagram". `max_datagram_size` "may change over the
  lifetime of a connection according to variation in the path MTU estimate."
- Exporters: `Connection::export_keying_material` — "Derive keying material from this
  connection's TLS session secrets. When both peers call this method with the same label and
  context arguments and output buffers of equal length, they will get the same sequence of
  bytes in output. These bytes are cryptographically strong and pseudorandom, and are suitable
  for use as keying material."

**For mantle (DERIVED).** A node's worst-case QUIC receive memory is the sum of its connections'
`receive_window`s, and its retained send memory the sum of their `send_window`s, whatever the
per-stream settings. Both are the node's to set and to lower on a live connection, so they can
follow the node's budget (audit §11.8's 144 MiB windows came from constants that did not).

## 5. Replay, early data and keying material

- "The use of 0-RTT in QUIC is similarly vulnerable to replay attack." "Ultimately, the
  responsibility for managing the risks of replay attacks with 0-RTT lies with an application
  protocol." "Disabling 0-RTT entirely is the most effective defense against replay attack."
  [RFC9001 §9.2]
- "There are no guarantees of non-replay between connections." [RFC8446 §2.3] "TLS does not
  provide inherent replay protections for 0-RTT data." [§8] Applications must be "specifically
  engineered to be safe under replay (minimally, this means idempotent, but in many cases may
  also require other stronger conditions, such as constant-time response)", and "Application
  protocols MUST NOT use 0-RTT data without a profile that defines its use." [Appendix E.5]
- Exporters: "TLS-Exporter(label, context_value, key_length) = HKDF-Expand-Label(Derive-Secret(
  Secret, label, ""), "exporter", Hash(context_value), key_length)". [RFC8446 §7.5]
- rustls: "by default it uses aws-lc-rs for implementing the cryptography in TLS."
  `max_early_data_size`: "Specify 0 to disable early data. The default is 0."
  `WebPkiClientVerifier`: "A client certificate verifier that uses the webpki crate to perform
  client certificate validation." [RUSTLS]

## 6. Datagrams outside a congestion-controlled stream

- QUIC's own datagrams: "DATAGRAM frames are not retransmitted upon loss detection" (§5.2); they
  "do not provide any explicit flow control signaling" and "MAY be dropped by the receiver if the
  receiver cannot process them" (§5.3); "DATAGRAM frames employ the QUIC connection's congestion
  controller. ... The sender MUST either delay sending the frame until the controller allows it
  or drop the frame without sending it" (§5.4); "DATAGRAM frames cannot be fragmented" (§5).
  [RFC9221]
- UDP: "If an application or protocol chooses not to use a congestion-controlled transport
  protocol, it SHOULD control the rate at which it sends UDP datagrams to a destination host";
  an application using several sockets "SHOULD perform congestion control over the aggregate
  traffic" (§3.1). "Applications that at any time exchange only a few UDP datagrams with a
  destination SHOULD still control their transmission behavior by not sending on average more
  than one UDP datagram per RTT to a destination." (§3.1.3) "an application SHOULD NOT send UDP
  datagrams that result in IP packets that exceed the Maximum Transmission Unit (MTU) along the
  path to the destination." Without a known path MTU, "For IPv4, EMTU_S is the smaller of 576
  bytes and the first-hop MTU ... For IPv6, EMTU_S is 1280 bytes" (§3.2). [RFC8085]
- Replay windows: "For each received packet, the receiver MUST verify that the packet contains a
  Sequence Number that does not duplicate the Sequence Number of any other packets received
  during the life of this SA." "Duplicates are rejected through the use of a sliding receive
  window." "The "right" edge of the window represents the highest, validated Sequence Number value
  received on this SA. Packets that contain sequence numbers lower than the "left" edge of the
  window are rejected." "A minimum window size of 32 packets MUST be supported when 32-bit
  sequence numbers are employed; a window size of 64 is preferred and SHOULD be employed as the
  default." The preliminary check before integrity "is performed prior to integrity checking and
  decryption", and the counter moves only once integrity is verified. [RFC4303 §3.4.3]

**For mantle (INFERENCE).** Raft's control messages sent as RFC 9221 datagrams would share the
connection's congestion controller with bulk streams, so a transfer in loss recovery would delay
or drop votes and heartbeats. A separate socket escapes that coupling but inherits RFC 8085's
obligation: its sending rate must be controlled per destination, and its datagrams must fit the
path MTU.

## 7. Deficit round robin

- "Our scheme achieves nearly perfect fairness in terms of throughput, requires only O(1) work to
  process a packet, and is simple enough to implement in hardware." [DRR, Abstract]
- "Consider any execution of the DRR scheme in which flow i is backlogged. After any K rounds
  the difference between K · Quantum_i (i.e., the bytes that flow i should have sent) and the
  bytes that flow i actually sends is bounded by Max." [DRR, Theorem 4.2]
- "The Work for Deficit Round Robin is O(1), if for all i, Quantum_i ≥ Max." "If Quantum ≥ Max, we
  are guaranteed to send at least one packet every time we visit a queue". [DRR, Theorem 4.5]

## 8. Failure detection when the detector itself is slow

- "slow message processing can cause SWIM to mark healthy members as failed (so called false
  positive failure detection), despite inclusion of a mechanism to avoid this." [LIFEGUARD,
  Abstract]
- "slow processing by the failure detector module itself is the primary cause of the false
  positives that SWIM's Suspicion mechanism fails to suppress." [§IV]
- The Local Health Multiplier "is a saturating counter, with a max value S and min value zero";
  a successful probe counts −1, a failed probe, a refuted suspicion about oneself and a probe with
  a missed nack +1 each; "ProbeInterval = BaseProbeInterval·(LHM(S) + 1)" and likewise the probe
  timeout. Full Lifeguard "reduces the number of false positives by a factor of between 50x and
  100x." [§IV, §V]
- The authors' own caveat: "Lifeguard has several parameters that currently use heuristically
  determined values. These include Local Health Aware Suspicion's re-gossip factor (K), the
  saturation limit of the LHM counter (S) and the scores given to the different events that
  affect the LHM counter." [§VII]

## 9. Memory pressure on Linux

- "The "some" line indicates the share of time in which at least some tasks are stalled on a
  given resource." "The "full" line indicates the share of time in which all non-idle tasks are
  stalled on a given resource simultaneously. In this state actual CPU cycles are going to waste,
  and a workload that spends extended time in this state is considered to be thrashing." [PSI]
- `memory.high`: "If a cgroup's usage goes over the high boundary, the processes of the cgroup
  are throttled and put under heavy reclaim pressure." "Going over the high limit never invokes
  the OOM killer and under extreme conditions the limit may be breached." [CGROUP2]

## 10. Unverified

- tokio's coop budget size; whether hyper's HTTP/2 server sends 100 (Continue) on its own.
- Whether S3 sends `x-amz-retry-after` on `SlowDown`, and which retry behavior deployed SDKs run
  by default before the 2026 opt-in becomes the default.
- The IETF has no RFC that sets the operating datagram size below the PMTU for latency on slow
  links; audit §13.1's airtime argument is DERIVED from serialization time.
