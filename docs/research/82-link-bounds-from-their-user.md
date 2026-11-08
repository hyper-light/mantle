# 82. Link bounds from their user: the timeouts a client or an operator states, and S3's where none is

Status: research note, 2026-11-07. For the owner's answer to research/81, verbatim: "We do need a
bound, so yes, but it should be set from the use side CLI (ie when uploading set a timeout) or the
server config file. If unset, we should provide a sane default, as prolonged connections could
block other work, eat up socket space, etc. examine s3 - what are its default timeouts?" Sources [1]
to [8] are standards, read as the RFC Editor's text; [9] to [20] are AWS's documentation; [21] to
[27] are the source code, issues and pull requests of AWS's SDKs, Go and MinIO at the commits and
dates named, with the SDKs' guides beside them; [28] is Ceph's documentation. All were read on
2026-11-07 at the addresses given at the end. [29] is mantle's own notes and code at
`68c7b831`. The S3 API Reference's two Signature Version 4 pages served the reference's index
instead of their text that day and are not quoted; what they say of a presigned URL's life is taken
from the User Guide [13] and the AWS CLI's reference [16]. AWS's GitHub issues and pull requests
[21, 26] are its SDK teams' statements, not documentation, and are named as such where quoted.
Every claim below carries a source's number, a file and line, or says it is derived and how.
Nothing was built, run or measured for this note.

## The question

Every mantle endpoint advertises a `max_idle_timeout` of 3.072 s
(`crates/protocol/src/bounds.rs:80-83`, set at `bounds.rs:212`, advertised at
`vendor/hyper-transport/src/endpoint.rs:207, 220`): the least idle timeout RFC 9000 allows at
estimates made before any sample, kept for a connection's life (research/81, "The question").
Research/79 proposed that no link end for going quiet, every end advertising 0, a link ending only
on evidence that its peer's run ended (its L1). The owner kept the bound and gave its value to its
users: a timeout given on the client's side per command, an upload's for one, or in a node's
configuration file, with a default where neither gives one, because a link held too long holds work
and sockets that others need. Research/77 records the owner's D2 beside it: each request carries how
long its client will wait, and is given 60 s, botocore's `read_timeout` and the AWS CLI's
`--cli-read-timeout`, where it carries none (`bounds.rs:27-31`).

So three questions. What Amazon S3 itself bounds and what the stock clients default to, since a
default here copies one of them; what QUIC and HTTP require of a bound that each end states; and how
mantle's clients and nodes take a bound from their users, tell it to the other end where that end
must know it, and keep research/81's evidence-based ending and learned detector beside it.

## What the sources establish

**S3 says that it ends a quiet request and a quiet connection, and not when.** Its error list has
`RequestTimeout`, 400 Bad Request: "Your socket connection to the server was not read from or
written to within the timeout period" [9], with no period. No S3 page read for this note says how
long S3 keeps an idle kept-alive connection [9]–[15]. AWS's own clients say it closes one quickly.
The CRT's S3 client, in the comment on its endpoints' cleanup: "S3 closes the idle connections in
~5 seconds" [25, `s3_client.c:92-94`]. The Java SDK's team cut its Netty client's idle time to 5 s
after finding that "service might close idle connections for a certain amount of time and there
might be a race condition where a soon-stale connection gets picked up by SDK and the connection
gets closed in the same time SDK sends out the request" [21, issue #1122, 2019-04-01]; with 5 s
"We haven't been able to reproduce this specific issue" [21, issue #1122, 2019-05-28], and the
override's comment still points at that issue (`NettyNioAsyncHttpClient.java:80-85`). The race is
HTTP/1.1's own: "a client might have started to send a new request at the same time that the server
has decided to close the "idle" connection" [6 §9.5]. S3 keeps a client's read timeout from firing
on its own long work by sending bytes: CompleteMultipartUpload "could take several minutes to
finalize", and "While processing is in progress, Amazon S3 periodically sends white space characters
to keep the connection from timing out" [10], what mantle's notes do for a node's long waits
(research/77 D1).

**S3 bounds no operation's length, only its size and when it may begin.** After a multipart upload
is begun "there is no expiry; you must explicitly complete or stop the multipart upload" [11], and
S3 "retains all the parts until you either complete or stop the upload" [11]. A part is 5 MiB to
5 GiB, at most 10,000 of them, an object at most 48.8 TiB [12]. A presigned URL lives "as high as 7
days" when the CLI or an SDK makes it and between 1 minute and 12 hours when the console does [13];
the CLI's `aws s3 presign --expires-in` is "Default is 3600 seconds. Maximum is 604800 seconds", which
its example carries as `X-Amz-Expires=604800` [16]. And "Amazon S3 checks the expiration date and
time of a signed URL at the time of the HTTP request. For example, if a client begins to download a
large file immediately before the expiration time, the download continues even if the expiration
time passes during the download" [13]. S3's deadlines decide whether a request may begin; once
begun, only silence ends it.

**S3 asks its clients to keep connections and to retry a slow request on a new one.** "We recommend
using a pool of HTTP connections and re-using each connection for a series of requests", which
"removes the need to perform TCP slow-start and Secure Sockets Layer (SSL) handshakes on each
request" [14]. "When you retry a request, we recommend using a new connection to Amazon S3 and
performing a fresh DNS lookup": a small GET or PUT "after 2 seconds" and again "after an additional 4
seconds", the slowest 5 % of large requests by their throughput [14]. "Amazon S3 doesn't have any
limits for the number of connections made to your bucket" [15].

**The stock clients split four bounds, and agree only that an operation has none.** A connect
timeout is 2 s in Java [21], 3,000 ms in the CRT [25], 3.1 s in the Rust SDK [24] and 3,100 ms in the
SDKs' standard mode [20], 30 s in Go [22, 23], and 60 s in botocore and the AWS CLI [16, 18]. A read
timeout, the wait for a byte with a request outstanding, is 60 s in botocore and the CLI [16, 18],
30 s in Java's Apache and Netty clients and none in its CRT clients [21, 26], 30 s below one byte a
second in the CRT's S3 client, "30 seconds mirrors the value currently used by the Java SDK" [25,
`s3_client.c:85-87`], 20 s below one byte a second in the Rust SDK's stalled-stream protection [24],
and in Go, where its caller sets none, a window the SDK resolves per service [22], which the table
the SDKs share leaves S3 without [26].
An idle pooled connection is closed after 5 s by Java's Netty client and 60 s by its Apache and CRT
clients [21], after 90 s by Go [22, 23], and never by botocore or the CRT S3 client, which state no
such bound [18, 25]. A whole operation is bounded in none of them unless its caller says so: "The
SDK for Java 2.x doesn't set API call timeouts or individual API call attempt timeouts by default"
[21];
the Rust SDK's operation timeout is "None" [24]; Go's `Client.Timeout` "of zero means no timeout"
[23]; botocore and the CLI name none [16, 18]. The CLI's two timeouts are command-line options only:
its configuration file names `tcp_keepalive` and `max_attempts` and neither timeout [17], and either
set to 0 is "blocking and not timeout" [16]. AWS's own defaults are moving: `defaults_mode` is
`legacy` unless set, each SDK keeping its own values, while `standard` gives "the latest recommended
default values that should be safe to run in most scenarios", a 3,100 ms connect among them, and
"New major versions of SDKs will default to standard" [20]. The AWS CLI v2 does not take the setting
[20].

**AWS's newest default leaves S3 out, and the one bound its CRT keeps short is learned.** In 2026
AWS's SDK teams began an opt-in default for clients that keep no read timeout of their own: "a
rolling inactivity window, not a deadline on the complete operation. Any byte transferred in either
direction resets the window", 5 minutes for most services, 15 for "Services with long-hold
operations", and none for "Services with streaming request bodies or bidirectional streams", since
"The longer and exempt categories account for APIs that can legitimately hold a connection open
without returning data" [26, Java #7385, JavaScript #8404]. The Rust SDK's pull request for it says
that from the same shared table "S3 is fully exempt and must receive no read timeout even at
`latest()`" [26, smithy-rs #5004, open]. Go's client already resolves such a window "per service"
where its caller sets none [22, `client.go:211-221`]. The CRT's S3 client fixes no timeout on an
upload's answer: "Typically, S3 responds to an upload in 0.2sec after the request is fully received.
But occasionally (about 0.1%) it takes 5sec to respond", so after its first samples it starts a
first-byte timeout at the larger of 1 s and their 90th percentile, moves it toward the mean plus
0.7 s, raises it when more than 0.1 % or 1 % of parts time out, and stops once it would pass 5 s
[25, `s3_client.c:3611-3657`]. So where AWS fixes a bound on a request's silence it is tens of
seconds to minutes, never S3's few seconds; the one short bound it keeps, on an upload's answer, it
learns from S3's latency rather than fixing.

**Other stores set the bounds S3 leaves unstated.** MinIO waits 30 s for the next request on a
kept-alive connection, "the maximum amount of time to wait for the next request when keep-alive are
enabled", renews a 30 s deadline at each read and each write of every connection it accepts, allows
30 s for a request's headers, and sets TCP_USER_TIMEOUT to 10 minutes, each a hidden server flag
[27]. Ceph's RGW waits 65,000 ms "for more incoming data or outgoing data before giving up",
"Setting this value to 0 will disable timeout" [28]. Neither bounds an operation.

**QUIC lets each end state its bound, holds both to the lesser, and tells both.** "Each endpoint
advertises a max_idle_timeout, but the effective value at an endpoint is computed as the minimum of
the two advertised values (or the sole advertised value, if only one endpoint advertises a non-zero
value)" [1 §10.1]; "Idle timeout is disabled when both endpoints omit this transport parameter or
specify a value of 0" [1 §18.2]; and whatever is advertised, "endpoints MUST increase the idle
timeout period to be at least three times the current Probe Timeout (PTO)" [1 §10.1]. Transport
parameters "are declarations that are made unilaterally by each endpoint", and "Once the handshake
completes, the transport parameters declared by the peer are available" [1 §7.4]. RFC 9308 asks for
what the owner asks: "An application therefore should be able to configure its own maximum value, as
well as have access to the computed minimum value for this connection. An application may adjust
the maximum idle timeout for new connections based on the number of open or expected connections
since shorter timeout values may free up resources more quickly" [3 §10]. An end that gives up a
connection sooner says so: "By announcing a max_idle_timeout, an endpoint commits to initiating an
immediate close (Section 10.2) if it abandons the connection prior to the effective value" [1
§10.1]. A client that finds its connection gone after a quiet spell "can simply reconnect" [3 §3.2].

**HTTP leaves the value to each server, and asks it to free what it holds.** HTTP/1.1 "places no
requirements on the length (or existence) of this timeout for either the client or the server", and
"prompt closure of both sides of a connection enables allocated system resources to be reclaimed"
[6 §9.5]. HTTP/2's servers "are encouraged to maintain open connections for as long as possible but
are permitted to terminate idle connections if necessary" [5 §9.1]. HTTP/3's clients keep a
connection open "while there are responses outstanding", since "allowing an idle connection to time
out is preferred over expending effort maintaining a connection that might not be needed", and
"Servers SHOULD NOT actively keep connections open" [4 §5.1]. A connection's PINGs are for state:
an application may defer the idle timeout to "avoid losing state that has been associated with an
open connection" [1 §10.1.2], not "more frequently than once every 15 seconds" [8 §3.5], and
"timeouts shorter than 30 seconds can make it harder to handle transient network interruptions,
such as Virtual Machine (VM) migration" [3 §3.2]. A request on a connection that closed before any
answer is safe to send again on a new one where it is idempotent [7 §9.2.2]; "Connections can be
closed at any time, with or without intention" [6 §9.3.1].

## Every default found

"Read" is the wait for a byte with a request outstanding, "idle" the wait with none, "connect" a
dial and its handshake, "operation" the whole request with its retries.

| Who | Setting | Default | Bounds | Source |
|---|---|---|---|---|
| Amazon S3 | `RequestTimeout`, 400 | not stated | read, either way | [9] |
| Amazon S3 | an idle kept-alive connection | not stated in [9]–[15]; "~5 seconds" by AWS's CRT | idle | [25] `s3_client.c:92-94`; [21] #1122 |
| Amazon S3 | CompleteMultipartUpload | "several minutes", white space sent meanwhile | read, kept alive by the server | [10] |
| Amazon S3 | a multipart upload | "there is no expiry" | operation | [11] |
| Amazon S3 | a part; parts; an object | 5 MiB to 5 GiB; 10,000; 48.8 TiB | size, not time | [12] |
| Amazon S3 | a presigned URL | 7 days by the CLI or an SDK; 1 minute to 12 hours by the console | when a request may begin, checked at its start | [13] |
| Amazon S3 | connections to a bucket | no limit | count | [15] |
| MinIO | `--idle-timeout` | 30 s | idle | [27] `server-main.go:94-100`, `server.go:43-44` |
| MinIO | each accepted connection's deadline | 30 s, renewed at each read and write | read, write | [27] `listener.go:66-77`, `deadlineconn.go:92-109` |
| MinIO | `--read-header-timeout` | 30 s | a request's head | [27] `server-main.go:101-107` |
| MinIO | `--conn-user-timeout` | 10 min | data sent unacknowledged | [27] `server-main.go:108-114` |
| Ceph RGW | Beast's `request_timeout_ms` | 65,000 ms; 0 disables | read, write | [28] |
| mantle's S3 door, today | octet wait and quiet close | 60 s | read; idle | [29] `listener/conn.rs:42-47`, research/72 |
| AWS CLI v2 | `--cli-connect-timeout` | 60 s; 0 none; no configuration key | connect | [16], [17] |
| AWS CLI v2 | `--cli-read-timeout` | 60 s; 0 none; no configuration key | read | [16], [17] |
| AWS CLI v2 | `aws s3 presign --expires-in` | 3,600 s, at most 604,800 s | a presigned URL's life | [16] |
| AWS CLI v2 | `tcp_keepalive` | false | TCP keep-alive | [17] |
| AWS CLI v2 | `retry_mode`, `max_attempts` | standard, 3 | retries | [17] |
| botocore | `connect_timeout` | 60 s | connect | [18] |
| botocore | `read_timeout` | 60 s | read | [18] |
| botocore | `max_pool_connections` | 10 | pooled connections | [18] |
| botocore | `tcp_keepalive` | False | TCP keep-alive | [18] |
| botocore | an idle pooled connection | no setting | idle | [18] |
| boto3 | retries | legacy mode, 5 attempts | retries | [19] |
| boto3 | `TransferConfig` | 8 MiB threshold and part, 10 threads, 5 download attempts, no timeout of its own | transfers | [19] |
| AWS SDKs | `defaults_mode` | legacy | which defaults apply | [20] |
| AWS SDKs | `connectTimeoutInMillis` | 3,100 standard and cross-region, 1,100 in-region, 30,000 mobile | connect | [20] |
| AWS SDKs | `tlsNegotiationTimeoutInMillis` | as `connectTimeoutInMillis` | TLS handshake | [20] |
| AWS SDKs | `retry_mode`, `max_attempts` | standard, 3 | retries | [20] |
| Java SDK v2 | `connectionTimeout` | 2 s | connect | [21] `SdkHttpConfigurationOption.java:156` |
| Java SDK v2 | `tlsNegotiationTimeout` | 5 s; 10 s for the CRT clients | TLS handshake | [21] `:163`; `AwsCrtHttpClientBase.java:64-70` |
| Java SDK v2 | `socketTimeout` (Apache), `readTimeout` and `writeTimeout` (Netty) | 30 s; none in the CRT clients | read, write | [21] `:154-155`; `ApacheHttpClient.java:492-493`; [26] |
| Java SDK v2 | `connectionAcquisitionTimeout` | 10 s | a wait for a pooled connection | [21] `:157` |
| Java SDK v2 | `connectionMaxIdleTime` | 60 s (Apache, CRT); 5 s (Netty) | idle | [21] `:158`; `NettyNioAsyncHttpClient.java:80-85` |
| Java SDK v2 | `connectionTimeToLive` | 0, "an infinite amount of time" | a connection's life | [21] `:159` |
| Java SDK v2 | `maxConnections` | 50 | pooled connections | [21] `:165` |
| Java SDK v2 | `apiCallTimeout`, `apiCallAttemptTimeout` | none | operation; one attempt | [21] guide |
| Go SDK v2 | dialer `Timeout`, `KeepAlive` | 30 s, 30 s | connect; TCP keep-alive interval | [22] `client.go:48-49, 222-228` |
| Go SDK v2 | `TLSHandshakeTimeout` | 10 s | TLS handshake | [22] `client.go:26` |
| Go SDK v2 | `IdleConnTimeout` | 90 s | idle | [22] `client.go:25` |
| Go SDK v2 | `MaxIdleConns`, `MaxIdleConnsPerHost`, `MaxConnsPerHost` | 100, 10, 2,048 | pooled connections | [22] `client.go:20-22` |
| Go SDK v2 | `ExpectContinueTimeout` | 1 s | a wait for `100 Continue` | [22] `client.go:27` |
| Go SDK v2 | `ResponseHeaderTimeout` | unset, none | a response's head | [22] `client.go:240-262`; [23] `transport.go:219-223` |
| Go SDK v2 | `WithReadTimeout` | unset, "a default per service", none for S3 | read, a rolling window | [22] `client.go:163-221`, `timeouts.go:12-16`; [26] |
| Go SDK v2 | `WithTimeout`, `http.Client.Timeout` | none | operation | [22]; [23] `client.go:92-102` |
| Go net/http | `DefaultTransport` | dial 30 s with 30 s keep-alive, idle 90 s, TLS 10 s, 100 idle, 2 idle a host | connect, idle, pool | [23] `transport.go:47-62` |
| Go net | `Dialer.Timeout`, `KeepAlive` left zero | none ("TCP timeouts are often around 3 minutes"); probes every 15 s | connect; TCP keep-alive | [23] `dial.go:127-139, 175-185` |
| Rust SDK | connect timeout | 3.1 s | connect | [24] guide; `defaults.rs:37-38` |
| Rust SDK | operation, attempt and read timeouts | none | operation; attempt; first byte | [24] guide |
| Rust SDK | stalled stream protection | on both ways, 1 byte a second, 20 s grace | read | [24] guide; `stalled_stream_protection.rs:16-20` |
| CRT S3 client | `connect_timeout_ms` | 3,000 ms | connect | [25] `s3_endpoint.c:25, 159` |
| CRT S3 client | `monitoring_options` | 1 byte a second over 30 s | read | [25] `s3_client.c:85-87, 823-829` |
| CRT S3 client | an idle pooled connection | no setting | idle | [25] `s3_endpoint.c:177-190`; `connection_manager.h:131-135` |
| CRT S3 client | an upload part's first byte | learned, from max(1 s, p90), off past 5 s | a part's answer | [25] `s3_client.c:3611-3657` |
| CRT S3 client | retries; TCP keep-alive | 5; off | retries; keep-alive | [25] `s3_client.c:82`; `s3_endpoint.c:163-168` |
| AWS SDKs, 2026 opt-in | default socket timeout | 5 min; 15 min for long holds; none for streaming bodies; S3 exempt | read, a rolling window | [26] |
| QUIC | `max_idle_timeout` | none unless advertised; the lesser of both; at least 3 PTOs | idle | [1] §10.1, §18.2 |
| hyper-quic (quinn's) | `max_idle_timeout`, `keep_alive_interval` | 30 s; none | idle; keep-alive | [29] `vendor/hyper-quic/src/config/transport.rs:410, 428` |
| HTTP/1.1 | an inactive connection | no requirement | idle | [6] §9.5 |
| HTTP/2 | an idle connection | kept "as long as possible", may be closed | idle | [5] §9.1 |
| HTTP/3 | an idle connection | QUIC's; kept open only with responses outstanding | idle | [4] §5.1 |
| mantle, today | `idle_timeout()`, `keep_alive`, `serve_period`, `STOCK_PATIENCE` | 3.072 s, 1.536 s, 3.072 s, 60 s | idle, keep-alive, a served wait, read | [29] `bounds.rs:27-31, 80-83, 211-215` |

## The design

Three bounds, each set by whoever owns the wait it ends, each with S3's default where nobody sets
it. Nothing of it is built.

**B1. Who sets what.** The owner named two places, the user's side per invocation and a node's
configuration file.

- Every object command of the `mantle` CLI (`mb`, `rb`, `ls`, `cp`, `rm`, `head`, `multipart ...`)
  takes three global options beside `--client` (`crates/mantle/src/main.rs:45-52`):
  `--connect-timeout SECONDS`, `--read-timeout SECONDS` and `--operation-timeout SECONDS`. The first
  two are the AWS CLI's `--cli-connect-timeout` and `--cli-read-timeout` [16] under mantle's names,
  the third Java's `apiCallTimeout` and the Rust SDK's operation timeout [21, 24]. They replace the
  CLI's `NO_DEADLINE` and its "a dial by its handshake's idle timeout"
  (`crates/mantle/src/client.rs:358-360`).
- The native client's `Settings` (`crates/client/src/lib.rs:105-121`) gains `connect_timeout` and
  `read_timeout`; an operation's bound stays each call's `deadline_ns`, as now.
- A node's file gains `link.connect_timeout_ms` and `link.read_timeout_ms`, operator statements
  `mantle serve` takes (`crates/node/src/process.rs:548`) and keys `mantle node` takes
  (`process.rs:474`), each said with its basis as every value is (`basis link.read_timeout_ms =
  60000 (cited: ...)`, node.md §1.4). A node states no operation timeout: an operation is its
  client's.

**B2. The defaults, and why each is S3's analogue.**

| Bound | Unset | Copied from |
|---|---|---|
| Read: a request's silence, either way | 60 s | botocore's `read_timeout`, the AWS CLI's `--cli-read-timeout` [16, 18]; D2's patience |
| Idle: a link that hears nothing | 60 s, the read timeout | the same; mantle's S3 door's quiet close (research/72) |
| Connect: a dial and its handshake | 3.1 s | the AWS SDKs' standard `connectTimeoutInMillis` [20]; the Rust SDK's [24] |
| Operation: the whole request | none | every stock client [16, 18, 21, 22, 24]; S3 itself [11, 13] |

*Read, 60 s.* It is the wait for a byte that botocore and the AWS CLI keep [16, 18], the clients
research/72 sized the S3 door for, and the longest any stock client sets (Java's and the CRT's 30 s,
Rust's 20 s grace [21, 24, 25]; Go and Java's CRT HTTP clients set none for S3 [22, 26]), so a node
that waits this long never stops waiting before a stock client that bounds its wait does, which is
D2's rule, "wait as long as the client would" (research/77). A client that sets no bound is waited
on this long and no longer: a node's work for a client that has gone is what the owner's "block
other work" names. S3's `RequestTimeout` [9] is its mirror on the server's side, which Ceph sets at
65 s and MinIO at 30 s [27, 28].

*Idle, 60 s, the same value.* QUIC has one idle parameter, and it bounds both a link with nothing
outstanding and a link whose peer went silent mid-request. Below the read timeout it would end a
link under a request still waited for, which is research/81's failure (holds of 4 s and 5 s ended
reads at 3.072 s, research/81 [25]); S3's own few seconds [25] and Netty's 5 s [21] would repeat it.
Netty's 5 s answers HTTP/1.1's race, a client that cannot know when the server will close having to
close first [21, #1122; 6 §9.5]; QUIC has no such race, each end knowing the agreed value [1 §10.1,
§7.4]. Sixty seconds is what mantle's S3 door already lets a quiet connection keep its place
(`listener/conn.rs:42-47`, research/72), what Java's Apache and CRT clients keep an idle connection
[21], and above the 30 s below which RFC 9308 finds transient interruptions hard to survive [3
§3.2]. What an idle link's 60 s costs the node's places is B5's to bound.

*Connect, 3.1 s.* AWS's current recommendation, the SDKs' standard mode and the default of their
next major versions [20], and the Rust SDK's [24]; the CRT's 3 s and Java's 2 s are shorter, Go's
30 s and the AWS CLI's and botocore's 60 s longer [16, 18, 21, 22, 25]. A client of S3 has one
endpoint to wait for; a mantle client has every node its configuration names (`Settings::nodes`),
and a dial that ends loses nothing (research/81 L1), so a short connect moves it to another node
rather than holding an operation on a node that is down. For QUIC it admits the first flight and two
probes: with `kInitialRtt` of 333 ms and its variation half that [2 §5.3, §6.2.2], and no
`max_ack_delay` in the handshake's spaces [2 §6.2.1], the first probe goes at 0.999 s and, doubled,
the second at 2.997 s (derived), leaving 103 ms for its answer.

*Operation, none.* No stock client bounds an operation unless asked [16, 18, 21, 22, 23, 24], and S3
bounds none: a multipart upload has no expiry [11] and a presigned download outlives its URL [13].
An operation without one is still bounded: by its read timeout between movements, and by its counted
attempts and retry quota (`crates/client/src/lib.rs:75-93`), so no wait is unbounded (CLAUDE.md
§2). An operation that keeps moving is doing the work its caller asked for, as a 48.8 TiB object's
upload does for hours.

**B3. What the wire carries.** Each end's read timeout rides as its own `max_idle_timeout` [1
§18.2], a declaration made "unilaterally" [1 §7.4]; both ends hold the connection to the lesser [1
§10.1] and both can read it once the handshake completes [1 §7.4], as RFC 9308 asks [3 §10]. So a
node learns how long each client waits with no new field: a request's patience on a node is the
effective value of the connection it came on, the client's own where it is shorter and the node's
where the client asked for more. That is D2's "as long as the client would", with the operator's
statement as its ceiling. QUIC raises any value to three probe timeouts [1 §10.1], which hyper-quic
already does (`vendor/hyper-quic/src/connection/mod.rs:3136`). An operation's deadline rides as D2's
time left in the request's head (`crates/protocol/src/object.rs:14`, `object.rs:872-885`), as now,
and a chunk a node asks of a peer for it carries what remains (research/77 D2). The connect timeout
is the dialer's alone and nothing carries it: a listener's handshakes in progress are bounded by
admission's pending places (`vendor/hyper-transport/src/admission.rs:4-7`). On the S3 door HTTP
carries neither timeout, so the node applies its own read timeout, whose default is the stock
clients' (B2).

**B4. Where a node applies them.**

- *A native request.* A job ends at its deadline or at its connection's effective read timeout past
  its work's last movement, the earlier (`crates/node/src/node/host.rs:110-117`, today
  `STOCK_PATIENCE`); it is vouched for each beat, a third of that timeout (research/77 D1); a peer's
  held chunk is vouched for within what its head carried and never past the read timeout of the link
  it came on (`node.rs:4145-4154`).
- *The S3 door.* The wait for a client's next octet, between requests and inside one, is
  `link.read_timeout_ms` (`listener/conn.rs:42-47`).
- *A peer.* Each node advertises `link.read_timeout_ms` to its peers too, and a link between two
  nodes holds the lesser. A node's asks of a peer for a request are judged within the request's time
  left and the read timeout of the connection the request came on (research/81 L4;
  `node.rs:2385, 3182, 3253, 3475, 3852`); a peer silent past the link's own agreed timeout ends the
  link, and the ask with it.
- *A dial.* A node's dials of its peers end at `link.connect_timeout_ms`, then back off as
  research/81 L6 sets; a client's end at its `connect_timeout` and it asks the next node within its
  attempts (research/73).

**B5. An idle link must not keep a newcomer out.** At 60 s rather than 3.072 s an idle client's
connection keeps its place twenty times longer. Today a client connecting past `gateway.clients` is
closed with `Refusal::Identities` (`crates/node/src/node.rs:1883-1893`; research/73), whatever the
clients already held are doing, so a crowd of idle clients would keep a new one out for up to a
minute: the owner's "block other work". The rule instead: a client past the bound takes the place
of the client whose every connection has carried nothing for longest, which is closed with
CONNECTION_CLOSE [1 §10.2], as RFC 9000 asks of an end that gives up a connection before its
agreed value [1 §10.1]; only with no client idle is the newcomer refused, as now. Idle is
hyper-transport's own sense, no exchange under way and nothing begun, ended or carried for its
quiet period (`vendor/hyper-transport/src/admission.rs:9-16`), so no request is cut. It is an
eviction by a stated rule (CLAUDE.md §2), and it frees a place when someone needs it, not on a
timer; the displaced client dials again for its next request [3 §3.2]. RFC 9308's other way,
advertising shorter timeouts to new connections as places fill [3 §10], frees nothing already held.
The S3 door keeps its own rule, its budget, its reserve and its `503 SlowDown`
(`crates/node/src/listener/mod.rs:44, 52`, research/72): the AWS CLI took a part whose connection
closed without a response for done (research/72 [7]), so the door does not close a kept-alive
connection a client may be about to use, except at its read timeout as now. Sockets follow
connections: a client holds one a connection where its platform gives each its own
(`crates/client/src/lib.rs:322-334`), so the places bound them.

**B6. Keep-alives by what a link holds.** A client's link with nothing outstanding sends none and
ends at the agreed timeout [4 §5.1, 3 §3.2]: that is the idle bound. A client's link with a request
outstanding PINGs after a third of the agreed timeout of quiet, or research/81 L5's middlebox
lifetime if shorter, never more often than every 15 s [8 §3.5], so a node that works without
sending keeps its link by acknowledging [1 §10.1.2]. A link to a peer PINGs the same way whatever it
holds, since the datagram plane's keys come from it (`node.rs:2106-2119`), "state that has been
associated with an open connection" [1 §10.1.2]: a live peer's quiet link survives and a silent
peer's ends at the timeout. A third leaves margin for two lost, as research/77 D1 sets the notes'
beat; at the default it is 20 s (derived), inside the 30 s middlebox lifetime research/81 L5 starts
from. This replaces `keep_alive`'s 1.536 s (`bounds.rs:213-215`), and for peers research/81 L5's
"A connection with nothing outstanding sends no keep-alive".

**B7. What is refused.** A node's read or connect timeout of 0 is refused when its file is read, and
`Limits::validate` refuses an idle timeout of 0 already
(`vendor/hyper-transport/src/endpoint.rs:179-195`): "We do need a bound". A client's 0 is refused as
a usage error unless the owner prefers the AWS CLI's meaning (question 6).

## Where the bound replaces 3.072 s

Research/79's inventory rows that are 3.072 s or follow from it, at `68c7b831` (node.rs's lines moved
by `68c7b831` since research/81 cited `e718cddb`; the other files did not change). Rows 1 to 5, 18,
19, 21 to 24, 28, 30 to 32, 34, 35 and 37 to 49 keep research/81 L8's bases.

| Row | Where | Today | With the bound |
|---|---|---|---|
| 6 | `crates/protocol/src/bounds.rs:80-83`, used at `:188, 212`; `vendor/hyper-transport/src/endpoint.rs:207, 220`; `vendor/hyper-quic/src/transport_parameters.rs:162` | 3.072 s, every endpoint's `max_idle_timeout` | the end's read timeout, stated in `bounds::Stated` (`bounds.rs:38-57`) by the node's file or the client's `Settings` |
| 7 | `bounds.rs:211`; `endpoint.rs:935-936` | 3.072 s, one serve period and quiet period an endpoint | the serve-side wait: each connection's effective read timeout (B4); admission's quiet period: research/81 question 6 |
| 8 | `bounds.rs:213-215` | PINGs every 1.536 s | B6 |
| 9 | `bounds.rs:85-89` | doubling capped at 3.072 s | research/81 L6, RFC 8085's rate |
| 10 | `bounds.rs:27-31`; `crates/node/src/node/host.rs:110-117`; `crates/node/src/node.rs:4145-4154`; `crates/client/src/lib.rs:1776-1778`; `crates/node/src/listener/conn.rs:42-47` | 60 s fixed | the connection's effective read timeout (native door), `link.read_timeout_ms` (S3 door), the client's `read_timeout`; `STOCK_PATIENCE` stays as their default |
| 11 | `node.rs:2385, 3182, 3253, 3475, 3852` | a node's asks judged at 3.072 s | the request's time left and its connection's read timeout (B4) |
| 12 | `node.rs:1583-1585` | the turn sleeps at most 3.072 s | the earliest deadline the node holds (research/81 L8) |
| 13 | `node.rs:1567-1570`; `host.rs:2957-2960` | a vouch each 1.024 s | a third of the asker's read timeout |
| 14 | `node.rs:1235-1240` | 3.072 s plus the backoff | the link's agreed timeout plus the backoff |
| 15 | `node.rs:3704-3715` | 3.072 s times the nodes | the request's patience (research/81 L6) |
| 16 | `crates/node/src/names.rs:64, 72, 137, 148` | doubled to 3.072 s | research/81 L6 |
| 17 | `crates/node/src/process.rs:1890-1899` | half the serve period | the notes' beat |
| 20 | `crates/node/src/client.rs:538-553` | at least 3.072 s | at least the agreed timeout |
| 25 | `crates/client/src/lib.rs:1782` | at least 3.072 s | the connection's agreed timeout |
| 26 | `endpoint.rs:179-195` | refuses 0 | kept (B7) |
| 27 | `endpoint.rs:344-347, 4116-4122` | three notes a 3.072 s period | three a period of the connection's agreed timeout, 20 s apart at the default |
| 29 | `vendor/hyper-transport/src/charge.rs:97-113`; `endpoint.rs:2053, 2065, 2089` | room for 26 probes (derived) | room for 34 at 60 s (derived), at the endpoint's own timeout, the most any of its connections agrees |
| 33 | `vendor/hyper-quic/src/config/transport.rs:410` | quinn's 30 s, overwritten by row 6 | overwritten by the stated timeout |
| 36 | `vendor/hyper-quic/src/connection/mod.rs:409-412, 3127-3142, 5210-5211, 6012-6019` | idle timer at the larger of 3.072 s and three PTOs; a handshake bounded by the local value | the larger of the agreed timeout and three PTOs [1 §10.1]; a handshake bounded by the connect timeout, apart from the idle timer |
| none | `crates/mantle/src/client.rs:358-360` | `NO_DEADLINE`, "a dial by its handshake's idle timeout" | the CLI's three options (B1) |
| none | `docs/design/node.md:1623-1630, 1663-1667, 1676` | "derived from the path, never fixed"; a stalled upload checkpointed at "the connection's idle timeout"; backoffs "capped by the idle timeout" | the stated timeout, agreed by QUIC; the read timeout; research/81 L6 |
| tests | `crates/mantle/tests/native.rs:1066`; `crates/mantle/tests/support/cell.rs:2023`; `crates/node/tests/cluster.rs:1653`; `crates/node/src/names.rs:242-244`; `crates/client/tests/support/mod.rs:320`; `crates/client/tests/progress.rs:37, 182, 233, 256` | read `bounds::idle_timeout()` | read the timeout each test states |

Row 29's counts: probes_within doubles from 1 ms to the timeout and allows two a doubling and two
more, 12 doublings to 3.072 s and 16 to 60 s (`charge.rs:100-113`; derived).

The vendored part is hyper-raft's: expose each connection's agreed idle timeout, which `PathFacts`
does not hold (`endpoint.rs:250-282`) [3 §10]; take the serve period, its judgments and the notes'
beat from it a connection at a time rather than one an endpoint (`endpoint.rs:935-936, 4116-4122`);
bound a dial by a connect timeout apart from the idle timer (`connection/mod.rs:409-412`;
research/81 L1); and send keep-alives by what a link holds (B6), where `keep_alive` is one interval
an endpoint (`endpoint.rs:221`). mantle's part is `bounds.rs`, the node's file and `derive`, the
client's `Settings`, the CLI, and B5's rule at `node.rs:1883-1893`, which needs each connection's
idle age from the transport.

## With research/81

The owner's answer settles research/81's question 1: a link to a peer that never comes back is
closed by time, at the stated timeout. The rest of its design stands with these changes.

- **L1.** Every end advertises its read timeout, not 0. A link still ends at once on evidence, the
  peer's close, a stateless reset, a newer connection of the same identity, the liveness stream's
  `Restarted` or `Stopped`, the cell's membership, the transport's budget, the owner's own close;
  and now also after the agreed timeout heard nothing [1 §10.1]. The bound is the last of these,
  the one that frees what evidence never reports. L1's dial bound is the connect timeout (B2), not
  persistent congestion's three probe timeouts.
- **L2.** Unchanged. Suspicion moves work early: elections start and a suspected peer's chunk reads
  end at once so they go to other copies (`node.rs:1855-1859`), at the freshness points the detector
  learns (research/81 [24] §2.8), not at the timeout. Suspicion closes nothing, so a peer held for
  less than the timeout keeps its links and the plane's keys and is heard at its first heartbeat.
- **L3.** Unchanged, and the bound is one of the judgments it protects: the link's timeout judges the
  peer's silence, so a resumed node reads its sockets before its timers fire and ends no link whose
  peer's packets wait there. A node held past the timeout finds its peers have discarded the
  connection; its first packet on it is answered with a stateless reset (hyper-quic's endpoint does
  so for a packet on a connection it does not know, `vendor/hyper-quic/src/endpoint.rs:486-498`;
  [1 §10.3]), which ends the link at once, and it dials again.
- **L4.** The patience a trusted link's exchange waits to is the connection's agreed read timeout,
  not a fixed 60 s. Hedging is unchanged: "latency decides hedging, never liveness" (research/77 D3).
- **L5.** B6 replaces it for peer links and for client links with a request outstanding.
- **L6, L7, L8.** Unchanged, except that L7's first bounds are now the stated or default timeouts,
  that rows 6, 8, 20, 25 and 26, which L8 removed, take B1, B3, B6 and B7 instead, and that row 10
  becomes each connection's agreed timeout.

A peer held still by its host, with the defaults on every node and client:

| | Held 30 s | Held 90 s | Held for good |
|---|---|---|---|
| Its peers' detectors | Suspect it within their learned bounds and move its work (L2); trust it at its first heartbeat after resume | The same | Suspect it for good |
| Its QUIC links | Kept: below the timeout | End `TimedOut` 60 s after each peer's idle timer last restarted, its last packet heard or the peer's first PING after it, so 60 s to 80 s into the hold [1 §10.1] (derived, B6's 20 s); on resume its first packets meet stateless resets and it dials again (L3) | End the same way; dialed at L6's rate until the cell removes it |
| The plane's keys | Kept | Dropped with the links (`node.rs:2106-2119`), made again with the new ones | Dropped with the links |
| A client's read through it, no deadline | Answered after resume, or by another node past the hedge point (L4) | Its exchange ends at its read timeout, 60 s of silence, and is asked of another node within its attempts, safe for a get and for a put carrying its identity (research/77 D4) | The same |
| A client's read with deadline `d` | Answered if `d` covers the hold | Ends at the earlier of `d` and 60 s of silence; asked of another node within its attempts if `d` has not passed | The same |
| What it costs its peers | Its connections' bytes (`bounds::beside`, `bounds.rs:340-381`) | The same, until the links end | The same until the links end, then a dialer's pause at L6's rate |

## What to measure and test before any code changes

The measurements decide values the sources leave open and come first; the tests are what the change
must pass. Each runs real processes on real sockets, waits on facts, and asserts exactly; a timeout
a test states is its input, never a guess at the host (CLAUDE.md §3).

- **M1. S3's own bounds.** AWS states neither its idle close nor its `RequestTimeout` period
  (above). Against a bucket in one region, from the box and from this Mac: a TLS connection kept
  after one HEAD, timed from the response's last byte to S3's close; a PUT that states its length
  and stops after its head, timed to `400 RequestTimeout` or the close; a GET whose client stops
  reading. Thirty of each, the count beside each median and tail. It needs an AWS account
  (question 8).
- **M2. The stock clients against the S3 door.** research/72 [7]'s fixture stalled mid-body:
  botocore, the AWS CLI's classic client and its CRT client, which the CLI's `auto` takes for an
  upload or a download when no other CLI process holds it [16], each timed from the stall to its
  error at its defaults and with
  `--cli-read-timeout 5`, which says whether the CRT path takes the option and what each client does
  next.
- **M3. Holds against the default.** research/81 M1's distribution of the holds hosts impose, on the
  box under the trace's loads, on this Mac and in the KIND cell: how many pass 60 s. Each is a link
  the default ends.
- **M4. What an idle link costs, and what ending one costs.** A connection's bytes beside the budget
  (`bounds::beside`, measured by `crates/node/tests/transport_memory.rs`) and its socket on each
  platform (`crates/client/src/lib.rs:322-334`); and the handshake a client pays to come back after
  the timeout, on loopback, between the box and this Mac, in the KIND cell and over a 500 ms path.
  These are the two sides of RFC 9308 §3.2's trade, measured rather than assumed.
- **M5. A crowd of idle clients.** Clients past a node's `gateway.clients`, each idle between
  requests: the time a newcomer waits today, at 60 s without B5, and with B5.
- **T1. Agreement.** A client stating X and a node stating Y each read the lesser after the
  handshake, for X below, at and above Y; a link that hears nothing ends `TimedOut` on each end that
  long after its idle timer last restarted by RFC 9000's rule [1 §10.1], and not before.
- **T2. A CLI timeout reaches the node.** `mantle cp` with `--read-timeout 5` to a node whose volume
  holds the put's chunk past it, held by the test until the node's counters say so: the CLI ends
  with the read timeout's typed error at or after 5 s and never before, and the node's job ends
  `503 SlowDown` at the same bound by its counters, not at 60 s.
- **T3. The node's file is a ceiling.** `link.read_timeout_ms` below the client's: the client's
  exchanges end at the node's value; the S3 door lets a quiet connection go at it.
- **T4. Holds across the timeout.** A node stopped with SIGSTOP and released on facts: held for less
  than the timeout, no link of it ends (the links' `dials` and `ended` counters unchanged,
  `node.rs:512-517`) and every read returns its object's exact bytes; held past it, every link to it
  ends at the timeout, the plane drops its keys, its first packets on resume meet stateless resets,
  its links are dialed again and its ranges catch up.
- **T5. Connect.** A dial to an address with no listener ends `Unreachable` at the connect timeout
  and not before, and the client asks the next node; with none reachable the request ends `Untaken`
  after `ATTEMPTS` (`crates/client/src/lib.rs:77`).
- **T6. Operation.** In hyper-sim's deterministic world, a put given an operation timeout ends at it
  while its bytes move; a put given none, its bytes moving each time just inside the read timeout, is
  never cut; one whose bytes stop is cut at the read timeout exactly.
- **T7. Eviction.** At `gateway.clients` with every client idle, a newcomer is admitted at once and
  the client idle longest is closed with CONNECTION_CLOSE; a client with an exchange under way is
  never displaced; the displaced client's next request dials again and is answered.
- **T8. Keep-alives.** A quiet peer link PINGs at a third of its agreed timeout, never more than once
  in 15 s, and survives; a client link with nothing outstanding sends nothing and ends at the
  timeout; one with a request outstanding stays while the node acknowledges.

## Open questions for the owner

1. Is 60 s the right default for how long a link may hear nothing? It is the stock S3 clients' read
   timeout and what the S3 door already waits, against S3's own few seconds or MinIO's 30 s.
2. Should a dial give up after 3.1 s, AWS's current SDK default, so a client moves to another node
   quickly, or after 60 s as the AWS CLI does?
3. Should a node's setting be a ceiling that a client asking for longer cannot pass?
4. Should an operation have a whole-operation timeout only when the user gives one, as in every AWS
   SDK and in S3 itself, a stall being ended by the read timeout?
5. When a node's client places are full, should it close the client that has been idle longest to
   let a new one in, rather than turn the new one away until an idle one times out?
6. Should a client's timeout of 0 mean "no limit of my own, the node's applies", as the AWS CLI's 0
   means no timeout, or be refused?
7. Should a client directory's `config` also hold these timeouts, so every command an agent runs
   uses them without flags?
8. May we measure real S3's idle close and request timeout with an AWS account, since AWS documents
   neither?
9. The transport's part, exposing the agreed timeout, a serve period and notes' beat for each
   connection, a dial bound of its own and keep-alives by what a link holds, is in hyper-transport
   and hyper-quic. Should it go to hyper-raft's agent, with Quiver reviewing as for D2?

## Sources

[1] J. Iyengar and M. Thomson (eds.), RFC 9000, "QUIC: A UDP-Based Multiplexed and Secure
Transport", May 2021, https://www.rfc-editor.org/rfc/rfc9000.txt, §7.4, §10.1, §10.1.2, §10.2,
§10.3, §18.2.

[2] J. Iyengar and I. Swett (eds.), RFC 9002, "QUIC Loss Detection and Congestion Control", May 2021,
https://www.rfc-editor.org/rfc/rfc9002.txt, §5.3, §6.2.1, §6.2.2.

[3] M. Kühlewind and B. Trammell, RFC 9308, "Applicability of the QUIC Transport Protocol",
September 2022, https://www.rfc-editor.org/rfc/rfc9308.txt, §3.2, §10. The resource cost of a held
connection is in §10, not §3.2.

[4] M. Bishop (ed.), RFC 9114, "HTTP/3", June 2022, https://www.rfc-editor.org/rfc/rfc9114.txt,
§5.1.

[5] M. Thomson and C. Benfield (eds.), RFC 9113, "HTTP/2", June 2022,
https://www.rfc-editor.org/rfc/rfc9113.txt, §9.1.

[6] R. Fielding, M. Nottingham and J. Reschke (eds.), RFC 9112, "HTTP/1.1", June 2022,
https://www.rfc-editor.org/rfc/rfc9112.txt, §9.3.1, §9.5.

[7] R. Fielding, M. Nottingham and J. Reschke (eds.), RFC 9110, "HTTP Semantics", June 2022,
https://www.rfc-editor.org/rfc/rfc9110.txt, §9.2.2.

[8] L. Eggert, G. Fairhurst and G. Shepherd, RFC 8085 (BCP 145), "UDP Usage Guidelines", March 2017,
https://www.rfc-editor.org/rfc/rfc8085.txt, §3.5.

[9] Amazon S3 API Reference, "Error", the list of error codes,
https://docs.aws.amazon.com/AmazonS3/latest/API/API_Error.html: `RequestTimeout`. The reference's
`sigv4-query-string-auth.html` and `sig-v4-authenticating-requests.html` served its index page on
2026-11-07 and were not read.

[10] Amazon S3 API Reference, "CompleteMultipartUpload",
https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html.

[11] Amazon S3 User Guide, "Uploading and copying objects using multipart upload in Amazon S3",
https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html: "Pause and resume object
uploads"; "Multipart upload and pricing".

[12] Amazon S3 User Guide, "Amazon S3 multipart upload limits",
https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html.

[13] Amazon S3 User Guide, "Download and upload objects with presigned URLs",
https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html: "Who can create a
presigned URL"; "Expiration time for presigned URLs".

[14] Amazon S3 User Guide, "Performance design patterns for Amazon S3",
https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-design-patterns.html:
"Timeouts and retries for latency-sensitive applications"; "Horizontal scaling and request
parallelization for high throughput".

[15] Amazon S3 User Guide, "Performance guidelines for Amazon S3",
https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-guidelines.html: "Scale
storage connections horizontally"; "Retry requests for latency-sensitive applications".

[16] AWS CLI 2.39.6 Command Reference: the global options, `--cli-read-timeout` and
`--cli-connect-timeout`, https://docs.aws.amazon.com/cli/latest/reference/index.html; `aws s3
presign`, `--expires-in`, https://docs.aws.amazon.com/cli/latest/reference/s3/presign.html; and "AWS
CLI S3 Configuration", `preferred_transfer_client`,
https://docs.aws.amazon.com/cli/latest/topic/s3-config.html.

[17] AWS CLI User Guide, "Configuration and credential file settings in the AWS CLI",
https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html: `max_attempts`,
`retry_mode`, `tcp_keepalive`, and no key for either timeout; and "AWS CLI retries in the AWS CLI",
https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-retries.html: "Standard retry mode
(default)".

[18] botocore 1.43.126, "Config Reference",
https://docs.aws.amazon.com/botocore/latest/reference/config.html: `connect_timeout`, `read_timeout`,
`max_pool_connections`, `retries`, `tcp_keepalive`.

[19] Boto3 1.43.126, "S3 customization reference", `boto3.s3.transfer.TransferConfig` and its
`DEFAULTS`, https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html;
and "Retries", "Legacy retry mode",
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html.

[20] AWS SDKs and Tools Reference Guide, "Smart configuration defaults",
https://docs.aws.amazon.com/sdkref/latest/guide/feature-smart-config-defaults.html; and "Retry
behavior", https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html, which on
2026-11-07 describes the 2026 behavior behind the opt-in `AWS_NEW_RETRIES_2026`.

[21] AWS SDK for Java 2.x, github.com/aws/aws-sdk-java-v2 at master `220e5a84`:
`http-client-spi/src/main/java/software/amazon/awssdk/http/SdkHttpConfigurationOption.java` (lines
cited, `GLOBAL_HTTP_DEFAULTS`), `http-clients/netty-nio-client/.../NettyNioAsyncHttpClient.java`,
`http-clients/apache-client/.../ApacheHttpClient.java`,
`http-clients/aws-crt-client/.../AwsCrtHttpClientBase.java`; the developer guide's "Configure
timeouts in AWS SDK for Java 2.x",
https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/timeouts.html; and issue #1122,
"Rapid S3 downloads causes failure", https://github.com/aws/aws-sdk-java-v2/issues/1122, the SDK
team's comments of 2019-04-01, 2019-05-28 and 2019-10-04.

[22] AWS SDK for Go v2, github.com/aws/aws-sdk-go-v2 at main `a65f5fcc`:
`aws/transport/http/client.go`, `aws/transport/http/timeouts.go`; and the developer guide's "Retries
and Timeouts",
https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-retries-timeouts.html.

[23] The Go standard library, github.com/golang/go at master `dcc06a82`: `src/net/http/transport.go`
(`DefaultTransport`, `DefaultMaxIdleConnsPerHost`, the `Transport` fields), `src/net/http/client.go`
(`Client.Timeout`), `src/net/dial.go` (`Dialer.Timeout`, `Dialer.KeepAlive`).

[24] AWS SDK for Rust, "Configuring timeouts in the AWS SDK for Rust",
https://docs.aws.amazon.com/sdk-for-rust/latest/dg/timeouts.html; and smithy-rs,
github.com/smithy-lang/smithy-rs at main `9e483217`:
`rust-runtime/aws-smithy-runtime/src/client/defaults.rs` (`DEFAULT_CONNECT_TIMEOUT`, "for all clients
with BehaviorVersion >= v2026_01_12") and
`rust-runtime/aws-smithy-runtime-api/src/client/stalled_stream_protection.rs`
(`DEFAULT_GRACE_PERIOD`).

[25] AWS Common Runtime: aws-c-s3, github.com/awslabs/aws-c-s3 at main `a2e3eb99`,
`source/s3_client.c`, `source/s3_endpoint.c`, `include/aws/s3/s3_client.h`; aws-c-http,
github.com/awslabs/aws-c-http at main `edac7e31`, `include/aws/http/connection_manager.h`
(`max_connection_idle_in_milliseconds`) and `include/aws/http/connection.h`
(`aws_http_connection_monitoring_options`).

[26] AWS's SDK teams on the 2026 default socket timeout: "Announcement: Opt-in read/write timeout for
the AWS CRT-based HTTP client (2026)", https://github.com/aws/aws-sdk-java-v2/issues/7385
(2026-09-18), and its merge, https://github.com/aws/aws-sdk-java-v2/pull/7336 (merged 2026-09-17);
"Announcement: Opt-in default socket timeout for the AWS SDK for JavaScript v3 (2026)",
https://github.com/aws/aws-sdk-js-v3/issues/8404 (2026-11-04); "Add a per-service default read
timeout for AWS SDK clients", https://github.com/smithy-lang/smithy-rs/pull/5004 (open,
2026-11-06).

[27] MinIO, github.com/minio/minio at master `7aac2a2c`: `cmd/server-main.go` (`--idle-timeout`,
`--read-header-timeout`, `--conn-user-timeout`; the server built at lines 900-907),
`internal/http/server.go` (`DefaultIdleTimeout`, `DefaultReadHeaderTimeout`),
`internal/http/listener.go` (each accepted connection wrapped with read and write deadlines of the
idle timeout), `internal/deadlineconn/deadlineconn.go` (a deadline set at each read and write).

[28] Ceph documentation, "HTTP Frontends", Beast's options,
https://docs.ceph.com/en/latest/radosgw/frontends/: `request_timeout_ms`.

[29] mantle: `docs/research/72-s3-listener-connections.md`, `73-native-client.md`,
`77-progress-and-patience.md` and `79-learned-link-liveness.md`, and the code at `68c7b831`, the
vendored crates' at the snapshot their `SNAPSHOT` files name, cited by file and line above.
