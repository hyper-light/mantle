# 72. The S3 listener's connections: how many stock clients open, and how one is closed

Status: research note, 2026-10-25; [7] and the answer past the budget, 2026-11-02. For the S3
listener's connection budget, what it answers past it, how long it waits on a quiet client, and
its closing of a connection whose request it refused before reading its body (node.md §4.1, §4.5;
`crates/node/src/listener`). Sources [1] to [6] were read from documents and source code, each
quoted where it was read; [7] was measured.

## The question

The listener holds a bounded number of connections at once (CLAUDE.md §2). Four things follow.
How many connections a stock client opens to one endpoint by default, so a cell's floor serves
one such client. What a connection past the bound is told, so a stock client takes it as the
refusal it is. How long a connection is held while its client says nothing, so idle ones do not
keep every slot. And how a connection is closed once a response says `Connection: close` while
its client may still be sending the body the listener did not read, so the client reads that
response rather than losing it to a reset.

## Sources

[1] AWS CLI Command Reference, "AWS CLI S3 Configuration",
https://docs.aws.amazon.com/cli/latest/topic/s3-config.html. `max_concurrent_requests`: "Default -
`10`"; "The `aws s3` transfer commands are multithreaded. At any given time, multiple requests to
Amazon S3 are in flight." "The `max_concurrent_requests` specifies the maximum number of transfer
commands that are allowed at any given time." `preferred_transfer_client`: "Default - `auto`";
`crt`: "Use the AWS Common Runtime (CRT) transfer client when possible." `target_bandwidth`:
"Default - Automatically derived based on system"; with `crt` the CLI "uses the greater of the
detected maximum network bandwidth and ten gigabits per second", and resolved through `auto` "the
detected maximum network bandwidth, or four gigabits per second ... if it is unable to detect it".

[2] botocore, "Config Reference", https://docs.aws.amazon.com/botocore/latest/reference/config.html.
"max_pool_connections (int) – The maximum number of connections to keep in a connection pool. If
this value is not set, the default value of 10 is used." boto3's clients and the AWS CLI's classic
transfer client are botocore's.

[3] aws-c-s3, `source/s3_client.c`, https://github.com/awslabs/aws-c-s3/blob/main/source/s3_client.c
(read 2026-10-25), the CRT transfer client's connection count: "num-connections-max =
throughput-target-gbps / s_throughput_per_connection_gbps"; `static const double
s_throughput_per_connection_gbps = 100.0 / 250;`; "After throughput math, clamp the min/max number
of connections": `g_min_num_connections = 10; /* Magic value based on: 10 was old behavior */` and
`g_max_num_connections = 10000;`, the count rounded up (`ceil`).

[4] RFC 9112, https://www.rfc-editor.org/rfc/rfc9112, §9.6: "If a server performs an immediate close
of a TCP connection, there is a significant risk that the client will not be able to read the last
HTTP response." "If the server receives additional data from the client on a fully closed
connection, such as another request sent by the client before receiving the server's response,
the server's TCP stack will send a reset packet to the client"; "the reset packet might erase the
client's unacknowledged input buffers before they can be read and interpreted by the client's HTTP
parser." "To avoid the TCP reset problem, servers typically close a connection in stages. First,
the server performs a half-close by closing only the write side of the read/write connection. The
server then continues to read from the connection until it receives a corresponding close by the
client, or until the server is reasonably certain that its own TCP stack has received the client's
acknowledgement of the packet(s) containing the server's last response. Finally, the server fully
closes the connection." The section names no duration.

[5] RFC 9110 §10.1.1, as research/25 §2 quotes it: an origin server "MUST send either: * an
immediate response with a final status code, if that status can be determined by examining just
the method, target URI, and header fields, or * an immediate 100 (Continue) response"; "A client
that sends a 100-continue expectation is not required to wait for any specific length of time;
such a client MAY proceed to send the content even if it has not yet received a response"; "A
server that responds with a final status code before reading the entire request content SHOULD
indicate whether it intends to close the connection ... or continue reading the request content."

[6] Amazon S3's error codes as research/05 §11.2 records them: `MissingContentLength` 411 "You must
provide the Content-Length HTTP header."; `MaxMessageLengthExceeded` 400 "Your request was too
large."; `RequestHeaderSectionTooLarge` 400; `IncompleteBody` 400. `XAmzContentSHA256Mismatch`,
for a body whose SHA-256 differs from a hex `x-amz-content-sha256`, is widely seen from S3 and in
neither of AWS's lists (research/05, **UNVERIFIED**).

[7] Measured 2026-11-02 on this Mac (macOS 26.4.1, Apple M5 Max): the installed AWS CLI
`aws-cli/2.34.18 Python/3.14.3 Darwin/25.4.0 exe/arm64`, its classic transfer client, running
`aws s3 cp s3://transfer/l0 - --only-show-errors --no-progress` against a loopback HTTP fixture
serving a 64 MiB object (eight 8 MiB parts). The fixture answered one part's ranged GET, on every
attempt, as the row says, and the stream on standard output was compared with the object byte
for byte. The CLI sends the HEAD's ETag as `If-Match` on every ranged GET and checks each 206's
`Content-Range` against the range it asked.

| One part's ranged GET got | Exit | Standard output | Standard error |
|---|---|---|---|
| `412 PreconditionFailed` | 1 | the parts before it | "did not match expected ETag" |
| a `206` shorter than asked, its framing whole | 1 | the parts before it | "does not match content range" |
| its body cut part way | 1 | the parts before it | `IncompleteRead` |
| `503 SlowDown` | 1 | the parts before it | "(SlowDown) ... reached max retries: 2" |
| `500 InternalError` | 1 | the parts before it | "(InternalError) ... reached max retries: 2" |
| the connection closed with no response | **0** | **the parts before it** | **empty** |

The last row held for a middle part (5 of 8 parts written, 70 s) and for the last part, whose
range the CLI asks open-ended (7 of 8, 194 s). Only that case leaves a truncated copy reported
as done. A cell's own S3 read had ended that way that day: `aws s3 cp` of a 10 GiB object exited 0
with 8,714,190,848 correct bytes, its stream cut 6.9 MB into part 1,038 of 1,280, while the same
script's boto3 client, its listing done, held one of that listener's ten connections idle, which
left the CLI's ten threads nine (docs/measurements/2026-10-28-transfer-scale.md). A thread that
found no pooled connection free opened one past the budget, which the listener closed as it came.
That fits the last row; it was not reproduced on the cell, whose nodes did not finish starting
within the check's bound.

[8] Microsoft, "Send data segment over TCP by using Winsock" (KB214397),
https://learn.microsoft.com/en-us/troubleshoot/windows/win32/data-segment-tcp-winsock: "Winsock
copies data buffers from application send calls to a Winsock kernel buffer ... You can change the
amount of Winsock kernel buffer allocated to the socket using the `SO_SNDBUF` option ... If
necessary, Winsock can buffer more than the `SO_SNDBUF` buffer size." Its rules for a send's
completion: "If the socket is still within SO_SNDBUF quota, Winsock copies the data from the
application send and indicates the send completion to the application"; "If the socket is beyond
`SO_SNDBUF` quota and there's only one previously buffered send still in the stack kernel buffer,
Winsock copies the data from the application send and indicates the send completion to the
application"; beyond the quota with more than one send buffered, it does not complete the send
until enough are sent.

[9] Microsoft, "SIO_IDEAL_SEND_BACKLOG_QUERY Control Code",
https://learn.microsoft.com/en-us/windows/win32/winsock/sio-ideal-send-backlog-query, Remarks:
"For the blocking and non-blocking send method, the send buffer limit determines how much data is
kept outstanding in TCP"; the ideal send backlog "is a function of the bandwidth-delay product of
the TCP connection and the receiver's advertised receive window" and "can theoretically vary from
0 to a maximum of 16 megabytes"; "Dynamic send buffering for TCP was added on Windows 7 and Windows
Server 2008 R2. By default, dynamic send buffering for TCP is enabled unless an application sets
the SO_SNDBUF socket option on the stream socket."

[10] Microsoft, "send function (winsock2.h)",
https://learn.microsoft.com/en-us/windows/win32/api/winsock2/nf-winsock2-send: "On nonblocking
stream oriented sockets, the number of bytes written can be between 1 and the requested length,
depending on buffer availability on both the client and server computers"; `WSAEWOULDBLOCK`: "The
socket is marked as nonblocking and the requested operation would block."

[11] J. Davies, "The Cable Guy: TCP Receive Window Auto-Tuning", TechNet Magazine, January 2007,
https://learn.microsoft.com/en-us/previous-versions/technet-magazine/cc162519(v=msdn.10): the
receive window is sized "by measuring the BDP and the application retrieve rate", "allowing up to
a 16MB maximum receive window size"; `SO_RCVBUF` set the maximum before Vista.

[12] `write(2)`, macOS and FreeBSD: "When using non-blocking I/O on objects, such as sockets, that
are subject to flow control, write() and writev() may write fewer bytes than requested"; Linux
`socket(7)`, `SO_SNDBUF`: "Sets or gets the maximum socket send buffer in bytes."

## What the sources settle

**A floor of ten connections.** botocore keeps at most ten connections to an endpoint by default
[2], which every boto3 client and the AWS CLI's classic transfer client are, and the CLI's
transfers keep at most ten requests in flight [1]. A listener holding ten serves one such client
at its defaults: `mantle_node::listener::CONNECTIONS`. The CLI's CRT transfer client opens its
throughput target over 0.4 Gb/s, rounded up, at least ten [3]: ten where `auto` falls back to 4
Gb/s, twenty-five at the 10 Gb/s `crt` takes at the least, more on a faster detected link [1, 3].
A floor of ten serves it only on the fallback. The budget growing past its floor with the node's
memory, which serves the CRT client and more clients than one, is open (node.md §4.5).

**A connection past the budget is answered, never closed with no answer.** A connection
closed as it comes is, to the CLI, a part whose every attempt met a closed connection, and the
CLI drops that part and reports success [7]; a `503 SlowDown`, S3's answer to a client asking
faster than it serves, it asks again after its backoff and then reports as a failure [7]. So the
listener answers a connection past its ten `503 SlowDown` with `Connection: close`, from a
reserve as large as the budget: what one more stock client opens at once [1, 2]
(`mantle_node::listener::REFUSING`). Past both, a connection is closed as it comes, a refusal
that costs nothing to make, and only a client beyond two stock clients' connections meets it.

**A quiet connection is let go after a stock client's patience.** A client's pooled connection
stays open between its requests for as long as the client lives, so ten idle connections hold
every slot. The listener waits on a client's next octet at most a stock client's patience, the
60 s botocore waits on the listener (`mantle_protocol::bounds::STOCK_PATIENCE`), then closes
the connection and gives its slot to a client that asks. Between requests no response is owed,
so none is lost to the close, and a request that crossed it is one a client asks again on another
connection, as RFC 9112 §9.3.1 lets it for a connection that closed before any response.

**A refused request's connection closes in stages.** A request whose body the listener did not
read cannot be told from the next request, so its response says `Connection: close` [5], and
whether or not its client waited for `100 Continue` it may send the body anyway [5]. The listener
half-closes its side after the response and reads until the client closes [4], dropping what
comes. RFC 9112 bounds that reading by no time [4]; the listener bounds it by octets, the most a
request's body may run to (`MAX_BODY`: S3's largest single object [6, research/05 §4.3] framed as
aws-chunked in S3's least chunks), and by the same patience between octets, past either of which
it closes.

**When a GET's socket is full.** A Linux or macOS socket takes no more of a non-blocking write
than its send buffer has room for and says how much it took [12], so a write it took in part says
the socket is full, and what it takes after is what its client made room for. A Windows socket
takes all of a send while it is under its quota, and all of one more past it while it holds a
single send, then refuses until enough is sent [8, 10]; the quota follows the ideal send backlog,
up to 16 MB, unless the socket sets `SO_SNDBUF` [9]. Offered whole blocks, it takes a get's first
two whole and never a part: no part says it is full, and once a send was refused, the next takes
a whole block for whatever room its client made. So the plain connection reads a refused send as
full on every platform, and on Windows offers its socket a record's plaintext at a time, as the
TLS connection does everywhere, so the socket takes at most two records past its client's room.
The receive window on the client's side follows its retrieve rate [11], so a slow client's stays
small however long its get.

## Open

How many connections the AWS CLI's CRT client opens on a machine whose link it detects, and how it
takes a refused one, is read from its source [3], not measured against a mantle endpoint; the CLI
and boto3 run against a cell in plan step 6. The classic client's table [7] is one CLI version's;
the stock-client suite runs the version CI's runners have. A client that stops reading a GET's
body holds its connection while the listener's writes wait on it, bounded by the transport's user
timeout only for data sent and unacknowledged; a time bound on a write a zero window holds is
open. The patience bounds the wait between a client's octets, as botocore's read timeout bounds
its wait between the listener's, so a client sending a head an octet a minute holds a slot for as
long as the head's octets last; a bound on a whole head's time is open.
