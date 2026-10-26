# 72. The S3 listener's connections: how many stock clients open, and how one is closed

Status: research note, 2026-10-25. For the S3 listener's connection budget and its closing of a
connection whose request it refused before reading its body (node.md §4.1, §4.5;
`crates/node/src/listener`). Sources [1] to [6] were read from documents and source code, each
quoted where it was read.

## The question

The listener holds a bounded number of connections at once (CLAUDE.md §2) and closes one past the
bound as it comes. Two things follow. How many connections a stock client opens to one endpoint
by default, so a cell's floor serves one such client. And how a connection is closed once a
response says `Connection: close` while its client may still be sending the body the listener did
not read, so the client reads that response rather than losing it to a reset.

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

## What the sources settle

**A floor of ten connections.** botocore keeps at most ten connections to an endpoint by default
[2], which every boto3 client and the AWS CLI's classic transfer client are, and the CLI's
transfers keep at most ten requests in flight [1]. A listener holding ten serves one such client
at its defaults: `mantle_node::listener::CONNECTIONS`. The CLI's CRT transfer client opens its
throughput target over 0.4 Gb/s, rounded up, at least ten [3]: ten where `auto` falls back to 4
Gb/s, twenty-five at the 10 Gb/s `crt` takes at the least, more on a faster detected link [1, 3].
A floor of ten serves it only on the fallback. The budget growing past its floor with the node's
memory, which serves the CRT client and more clients than one, is open (node.md §4.5).

**A refused request's connection closes in stages.** A request whose body the listener did not
read cannot be told from the next request, so its response says `Connection: close` [5], and
whether or not its client waited for `100 Continue` it may send the body anyway [5]. The listener
half-closes its side after the response and reads until the client closes [4], dropping what
comes. RFC 9112 bounds that reading by no time [4]; the listener bounds it by octets, the most a
request's body may run to (`MAX_BODY`: S3's largest single object [6, research/05 §4.3] framed as
aws-chunked in S3's least chunks), past which it closes.

## Open

How many connections the AWS CLI's CRT client opens on a machine whose link it detects, and how it
takes a refused one, is read from its source [3], not measured against a mantle endpoint; the CLI
and boto3 run against a cell in plan step 6.
