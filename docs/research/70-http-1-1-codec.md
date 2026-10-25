# 70. An HTTP/1.1 codec for the S3 listener

Status: research note, 2026-10-25. For node.md §4.1 and `crates/http`: what mantle's own HTTP/1.1
codec must accept and refuse, and within which bounds, now that the S3 listener is mantle's own
code on hyper-rt's TCP rather than hyper's server (the owner's decision: no hyper, no tokio). The
decisions it supports are in node.md §4.1.

## The question

Stock S3 tools (the AWS CLI, boto3, the SDKs, rclone) speak HTTP/1.1 to S3, and S3 serves no
other HTTP version (node.md §4.1). The codec reads requests and writes responses for them. Four
things decide its shape: the message grammar a recipient must accept, the framing errors it must
refuse and how, the bounds S3 itself puts on a request, and what a persistent connection owes the
requests that follow on it.

## Sources

RFC 9112, *HTTP/1.1* (June 2022), and RFC 9110, *HTTP Semantics* (June 2022), as published by the
RFC Editor (rfc-editor.org/rfc/rfc9112.txt and rfc9110.txt, read 2026-10-25). Quotations are
verbatim.

### The message (RFC 9112 §2)

- §2.1: `HTTP-message = start-line CRLF *( field-line CRLF ) CRLF [ message-body ]`.
- §2.2: "A recipient MUST parse an HTTP message as a sequence of octets in an encoding that is a
  superset of US-ASCII". "Although the line terminator for the start-line and fields is the
  sequence CRLF, a recipient MAY recognize a single LF as a line terminator and ignore any
  preceding CR." "A recipient of such a bare CR MUST consider that element to be invalid or
  replace each bare CR with SP before processing the element".
- §2.2: "a server that is expecting to receive and parse a request-line SHOULD ignore at least one
  empty line (CRLF) received prior to the request-line."
- §2.2: "A recipient that receives whitespace between the start-line and the first header field
  MUST either reject the message as invalid or consume each whitespace-preceded line without
  further processing of it".
- §2.2: a server that "receives a sequence of octets that does not match the HTTP-message grammar
  aside from the robustness exceptions listed above ... SHOULD respond with a 400 (Bad Request)
  response and close the connection."
- §2.3: `HTTP-version = HTTP-name "/" DIGIT "." DIGIT`, `HTTP-name = %s"HTTP"`; "HTTP-version is
  case-sensitive."

### The request line (RFC 9112 §3)

- `request-line = method SP request-target SP HTTP-version`. "recipients MAY instead parse on
  whitespace-delimited word boundaries ... However, lenient parsing can result in request
  smuggling security vulnerabilities if there are multiple recipients of the message and each has
  its own unique interpretation of robustness".
- "A server that receives a method longer than any that it implements SHOULD respond with a 501
  (Not Implemented) status code. A server that receives a request-target longer than any URI it
  wishes to parse MUST respond with a 414 (URI Too Long) status code".
- "It is RECOMMENDED that all HTTP senders and recipients support, at a minimum, request-line
  lengths of 8000 octets."
- §3.1: "The request method is case-sensitive." `method = token`.
- §3.2: "No whitespace is allowed in the request-target." "A server MUST respond with a 400 (Bad
  Request) status code to any HTTP/1.1 request message that lacks a Host header field and to any
  request message that contains more than one Host header field line or a Host header field with
  an invalid field value."
- §3.2.1: `origin-form = absolute-path [ "?" query ]`. §3.2.2: "A server MUST accept the
  absolute-form in requests even though most HTTP/1.1 clients will only send the absolute-form to
  a proxy", and "When an origin server receives a request with an absolute-form of
  request-target, the origin server MUST ignore the received Host header field (if any) and
  instead use the host information of the request-target." §3.2.3: the authority-form "is only
  used for CONNECT requests". §3.2.4: the asterisk-form "is only used for a server-wide OPTIONS
  request".

### Fields (RFC 9112 §5; RFC 9110 §5)

- RFC 9112 §5: `field-line = field-name ":" OWS field-value OWS`.
- §5.1: "No whitespace is allowed between the field name and colon. ... A server MUST reject, with
  a response status code of 400 (Bad Request), any received request message that contains
  whitespace between a header field name and colon." "OWS occurring before the first
  non-whitespace octet of the field line value, or after the last non-whitespace octet of the
  field line value, is excluded by parsers when extracting the field line value".
- §5.2: "A server that receives an obs-fold in a request message that is not within a
  "message/http" container MUST either reject the message by sending a 400 (Bad Request) ... or
  replace each received obs-fold with one or more SP octets".
- RFC 9110 §5.1: `field-name = token`; "Field names are case-insensitive". §5.6.2:
  `token = 1*tchar`, `tchar = "!" / "#" / "$" / "%" / "&" / "'" / "*" / "+" / "-" / "." / "^" /
  "_" / "`" / "|" / "~" / DIGIT / ALPHA`.
- RFC 9110 §5.5: `field-value = *field-content`, `field-content = field-vchar [ 1*( SP / HTAB /
  field-vchar ) field-vchar ]`, `field-vchar = VCHAR / obs-text`, `obs-text = %x80-FF`. "a
  recipient of CR, LF, or NUL within a field value MUST either reject the message or replace each
  of those characters with SP". "Field values containing other CTL characters are also invalid".

### Framing (RFC 9112 §6, §7; RFC 9110 §8.6)

- §6: "The presence of a message body in a request is signaled by a Content-Length or
  Transfer-Encoding header field. Request message framing is independent of method semantics."
- §6.1: "A recipient MUST be able to parse the chunked transfer coding". "If any transfer coding
  other than chunked is applied to a request's content, the sender MUST apply chunked as the final
  transfer coding". "A server that receives a request message with a transfer coding it does not
  understand SHOULD respond with 501 (Not Implemented)." "A server MAY reject a request that
  contains both Content-Length and Transfer-Encoding or process such a request in accordance with
  the Transfer-Encoding alone. Regardless, the server MUST close the connection after responding to
  such a request". "A server or client that receives an HTTP/1.0 message containing a
  Transfer-Encoding header field MUST treat the message as if the framing is faulty, even if a
  Content-Length is present, and close the connection after processing the message." A sender
  "MUST NOT apply the chunked transfer coding more than once to a message body".
- §6.3, in order of precedence: item 3, both fields: "ought to be handled as an error"; item 4,
  "If a Transfer-Encoding header field is present in a request and the chunked transfer coding is
  not the final encoding, the message body length cannot be determined reliably; the server MUST
  respond with the 400 (Bad Request) status code and then close the connection"; item 5, "If a
  message is received without Transfer-Encoding and with an invalid Content-Length header field,
  then the message framing is invalid and the recipient MUST treat it as an unrecoverable error,
  unless the field value can be successfully parsed as a comma-separated list ..., all values in
  the list are valid, and all values in the list are the same" (then "the server MUST respond with
  a 400 (Bad Request) status code and then close the connection"); item 6, "If the sender closes
  the connection or the recipient times out before the indicated number of octets are received,
  the recipient MUST consider the message to be incomplete and close the connection"; item 7, "If
  this is a request message and none of the above are true, then the message body length is zero".
- RFC 9110 §8.6: `Content-Length = 1*DIGIT`; "a recipient MUST anticipate potentially large
  decimal numerals and prevent parsing errors due to integer conversion overflows".
- §7.1: `chunked-body = *chunk last-chunk trailer-section CRLF`, `chunk = chunk-size [ chunk-ext ]
  CRLF chunk-data CRLF`, `chunk-size = 1*HEXDIG`, `last-chunk = 1*("0") [ chunk-ext ] CRLF`.
  "recipients MUST anticipate potentially large hexadecimal numerals and prevent parsing errors
  due to integer conversion overflows". "The chunked coding does not define any parameters. Their
  presence SHOULD be treated as an error."
- §7.1.1: `chunk-ext = *( BWS ";" BWS chunk-ext-name [ BWS "=" BWS chunk-ext-val ] )`. "A
  recipient MUST ignore unrecognized chunk extensions. A server ought to limit the total length of
  chunk extensions received in a request to an amount reasonable for the services provided, in the
  same way that it applies length limitations and timeouts for other parts of a message, and
  generate an appropriate 4xx (Client Error) response if that amount is exceeded."
- §7.1.2: `trailer-section = *( field-line CRLF )`; "A recipient MUST NOT merge a received trailer
  field into the header section unless its corresponding header field definition explicitly
  permits and instructs how the trailer field value can be safely merged."

### Expectation (RFC 9110 §10.1.1)

- "The only expectation defined by this specification is "100-continue"". "A server that receives
  an Expect field value containing a member other than 100-continue MAY respond with a 417
  (Expectation Failed) status code". "A server that receives a 100-continue expectation in an
  HTTP/1.0 request MUST ignore that expectation." "A server MAY omit sending a 100 (Continue)
  response if it has already received some or all of the content for the corresponding request,
  or if the framing indicates that there is no content." research/25 §2 records what the server owes a 100-continue:
  "either ... an immediate response with a final status code ... or ... an immediate 100 (Continue)
  response", and "The origin server MUST NOT wait for the content before sending the 100
  (Continue) response."

### Persistence and pipelining (RFC 9112 §9.3)

- "If the "close" connection option is present (Section 9.6), the connection will not persist
  after the current response; else, If the received protocol is HTTP/1.1 (or later), the
  connection will persist after the current response; else, If the received protocol is HTTP/1.0,
  the "keep-alive" connection option is present, ..., the connection will persist after the
  current response; otherwise, The connection will close after the current response."
- "A server MUST read the entire request message body or close the connection after sending its
  response; otherwise, the remaining data on a persistent connection would be misinterpreted as
  the next request."
- §9.3.2: "A server MAY process a sequence of pipelined requests in parallel if they all have safe
  methods ..., but it MUST send the corresponding responses in the same order that the requests
  were received."

### S3's own bounds (research/05)

- §10.2: "The `PUT` request header is limited to 8 KB in size." HeadObject: "Request headers are
  limited to 8 KB in size." The error table: `RequestHeaderSectionTooLarge` (400), "The request
  header and query parameters used to make the request exceed the maximum allowed size."
- §11.2: `MaxMessageLengthExceeded` (400), "Your request was too large."

## What the codec does (decisions in node.md §4.1)

1. **One head of S3's 8 KB.** A request's head, its request line and field lines with their line
   ends and the empty line, is held to 8,192 octets, the bound S3 states for "the request header
   and query parameters". Past it the request is refused as S3 refuses it,
   `RequestHeaderSectionTooLarge`, unless its request line has not ended within the bound, which
   RFC 9112 §3 answers 414. The request line is supported up to that bound less the field lines
   it carries: RFC 9112's recommended 8,000 octets leave 188 for the field lines (8,192 less the
   request line's and the empty line's two line ends), and S3 refuses a longer request the same
   way.
2. **Strict grammar where leniency smuggles.** Single SP between the request line's three parts;
   no whitespace in the target; no whitespace before a field's colon (400, a MUST); obs-fold and
   whitespace-preceded lines after the request line refused (400, the "reject" each rule allows);
   a field value with a CTL other than HTAB refused (400). A line ends at CRLF or a lone LF (the
   MAY of §2.2); a bare CR anywhere else in the head is invalid. Empty lines before the request
   line are passed, within the head's bound.
3. **Framing refused, then the connection closed.** Transfer-Encoding with Content-Length: 400 and
   close (the MAY-reject of §6.1, with its MUST-close). A Transfer-Encoding whose final coding is
   not chunked, or with chunked twice: 400 and close (§6.3 item 4). A coding other than chunked,
   chunked last: 501 (§6.1's SHOULD); mantle applies no transfer coding but chunked. A
   Transfer-Encoding on an HTTP/1.0 request: faulty framing, 400 and close. A Content-Length that
   is not `1*DIGIT`, overflows 64 bits, or lists differing values: 400 and close; a list of one
   repeated valid value is that value (§6.3 item 5). Neither field: no body.
4. **Chunked bodies decoded as they stream.** A chunk's size is parsed with its overflow checked;
   its extensions are passed unread, their total held to the head's bound per request (§7.1.1's
   "same way" as the rest of the message), past it 400; the trailer section is held to the head's
   bound too and given to the caller apart from the head (§7.1.2). A body's bytes are given out as
   spans of the caller's own buffer.
5. **`Expect: 100-continue` before any body byte.** The head says whether the request expects it,
   so its caller answers before reading the body: 100, or its final status. It is ignored on
   HTTP/1.0 and where the framing says no content follows, as RFC 9110 §10.1.1 lets it be. Another
   expectation is answered 417.
6. **Host.** An HTTP/1.1 request without exactly one Host field is refused 400; an absolute-form
   target's authority is the host, the Host field ignored (§3.2.2). The authority-form (CONNECT) is
   S3's to refuse, not the codec's.
7. **One request at a time on a persistent connection.** A request ends when its body does; the
   octets after it begin the next request, which is read only once the response to this one has
   gone, so responses go in order. A connection persists by the version and `Connection` rules of
   §9.3; a request whose body the server does not read to its end, or whose framing it refused, is
   followed by the connection's close.
