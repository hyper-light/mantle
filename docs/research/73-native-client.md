# 73. The native client: admitted by its certificate, and what it checks and asks again

Status: research note, 2026-10-26. For the client of mantle's native protocol and its admission at
a node (gateway.md §6, node.md §3.6; `crates/client`, `crates/node/src/directory.rs`,
`crates/cert`, `crates/mantle/src/client.rs`). Sources [1] to [10] were read from documents and
source code, each quoted where it was read.

## The questions

A node knew every peer by the certificate its configuration pinned (node.md §1.5, §3.6), clients
included: a new client meant a line in every node's configuration, and node.md §11 left open how a
peer could be known by its name under the authority instead. Step 7 of the data-layer plan asks for
clients admitted by a certificate the cell's authority issued for the client role, pinned nowhere,
within hyper-transport's bounded identities. That raises four questions. How a certificate says
"client" so that it cannot act as a node; what principal a client's operations act for; how many
clients a node admits and what one past them is told; and, for the client itself, what it asks
again, what checksum its streamed puts carry, and what it holds a get's bytes to.

## Sources

[1] RFC 5280, https://www.rfc-editor.org/rfc/rfc5280.html. §4.2.1.12: "id-kp-serverAuth OBJECT
IDENTIFIER ::= { id-kp 1 } -- TLS WWW server authentication"; "id-kp-clientAuth OBJECT IDENTIFIER
::= { id-kp 2 } -- TLS WWW client authentication"; "If the extension is present, then the
certificate MUST only be used for one of the purposes indicated." §4.1.2.6: "The subject field
identifies the entity associated with the public key stored in the subject public key field."

[2] webpki, `src/verify_cert.rs`, https://github.com/rustls/webpki (read 2026-10-26), the verifier
rustls's `WebPkiClientVerifier` and `WebPkiServerVerifier` run: `pub const SERVER_AUTH: Self =
Self::required_if_present(EKU_SERVER_AUTH);` and `pub const CLIENT_AUTH: Self =
Self::required_if_present(EKU_CLIENT_AUTH);`, with `EKU_SERVER_AUTH` the OID 1.3.6.1.5.5.7.3.1 and
`EKU_CLIENT_AUTH` 1.3.6.1.5.5.7.3.2. Its check of a certificate whose extension lists other purposes
and not the one required ends in `Error::RequiredEkuNotFound` "regardless of mode": a certificate
listing clientAuth alone fails a server's verification.

[3] Kubernetes, "Authenticating", X509 client certificates,
https://kubernetes.io/docs/reference/access-authn-authz/authentication/: "If a client certificate is
presented and verified, the common name of the subject is used as the user name for the request."
"Client certificates can also indicate a user's group memberships using the certificate's
organization fields." A deployed precedent for naming a client and what it belongs to in its
certificate's subject, read after the chain verifies.

[4] hyper-transport, `src/admission.rs` (vendored): "Once its certificate names a peer it is charged
to that **identity**: an identity holds at most `per_identity` connections"; "the identities are
bounded, and an identity past its own bound displaces only itself." `src/lib.rs`, `Directory`:
"The peer and role a connection's handshake names, or `None`: the connection is refused
([`Refusal::Identity`]) and charged to no identity." `src/endpoint.rs`, `refusal_of`: "a TLS alert
from either side is a certificate refused ([`Refusal::Identity`])"; `Endpoint::refuse`: "Close every
connection to `peer`, telling it why".

[5] AWS SDKs and Tools Reference Guide, "Retry behavior",
https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html: "Max attempts | Total
attempts including the initial request | AWS_MAX_ATTEMPTS | max_attempts | 3 (see notes)"; "A max
attempts value of `3` means the SDK makes one initial request and up to two retries." Standard mode
is "Default for all workloads".

[6] Amazon S3 User Guide, "Checking object integrity for data uploads in Amazon S3",
https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html: "If you
don't specify a checksum algorithm and the SDK also doesn't calculate a checksum for you, then S3
automatically chooses the CRC-64/NVME (`CRC64NVME`) checksum algorithm." Its table of multipart
checksum types: "CRC-64/NVME (CRC64NVME) | Yes | No" (full object, composite). "Full object
checksums in multipart uploads are only available for CRC-based checksums because they can
linearize into a full object checksum." On trailers: "The SDK uses that algorithm to calculate the
checksum value for your object (or object parts) and automatically appends it to the end of your
chunked upload request."

[7] botocore, `botocore/httpchecksum.py`, `handle_checksum_body`,
https://github.com/boto/botocore (read 2026-10-26): "If a - is in the checksum this is not valid
Base64. S3 returns checksums that include a -# suffix to indicate a checksum derived from the hash
of all part checksums. We cannot wrap this response" — followed by `if "-" in headers[header_name]:
continue`, the response passed unchecked.

[8] RFC 8879, https://www.rfc-editor.org/rfc/rfc8879.html. §3: "The compress_certificate extension is
a unidirectional indication; no corresponding response extension is needed." §4: "If the peer has
indicated that it supports compression, server and client MAY compress their corresponding
Certificate messages". A side that advertises no algorithm receives its peer's certificate
uncompressed.

[9] mantle's own measurements: `crates/protocol/src/bounds.rs` `CERTIFICATE_COMPRESSION_BYTES`,
34,267,200 bytes for hyper-tls's brotli at quality 11 compressing one certificate, measured by
mantle-node's `tests/transport_memory.rs`; an Ed25519 certificate of this profile is a few hundred
bytes (`crates/cert`).

[10] AWS CLI Command Reference, "AWS CLI S3 Configuration",
https://docs.aws.amazon.com/cli/latest/topic/s3-config.html: `multipart_threshold`, "Default -
`8MB`"; `multipart_chunksize`, "Default - `8MB`", "Minimum For Uploads - `5MB`": "Once the S3
commands have decided to use multipart operations, the file is divided into chunks. This
configuration option specifies what the chunk size (also referred to as the part size) should be."

## Findings and decisions

**A client's certificate is a leaf for clientAuth alone, named by its subject.** [1] lets a
certificate listing purposes serve those alone, and webpki [2] refuses at a server's verification a
certificate whose extension omits serverAuth. So a certificate issued with `extKeyUsage` clientAuth
and nothing else can never serve a node's end of a handshake, while a node's (serverAuth and
clientAuth) can serve both. Its subject names the client ([1] §4.1.2.6): its common name
`client-ID`, the transport's identity, and its organization the account it acts for, as Kubernetes
names a user and its groups [3]. `mantle_cert::client` issues it; `mantle_cert::client_names` reads
the two names back from a certificate whose extensions are exactly the client profile's, so a
node's certificate, or one carrying any other extension, names no client.

**A node admits a client by those names, pinning none** (node.md §3.6, closing §11's open item for
clients). The handshake verified the chain to the cell's authority for clientAuth [2]; a node's
directory then takes a certificate it does not pin as a client's when its names read [4], unless
the id is a pinned peer's or one held under another account. Nodes and gateways stay pinned: what
they may send (Raft, a range's commands, chunks) is no client's.

**A client's operations act for its account.** The principal of a client's object operation is the
account its certificate names, the one the cell's access keys act for on the S3 listener, so a
bucket a client makes through either door is its account's (S3 owns buckets by account). The
directory keeps each admitted client's account while the transport holds a connection of it, and
the node prunes the rest whenever its connections move (`Peers::keep_clients`): a table bounded by
the identities and the handshakes the transport admits [4].

**A client sends objects alone.** A range's commands and reads, which write and read metadata rows
directly, are a node's or a gateway's to send: a client admitted by a certificate is the cell's
operator's grant of S3's operations, judged by the node's gateway, not of the metadata.

**How many clients, and what one past them is told.** A node that hosts objects admits
`gateway.clients` clients at once, each an identity of its transport beside its pinned peers, so a
crowd of clients cannot take a node's identity from its cell. `mantle serve` derives it at the
floor: as many as the operations the node runs at once (`gateway.objects`), each client one it may
run, growth past it waiting for its knee as every floor does (node.md §1.4). `Directory::identify`
can only refuse with `Refusal::Identity` [4], which a client cannot tell from a certificate refused
at TLS [4]; so the directory admits a client whose names read, and the node, once the transport
says the connection is up, closes one past its bound with `Refusal::Identities` (`Endpoint::refuse`
[4]), the refusal a transport gives an identity past its bound, which a client asks again after.

**What the client asks again.** A request is asked again only where no node can have taken it (its
node not reached, the transport's admission refused its connection, or its exchange got no stream),
with none of its body taken, of the next node; at most three attempts, the AWS SDKs' standard mode's default [5], each after RFC 6298's
doubling from the measured probe timeout with equal jitter (node.md §3.8). Anything else is the
caller's, told whether the request was sent whole, except a node's `SlowDown`: asked again
like the SDKs' throttling errors (2026-10-31, research/77 D4), within the same three attempts,
after botocore's standard mode's throttling backoff (full jitter over `min(2^(n-1) s, 20 s)`) and
paid from its retry quota (500 units, 5 a retry, refilled by answers), of the next node, its
identity with it; a run of an upload is sent again from where it began.

**A streamed put states its checksum in a trailer, CRC-64/NVME unless named.** A source read once
(standard input) cannot state its checksum before its bytes, so the native put carries the
trailer's algorithm in its head and the value after its bytes, which the node checks before
anything commits, as aws-chunked's trailer does [6]. The default is CRC-64/NVME: the one S3 keeps
for an object uploaded without a checksum, and a full-object value across a multipart upload's
parts [6], so a multipart object's checksum stays one value of its bytes.

**A get is held to a full-object checksum, not to a composite one.** A composite value combines the
parts' checksums and is no checksum of the bytes; botocore passes such a response unchecked [7],
and so does the client. A range is not checked against the whole object's value.

**Standard input is put in the AWS CLI's parts.** A part states its length before its bytes, so a
source whose length is unknown until its end is read a part at a time, each whole before it is
sent: the command line's parts are the AWS CLI's default 8 MB [10], above S3's 5 MB least part
but the last, and its memory for such a copy one part.

**Certificates are not compressed between a client and a node.** Compression is optional and
offered by a side's advertisement [8]; brotli's compression of one certificate took 34 MB against a
certificate of a few hundred bytes [9], so the client neither compresses its certificate nor
advertises decompression, and its node sends its own uncompressed.

## Open

- The client's own journal, resumable uploads in server-cut runs and their kill tests (gateway.md
  §2.1, §6): the plan's step 8.
- A client's certificate is not yet revoked or rotated, as no certificate of the cell's is
  (node.md §1.5).
