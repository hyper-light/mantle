//! `mantle`'s object commands through the cell's S3 door (`--door s3`; docs/design/doors.md): each
//! command asked as S3's own operation over HTTP/1.1, SigV4-signed with the cell's key, at the
//! endpoint the client's S3 profile names (`mantle init client` writes it under CLIENT/s3;
//! src/profile.rs). Its lines are the native door's, so a command says the same through either.
//!
//! **Its exchanges** are mantle's own codec's (`mantle_http`): a request's head written by
//! `write::request`, a response's head parsed where it lies (`response::parse_response`) and its
//! body decoded as it streams (`mantle_http::Body`), over one connection kept from request to
//! request, plain on a loopback endpoint and TLS elsewhere, verified by the endpoint's authority
//! (node.md §4.1). It runs on one hyper-rt shard, as the native door's client does; files, standard
//! input and output are the native door's own (src/client.rs). Every wait on the socket is held to
//! a stock S3 client's patience, 60 s without a byte (`bounds::STOCK_PATIENCE`).
//!
//! **What it asks again.** A request the listener let go of its connection before answering is
//! asked once more on a new connection, where it is idempotent (RFC 9112 §9.3.1); one that is not
//! (CreateMultipartUpload, CompleteMultipartUpload) is sent only on a connection that has carried
//! nothing before, so a kept connection the listener let go as quiet never takes one. A `503` is
//! asked again as the native client asks a `SlowDown` (research/73): at most [`ATTEMPTS`] in all,
//! after botocore's full-jitter backoff, paid from the same retry quota. A body read once (standard
//! input streamed to a part) is never sent twice.
//!
//! **A copy into the cell** goes as the AWS CLI's does (research/73 [10]): an object of fewer than
//! [`PART`] bytes in one PutObject, a larger one as a multipart upload in parts of [`PART`], or of
//! as many more as 10,000 parts need of a file. Every object body is `aws-chunked` with a trailing
//! CRC-64/NVME (`STREAMING-UNSIGNED-PAYLOAD-TRAILER`; research/05 §1.8–§1.9), so it streams once and
//! the listener checks it whole. A multipart upload is kept in the S3 door's journal (CLIENT/s3/
//! journal; src/journal.rs) before its first part, so a copy run again after a kill lists the parts
//! the cell holds, reads the source's bytes of each again, and goes on past those whose MD5 is
//! their ETag, sending none of them again; a source changed below them is `BadDigest`, its upload
//! aborted. Its parts' CRC-64/NVMEs, combined, are held to the object's full-object value the
//! completion answers. A part from standard input is held until it is acknowledged, one part, as
//! the native door holds one run.
//!
//! **A copy out of the cell** is one GetObject streamed to the file or standard output, its bytes
//! counted to their length and held to the object's full-object CRC-64/NVME where it keeps one; a
//! file is written beside its place and renamed over it, so it is the whole object or untouched.
//!
//! **Its memory** is bounded: the connection's buffer of one piece (`mantle_client::PIECE`), a
//! response's document at most [`DOCUMENT`], a head at most `MAX_HEAD`, and one part where a source
//! reads once.

use std::io::{self, Read as _, Write};
use std::net::SocketAddr;
use std::path::Path;

use hyper_rt::combine::{Either, race2};
use hyper_rt::futures::sleep;
use hyper_rt::runtime::LocalRuntime;
use hyper_rt::tcp::{Shutdown, TcpStream};
use hyper_tls::pki_types::{CertificateDer, ServerName};
use hyper_tls::{ClientConfig, ClientConnection, RootCertStore};
use mantle_client::{
    ATTEMPTS, PIECE, QUOTA, SLOWED_BASE, SLOWED_MOST, SLOWED_RETRY, Sink, Source, TRAILER,
    UNRETRIED_REFUND,
};
use mantle_http::response::{Ends, Response, parse_response};
use mantle_http::write::{self as http_write, Length};
use mantle_http::{Body as Decoder, MAX_HEAD};
use mantle_protocol::bounds::{STOCK_PATIENCE, full_jitter};
use mantle_s3::checksum::{self, Algorithm, Checksum, Hasher};
use mantle_s3::sigv4;
use mantle_s3::xml::{Reader, Writer};

use crate::client::{
    self, Command, Error, FileSink, FileSource, Location, PART, Place, Stdin, Stdout, date, line,
};
use crate::journal::{Journal, Origin, Record};
use crate::profile::Profile;
use crate::s3_code::Code;

/// The most octets a response's document is kept to: a listing page's, the largest the door reads,
/// its head ([`HEAD`]) and `mantle_s3::list::MAX_KEYS` entries (S3's 1,000: a ListObjectsV2 page's
/// keys and common prefixes together, a ListMultipartUploads page's uploads), each its key of at
/// most `mantle_s3::route::MAX_KEY` octets escaped at worst and the rest of it at most [`ENTRY`]. A
/// ListParts page's parts hold no key, and a ListBuckets page's `mantle_protocol::object::MAX_BUCKETS`
/// buckets names and regions that never escape, so both are smaller (the tests write each).
const DOCUMENT: usize = HEAD + mantle_s3::list::MAX_KEYS * (KEY + ENTRY);

/// The most octets the listing writer puts for one octet of text: a control character's reference,
/// `&#x1f;` (`mantle_s3::xml`'s `escape`).
const ESCAPED: usize = "&#x1f;".len();

/// A key at its widest: `mantle_s3::route::MAX_KEY` octets, each escaped.
const KEY: usize = ESCAPED * mantle_s3::route::MAX_KEY;

/// An owner's name at its widest: `mantle_s3::account::MAX_OWNER` octets, each escaped.
const OWNER: usize = ESCAPED * mantle_s3::account::MAX_OWNER;

/// An ETag or an upload's ID at its widest: `mantle_protocol::object::MAX_TAG` octets, each escaped.
const TAG: usize = ESCAPED * mantle_protocol::object::MAX_TAG;

/// The most octets a listing entry holds beside its key, as `mantle_s3::response` writes each: a
/// ListObjects page's [`CONTENTS`] or a ListMultipartUploads page's [`UPLOAD`], whichever is wider;
/// a common prefix's holds less than either.
const ENTRY: usize = if CONTENTS > UPLOAD { CONTENTS } else { UPLOAD };

/// A `Contents` at its widest beside its key: [`CONTENTS_TEXT`], its quoted ETag and its owner.
const CONTENTS: usize = CONTENTS_TEXT + TAG + OWNER;

/// The text of a `Contents` at its widest with its ETag, key and owner's name left empty, as
/// `mantle_s3::response` writes one: the longest checksum algorithm's name (`CRC64NVME`, as long as
/// `XXHASH128`; `Algorithm::name`) and type's (`ChecksumType::name`), the last time an S3 time holds,
/// `u64::MAX`'s digits and its storage class.
const CONTENTS_TEXT: usize = "<Contents><ChecksumAlgorithm>CRC64NVME</ChecksumAlgorithm>\
    <ChecksumType>FULL_OBJECT</ChecksumType><ETag>\"\"</ETag><Key></Key>\
    <LastModified>9999-12-31T23:59:59.000Z</LastModified><Owner><ID></ID></Owner>\
    <Size>18446744073709551615</Size><StorageClass>STANDARD</StorageClass></Contents>"
    .len();

/// An `Upload` at its widest beside its key: [`UPLOAD_TEXT`], its initiator, owner and ID.
const UPLOAD: usize = UPLOAD_TEXT + OWNER + OWNER + TAG;

/// The text of an `Upload` at its widest with its initiator's and owner's names, key and ID left
/// empty, as `mantle_s3::response` writes one.
const UPLOAD_TEXT: usize = "<Upload><ChecksumAlgorithm>CRC64NVME</ChecksumAlgorithm>\
    <ChecksumType>FULL_OBJECT</ChecksumType><Initiated>9999-12-31T23:59:59.000Z</Initiated>\
    <Initiator><ID></ID></Initiator><Key></Key><Owner><ID></ID></Owner>\
    <StorageClass>STANDARD</StorageClass><UploadId></UploadId></Upload>"
    .len();

/// The most octets a listing the door reads holds beside its entries: [`ROOT_TEXT`] and
/// ListParts' twelve elements, the most any listing's head holds, each no wider than a
/// [`FIELD_TEXT`] holding a key at its widest: every head element holds a key, a marker, a
/// continuation token (a key's octets and one in base64), an upload's ID, an owner, a bucket's
/// name, a count or a name of S3's, each at most that (`mantle_s3::response`). A document past
/// [`DOCUMENT`] is refused, typed, as it streams in.
const HEAD: usize = ROOT_TEXT + 12 * (FIELD_TEXT + KEY);

/// The XML declaration and the longest listing root with its namespace, as `mantle_s3::xml::Writer`
/// writes a document (`NAMESPACE`).
const ROOT_TEXT: usize = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListMultipartUploadsResult \
    xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></ListMultipartUploadsResult>"
    .len();

/// The tags of the longest-named head element.
const FIELD_TEXT: usize = "<NextContinuationToken></NextContinuationToken>".len();

/// The patience every wait on the socket is held to, in nanoseconds.
fn patience_ns() -> u64 {
    u64::try_from(STOCK_PATIENCE.as_nanos()).unwrap_or(u64::MAX)
}

/// Runs `command` through the S3 door as the client of the directory `dir`, its lines to `out`.
pub fn run(dir: &Path, command: Command, mut out: impl Write + 'static) -> Result<(), Error> {
    let profile = Profile::read(&dir.join("s3")).map_err(|e| Error::Config(e.0, e.1))?;
    let endpoint = Endpoint::of(&profile)?;
    // One socket, its patience's timer and a backoff's.
    let mut rt: LocalRuntime = client::runtime_of(dir, 1, 1)?;
    client::start_blocking()?;
    let dir = dir.to_path_buf();
    let ran = rt
        .block_on(async move {
            let mut door = S3Door::new(profile, endpoint)?;
            command_on(&mut door, &dir, command, &mut out).await
        })
        .map_err(Error::from);
    hyper_rt::blocking::stop()?;
    ran?
}

/// Where the S3 door listens, as the profile's `endpoint_url` names it.
struct Endpoint {
    tls: bool,
    host: String,
    port: u16,
}

impl Endpoint {
    fn of(profile: &Profile) -> Result<Self, Error> {
        let url = profile.endpoint.as_deref().ok_or_else(|| {
            Error::Usage(
                "the client's S3 profile names no endpoint_url: the cell's founders had no S3 \
                 port and name `mantle init` could state"
                    .into(),
            )
        })?;
        let bad = || Error::Usage(format!("{url}: not http://HOST:PORT or https://HOST:PORT"));
        let (tls, rest) = match url.split_once("://") {
            Some(("http", rest)) => (false, rest),
            Some(("https", rest)) => (true, rest),
            _ => return Err(bad()),
        };
        let authority = rest.trim_end_matches('/');
        let (host, port) = authority.rsplit_once(':').ok_or_else(bad)?;
        let port: u16 = port.parse().map_err(|_| bad())?;
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if host.is_empty() || authority.contains('/') {
            return Err(bad());
        }
        Ok(Self {
            tls,
            host: host.to_owned(),
            port,
        })
    }

    /// The Host field's value: the authority as the URL names it (RFC 9110 §7.2).
    fn authority(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

/// A connection's transport: plain on a loopback endpoint, TLS elsewhere.
enum Conn {
    Plain(TcpStream),
    Tls(Box<Tls>),
}

/// A TLS connection: its socket, its state and the configuration its handshake reads.
struct Tls {
    stream: TcpStream,
    connection: ClientConnection,
    config: ClientConfig,
}

/// Awaits `ready` for at most the patience: the socket's readiness, or why it never came.
async fn within<F>(ready: F, what: &str) -> Result<(), Error>
where
    F: std::future::Future<Output = Result<(), hyper_rt::RtError>>,
{
    match race2(ready, sleep(patience_ns())).await {
        Either::First(ready) => ready.map_err(|e| Error::Http(format!("{what}: {e:?}"))),
        Either::Second(_) => Err(Error::Http(format!(
            "{what}: no byte in {} s, a stock S3 client's patience",
            STOCK_PATIENCE.as_secs()
        ))),
    }
}

/// The socket as `std::io` reads and writes it, for hyper-tls: a read or write the socket cannot
/// take now is `WouldBlock`.
struct Socket<'a>(&'a TcpStream);

impl io::Read for Socket<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.0.try_read(buf) {
            Ok(Some(n)) => Ok(n),
            Ok(None) => Err(io::ErrorKind::WouldBlock.into()),
            Err(e) => Err(io::Error::other(format!("{e:?}"))),
        }
    }
}

impl io::Write for Socket<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.0.try_write(buf) {
            Ok(Some(n)) => Ok(n),
            Ok(None) => Err(io::ErrorKind::WouldBlock.into()),
            Err(e) => Err(io::Error::other(format!("{e:?}"))),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Tls {
    /// Sends what the connection holds of records, waiting while the socket is full.
    async fn flush(&mut self) -> Result<(), Error> {
        while self.connection.wants_write() {
            match self.connection.write_tls(&mut Socket(&self.stream)) {
                Ok(0) => return Err(Error::Http("a write the socket took none of".into())),
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    within(self.stream.writable(), "writing").await?;
                }
                Err(e) => return Err(Error::Http(format!("writing: {e}"))),
            }
        }
        Ok(())
    }

    /// Reads records the socket holds and processes them, waiting until some come: false once
    /// the server's side has ended.
    async fn take_records(&mut self) -> Result<bool, Error> {
        loop {
            match self.connection.read_tls(&mut Socket(&self.stream)) {
                Ok(0) => return Ok(false),
                Ok(_) => break,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    within(self.stream.readable(), "reading").await?;
                }
                Err(e) => return Err(Error::Http(format!("reading: {e}"))),
            }
        }
        if let Err(e) = self.connection.process_new_packets(&mut self.config) {
            // The alert that says why goes to the server before the connection ends.
            let _ = self.flush().await;
            return Err(Error::Http(format!("TLS: {e}")));
        }
        Ok(true)
    }

    /// The handshake, run through before any request is written.
    async fn handshake(&mut self) -> Result<(), Error> {
        while self.connection.is_handshaking() {
            self.flush().await?;
            if self.connection.is_handshaking()
                && self.connection.wants_read()
                && !self.take_records().await?
            {
                return Err(Error::Http("TLS: the server ended the handshake".into()));
            }
        }
        self.flush().await
    }
}

impl Conn {
    /// Reads plaintext into `buf`, waiting until some comes: the octets read, none once the
    /// server's side has ended. Under TLS, a close without `close_notify` ends it too: HTTP's own
    /// framing tells a whole message from a cut one (RFC 9112 §9.8).
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        match self {
            Self::Plain(stream) => loop {
                if let Some(n) = stream
                    .try_read(buf)
                    .map_err(|e| Error::Http(format!("reading: {e:?}")))?
                {
                    return Ok(n);
                }
                within(stream.readable(), "reading").await?;
            },
            Self::Tls(tls) => loop {
                match tls.connection.reader().read(buf) {
                    Ok(n) => return Ok(n),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(0),
                    Err(e) => return Err(Error::Http(format!("reading: {e}"))),
                }
                tls.flush().await?;
                if !tls.take_records().await? {
                    return Ok(0);
                }
            },
        }
    }

    /// Writes all of `bytes`, waiting while the socket is full.
    async fn write(&mut self, bytes: &[u8]) -> Result<(), Error> {
        match self {
            Self::Plain(stream) => {
                let mut at = 0usize;
                while let Some(rest) = bytes.get(at..).filter(|rest| !rest.is_empty()) {
                    match stream
                        .try_write(rest)
                        .map_err(|e| Error::Http(format!("writing: {e:?}")))?
                    {
                        Some(0) => {
                            return Err(Error::Http("a write the socket took none of".into()));
                        }
                        Some(n) => at = at.saturating_add(n),
                        None => within(stream.writable(), "writing").await?,
                    }
                }
                Ok(())
            }
            Self::Tls(tls) => {
                let mut at = 0usize;
                while let Some(rest) = bytes.get(at..).filter(|rest| !rest.is_empty()) {
                    let took = tls
                        .connection
                        .writer()
                        .write(rest)
                        .map_err(|e| Error::Http(format!("writing: {e}")))?;
                    at = at.saturating_add(took);
                    tls.flush().await?;
                }
                tls.flush().await
            }
        }
    }

    /// Ends the connection: TLS's `close_notify`, then TCP's FIN.
    async fn close(&mut self) {
        let stream = match self {
            Self::Plain(stream) => stream,
            Self::Tls(tls) => {
                tls.connection.send_close_notify();
                let _ = tls.flush().await;
                &tls.stream
            }
        };
        let _ = stream.shutdown(Shutdown::Write);
    }
}

/// A response's head as it came: its octets and what they say.
struct Answered {
    head: Vec<u8>,
    response: Response,
}

impl Answered {
    fn status(&self) -> u16 {
        self.response.status()
    }

    /// The first field named `name`, as text.
    fn field(&self, name: &str) -> Option<&str> {
        self.response
            .field(&self.head, name)
            .and_then(|v| std::str::from_utf8(v).ok())
    }

    /// The ETag field, bare of its quotes.
    fn etag(&self) -> Option<String> {
        self.field("etag").map(|e| e.trim_matches('"').to_owned())
    }
}

/// What a request carries: no body, a document, or an object's bytes of a stated length, sent
/// `aws-chunked` with a trailing CRC-64/NVME.
enum Sent<'b, B> {
    None,
    Document(&'b [u8]),
    Object(&'b mut B, u64),
}

/// An object's or a part's bytes, read once each time it is sent.
trait Bytes {
    async fn read(&mut self, into: &mut [u8]) -> io::Result<usize>;
    /// Back to their first byte for another attempt: false where the source is read once.
    async fn again(&mut self) -> io::Result<bool>;
}

/// No bytes: what a request with no object's body is typed by.
struct Nothing;

impl Bytes for Nothing {
    async fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Ok(0)
    }

    async fn again(&mut self) -> io::Result<bool> {
        Ok(true)
    }
}

/// A file's bytes from `offset`, read again from there for another attempt.
struct FileRange {
    source: FileSource,
    offset: u64,
}

impl Bytes for FileRange {
    async fn read(&mut self, into: &mut [u8]) -> io::Result<usize> {
        self.source.read(into).await
    }

    async fn again(&mut self) -> io::Result<bool> {
        mantle_client::Rewind::rewind(&mut self.source, self.offset).await?;
        Ok(true)
    }
}

/// Bytes held, as a part read from standard input is until it is acknowledged.
struct Held<'h> {
    bytes: &'h [u8],
    at: usize,
}

impl Bytes for Held<'_> {
    async fn read(&mut self, into: &mut [u8]) -> io::Result<usize> {
        let rest = self.bytes.get(self.at..).unwrap_or_default();
        let n = rest.len().min(into.len());
        if let (Some(into), Some(from)) = (into.get_mut(..n), rest.get(..n)) {
            into.copy_from_slice(from);
        }
        self.at = self.at.saturating_add(n);
        Ok(n)
    }

    async fn again(&mut self) -> io::Result<bool> {
        self.at = 0;
        Ok(true)
    }
}

/// Standard input streamed once.
struct Once(Stdin);

impl Bytes for Once {
    async fn read(&mut self, into: &mut [u8]) -> io::Result<usize> {
        self.0.read(into).await
    }

    async fn again(&mut self) -> io::Result<bool> {
        Ok(false)
    }
}

impl<B: Bytes> Sent<'_, B> {
    /// Ready to be sent again from its first byte: false where it is read once.
    async fn again(&mut self) -> Result<bool, Error> {
        match self {
            Self::None | Self::Document(_) => Ok(true),
            Self::Object(bytes, _) => bytes
                .again()
                .await
                .map_err(|e| Error::Http(format!("reading the body again: {e}"))),
        }
    }
}

/// An S3 request: its operation's name, method, bucket and key, its query's parameters (decoded)
/// and its own headers (lower-case names).
struct Ask<'a> {
    op: &'static str,
    method: &'static str,
    bucket: &'a str,
    key: Option<&'a str>,
    query: Vec<(&'static str, String)>,
    headers: Vec<(&'static str, String)>,
}

impl<'a> Ask<'a> {
    fn new(op: &'static str, method: &'static str, bucket: &'a str, key: Option<&'a str>) -> Self {
        Self {
            op,
            method,
            bucket,
            key,
            query: Vec::new(),
            headers: Vec::new(),
        }
    }

    fn query(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.query.push((name, value.into()));
        self
    }

    fn header(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.headers.push((name, value.into()));
        self
    }

    /// Whether asking it twice does what asking it once does (RFC 9110 §9.2.2).
    fn idempotent(&self) -> bool {
        matches!(self.method, "GET" | "HEAD" | "PUT" | "DELETE")
    }
}

/// How one attempt ended without an answer: before any octet of one, so it may be asked again,
/// or past that.
enum Attempt {
    Unanswered(Error),
    Failed(Error),
}

/// The S3 door's client: its profile, its endpoint and its one connection.
struct S3Door {
    profile: Profile,
    endpoint: Endpoint,
    host: String,
    conn: Option<Conn>,
    /// Whether the connection held may carry the next request.
    persists: bool,
    read: Vec<u8>,
    held: usize,
    head: Vec<u8>,
    piece: Vec<u8>,
    /// The CRC-64/NVME of the object's body the last attempt sent whole.
    sent: Option<Checksum>,
    /// The retry quota's units left (`mantle_client::QUOTA`).
    quota: u32,
    /// The jitter's draws: this client's own number, and the draws taken.
    local: u64,
    waits: u32,
}

impl S3Door {
    fn new(profile: Profile, endpoint: Endpoint) -> Result<Self, Error> {
        let host = endpoint.authority();
        let local =
            u64::try_from(mantle_client::identity()? & u128::from(u64::MAX)).unwrap_or(u64::MAX);
        Ok(Self {
            profile,
            endpoint,
            host,
            conn: None,
            persists: false,
            read: vec![0u8; PIECE.max(MAX_HEAD)],
            held: 0,
            head: Vec::new(),
            piece: Vec::with_capacity(PIECE),
            sent: None,
            quota: QUOTA,
            local,
            waits: 0,
        })
    }

    /// A connection to the endpoint, its TLS handshake run where it is one: each address its name
    /// resolves to tried in the resolver's order, each held to the patience, until one answers, as
    /// Python's `socket.create_connection` and urllib3's, which botocore's pool dials through, try
    /// them (research/86 [9]); the last one's failure where none does.
    async fn connect(&self) -> Result<Conn, Error> {
        let addresses: Vec<SocketAddr> = match self.endpoint.host.parse::<std::net::IpAddr>() {
            Ok(ip) => vec![SocketAddr::new(ip, self.endpoint.port)],
            Err(_) => hyper_rt::dns::resolve(&self.endpoint.host, self.endpoint.port, usize::MAX)
                .await
                .map_err(|e| Error::Http(format!("{}: {e:?}", self.endpoint.host)))?,
        };
        let mut failed = Error::Http(format!("{}: no address", self.endpoint.host));
        let mut dialed = None;
        for address in addresses {
            match race2(TcpStream::connect(address), sleep(patience_ns())).await {
                Either::First(Ok(stream)) => {
                    dialed = Some(stream);
                    break;
                }
                Either::First(Err(e)) => {
                    failed = Error::Http(format!("connecting to {address}: {e:?}"));
                }
                Either::Second(_) => {
                    failed = Error::Http(format!(
                        "connecting to {address}: no answer in {} s",
                        STOCK_PATIENCE.as_secs()
                    ));
                }
            }
        }
        let Some(stream) = dialed else {
            return Err(failed);
        };
        if !self.endpoint.tls {
            return Ok(Conn::Plain(stream));
        }
        let setup = |e: hyper_tls::Error| Error::Http(format!("TLS: {e}"));
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from(self.profile.authority.clone()))
            .map_err(setup)?;
        let mut config =
            ClientConfig::builder_with_provider(&hyper_tls::crypto::aws_lc_rs::DEFAULT_PROVIDER)
                .with_safe_default_protocol_versions()
                .map_err(setup)?
                .with_root_certificates(roots)
                .with_no_client_auth();
        let name = ServerName::try_from(self.endpoint.host.clone())
            .map_err(|e| Error::Http(format!("{}: {e}", self.endpoint.host)))?;
        let connection = ClientConnection::new(&mut config, name).map_err(setup)?;
        let mut tls = Tls {
            stream,
            connection,
            config,
        };
        tls.handshake().await?;
        Ok(Conn::Tls(Box::new(tls)))
    }

    /// Lets the connection go: the next request opens another.
    async fn drop_conn(&mut self) {
        if let Some(mut conn) = self.conn.take() {
            conn.close().await;
        }
        self.held = 0;
    }

    /// `ask`'s head, signed now (research/05 §1.2), written by mantle's codec into the client's
    /// head buffer: every header signed, the length the codec's to write.
    fn sign<B>(&mut self, ask: &Ask<'_>, body: &Sent<'_, B>) -> Result<(), Error> {
        let (stamp, day) =
            crate::bench_s3::timestamp().map_err(|e| Error::Http(format!("signing: {e}")))?;
        let mut path = String::from("/");
        path.push_str(&sigv4::uri_encode(ask.bucket.as_bytes(), true));
        if let Some(key) = ask.key {
            path.push('/');
            path.push_str(&sigv4::uri_encode(key.as_bytes(), false));
        }
        let mut query = String::new();
        for (name, value) in &ask.query {
            if !query.is_empty() {
                query.push('&');
            }
            query.push_str(name);
            query.push('=');
            query.push_str(&sigv4::uri_encode(value.as_bytes(), true));
        }
        let (payload, length, own): (String, Length, Vec<(&str, String)>) = match body {
            Sent::None => (sigv4::EMPTY_SHA256.to_owned(), Length::None, Vec::new()),
            Sent::Document(bytes) => (
                sigv4::hex(&mantle_s3::crypto::sha256(bytes).map_err(crypto)?),
                Length::Known(u64::try_from(bytes.len()).map_err(|_| too_large())?),
                Vec::new(),
            ),
            Sent::Object(_, length) => (
                "STREAMING-UNSIGNED-PAYLOAD-TRAILER".to_owned(),
                Length::Known(chunked_length(*length, &mut self.piece)?),
                vec![
                    ("content-encoding", "aws-chunked".to_owned()),
                    ("x-amz-decoded-content-length", length.to_string()),
                    ("x-amz-trailer", TRAILER.header().to_owned()),
                ],
            ),
        };
        let mut all: Vec<(&str, &str)> = Vec::with_capacity(
            ask.headers
                .len()
                .saturating_add(own.len())
                .saturating_add(3),
        );
        all.push(("host", &self.host));
        all.push(("x-amz-date", &stamp));
        all.push(("x-amz-content-sha256", &payload));
        all.extend(ask.headers.iter().map(|(n, v)| (*n, v.as_str())));
        all.extend(own.iter().map(|(n, v)| (*n, v.as_str())));
        let mut signed: Vec<&str> = all.iter().map(|(n, _)| *n).collect();
        signed.sort_unstable();
        signed.dedup();
        let canonical = sigv4::canonical_request(
            &sigv4::Request {
                method: ask.method,
                path: &path,
                query: &query,
                headers: &all,
            },
            &signed,
            &payload,
            None,
        )
        .map_err(|e| Error::Http(format!("signing: {e}")))?;
        let signature = sigv4::sign(
            &self.profile.secret,
            &stamp,
            &day,
            &self.profile.region,
            &canonical,
        )
        .map_err(|e| Error::Http(format!("signing: {e}")))?;
        let scope = sigv4::scope(&day, &self.profile.region, "s3");
        let names = signed.join(";");
        let hex = sigv4::hex(&signature);
        let authorization: [&[u8]; 9] = [
            sigv4::ALGORITHM.as_bytes(),
            b" Credential=",
            self.profile.id.as_bytes(),
            b"/",
            scope.as_bytes(),
            b", SignedHeaders=",
            names.as_bytes(),
            b", Signature=",
            hex.as_bytes(),
        ];
        let values: Vec<[&[u8]; 1]> = all.iter().map(|(_, v)| [v.as_bytes()]).collect();
        let mut fields: Vec<(&str, &[&[u8]])> = all
            .iter()
            .zip(&values)
            .map(|((name, _), value)| (*name, &value[..]))
            .collect();
        fields.push(("authorization", authorization.as_slice()));
        let parts: [&[u8]; 3] = [path.as_bytes(), b"?", query.as_bytes()];
        let target = if query.is_empty() {
            parts.get(..1).unwrap_or_default()
        } else {
            parts.as_slice()
        };
        self.head.clear();
        http_write::request(&mut self.head, ask.method, target, &fields, length)
            .map_err(|e| Error::Http(format!("{}: the request's head: {e}", ask.op)))
    }

    /// Asks `ask` with `body` and reads its response's head: asked again on a new connection
    /// where the listener let the one held go before answering and the request is idempotent, and
    /// again after a `503`'s backoff, within [`ATTEMPTS`] and the retry quota.
    async fn ask<B: Bytes>(
        &mut self,
        ask: &Ask<'_>,
        body: &mut Sent<'_, B>,
    ) -> Result<Answered, Error> {
        let mut attempts = 0u32;
        let mut stale = false;
        loop {
            attempts = attempts.saturating_add(1);
            // A request that may not be asked twice (RFC 9112 §9.3.1) goes on a connection that
            // has carried nothing, which the listener cannot have let go as quiet: a POST is
            // never left to a kept connection's chance.
            if !ask.idempotent() {
                self.drop_conn().await;
            }
            let reused = self.conn.is_some();
            match self.once(ask, body).await {
                Ok(answered)
                    if answered.status() == 503
                        && attempts < ATTEMPTS
                        && self.quota >= SLOWED_RETRY
                        && body.again().await? =>
                {
                    self.finish(&answered, &mut Kept::discard()).await?;
                    self.quota = self.quota.saturating_sub(SLOWED_RETRY);
                    let doubled = 1u32
                        .checked_shl(attempts.saturating_sub(1))
                        .unwrap_or(u32::MAX);
                    let cap = SLOWED_BASE.saturating_mul(doubled).min(SLOWED_MOST);
                    let pause = full_jitter(cap, self.local, self.waits);
                    self.waits = self.waits.wrapping_add(1);
                    let _ = sleep(u64::try_from(pause.as_nanos()).unwrap_or(u64::MAX)).await;
                }
                Ok(answered) => {
                    if attempts == 1 {
                        self.quota = self.quota.saturating_add(UNRETRIED_REFUND).min(QUOTA);
                    }
                    return Ok(answered);
                }
                Err(Attempt::Unanswered(e)) => {
                    self.drop_conn().await;
                    if !(reused && !stale && ask.idempotent() && body.again().await?) {
                        return Err(e);
                    }
                    stale = true;
                    attempts = attempts.saturating_sub(1);
                }
                Err(Attempt::Failed(e)) => {
                    self.drop_conn().await;
                    return Err(e);
                }
            }
        }
    }

    /// One attempt: the head and body written, the response's head read.
    async fn once<B: Bytes>(
        &mut self,
        ask: &Ask<'_>,
        body: &mut Sent<'_, B>,
    ) -> Result<Answered, Attempt> {
        self.sign(ask, &*body).map_err(Attempt::Failed)?;
        if self.conn.is_none() {
            self.conn = Some(self.connect().await.map_err(Attempt::Failed)?);
            self.held = 0;
        }
        let head = std::mem::take(&mut self.head);
        let wrote = self.write(&head).await;
        self.head = head;
        wrote.map_err(Attempt::Unanswered)?;
        match body {
            Sent::None => {}
            Sent::Document(bytes) => self.write(bytes).await.map_err(Attempt::Unanswered)?,
            Sent::Object(bytes, length) => {
                let length = *length;
                let sum = self
                    .send_chunked(&mut **bytes, length)
                    .await
                    .map_err(Attempt::Unanswered)?;
                self.sent = Some(sum);
            }
        }
        let is_head = ask.method == "HEAD";
        loop {
            let found = parse_response(self.read.get(..self.held).unwrap_or_default(), is_head)
                .map_err(|e| Attempt::Failed(Error::Http(format!("{}: {e}", ask.op))))?;
            if let Some(response) = found {
                let len = response.len();
                let head = self.read.get(..len).unwrap_or_default().to_vec();
                self.take(len);
                // An interim response is passed: its final one follows (RFC 9110 §15.2).
                if (100..200).contains(&response.status()) {
                    continue;
                }
                self.persists = response.persists();
                return Ok(Answered { head, response });
            }
            let before = self.held;
            match self.fill().await {
                Ok(0) if before == 0 => {
                    return Err(Attempt::Unanswered(Error::Http(format!(
                        "{}: the listener closed the connection before answering",
                        ask.op
                    ))));
                }
                Ok(0) => {
                    return Err(Attempt::Failed(Error::Http(format!(
                        "{}: the listener closed within an answer's head",
                        ask.op
                    ))));
                }
                Ok(_) => {}
                Err(e) if before == 0 => return Err(Attempt::Unanswered(e)),
                Err(e) => return Err(Attempt::Failed(e)),
            }
        }
    }

    async fn write(&mut self, bytes: &[u8]) -> Result<(), Error> {
        self.conn
            .as_mut()
            .ok_or_else(|| Error::Http("no connection".into()))?
            .write(bytes)
            .await
    }

    /// Reads more octets after those held: none at the connection's end.
    async fn fill(&mut self) -> Result<usize, Error> {
        if self.held >= self.read.len() {
            return Err(Error::Http("a response head past the read buffer".into()));
        }
        let conn = self
            .conn
            .as_mut()
            .ok_or_else(|| Error::Http("no connection".into()))?;
        let into = self
            .read
            .get_mut(self.held..)
            .ok_or_else(|| Error::Http("the read buffer".into()))?;
        let n = conn.read(into).await?;
        self.held = self.held.saturating_add(n);
        Ok(n)
    }

    /// Drops the first `n` octets held.
    fn take(&mut self, n: usize) {
        let n = n.min(self.held);
        self.read.copy_within(n..self.held, 0);
        self.held = self.held.saturating_sub(n);
    }

    /// Sends `length` bytes of `bytes` `aws-chunked`, each chunk a piece, ended by the trailer's
    /// CRC-64/NVME of them: their checksum, which the listener holds them to.
    async fn send_chunked<B: Bytes>(
        &mut self,
        bytes: &mut B,
        length: u64,
    ) -> Result<Checksum, Error> {
        let mut crc = Hasher::new(TRAILER).map_err(crypto)?;
        let mut piece = std::mem::take(&mut self.piece);
        let mut at = 0u64;
        let sent = async {
            while at < length {
                let n = usize::try_from(length.saturating_sub(at))
                    .unwrap_or(usize::MAX)
                    .min(PIECE);
                piece.clear();
                http_write::chunk(&mut piece, n);
                let start = piece.len();
                piece.resize(start.saturating_add(n), 0);
                let mut filled = 0usize;
                while filled < n {
                    let into = piece
                        .get_mut(start.saturating_add(filled)..start.saturating_add(n))
                        .unwrap_or_default();
                    let got = bytes
                        .read(into)
                        .await
                        .map_err(|e| Error::Http(format!("reading the body: {e}")))?;
                    if got == 0 {
                        return Err(Error::Http(format!(
                            "the source ended at {} of its {length} bytes",
                            at.saturating_add(u64::try_from(filled).unwrap_or(u64::MAX))
                        )));
                    }
                    filled = filled.saturating_add(got);
                }
                crc.update(piece.get(start..).unwrap_or_default())
                    .map_err(crypto)?;
                piece.extend_from_slice(http_write::CHUNK_END);
                self.write(&piece).await?;
                at = at.saturating_add(u64::try_from(n).map_err(|_| too_large())?);
            }
            let sum = crc.finish().map_err(crypto)?;
            piece.clear();
            http_write::last(
                &mut piece,
                &[(TRAILER.header(), sum.to_base64().as_bytes())],
            )
            .map_err(|e| Error::Http(format!("the trailer: {e}")))?;
            self.write(&piece).await?;
            Ok(sum)
        };
        let sum = sent.await;
        self.piece = piece;
        sum
    }

    /// Reads the response's body into `sink`, and lets the connection go where it does not
    /// persist past it.
    async fn finish<K: Sink>(&mut self, answered: &Answered, sink: &mut K) -> Result<u64, Error> {
        let got = self.body(answered.response.ends(), sink).await;
        if got.is_err() || !self.persists {
            self.drop_conn().await;
        }
        got
    }

    async fn body<K: Sink>(&mut self, ends: Ends, sink: &mut K) -> Result<u64, Error> {
        let short = || Error::Http("the listener ended a body short".into());
        let mut got = 0u64;
        let mut decoder = match ends {
            Ends::Framed(framing) => Some(Decoder::new(framing)),
            Ends::Close => None,
        };
        loop {
            match decoder.as_mut() {
                Some(decoder) => {
                    if decoder.done() {
                        return Ok(got);
                    }
                    if self.held == 0 && self.fill().await? == 0 {
                        return Err(short());
                    }
                    let input = self.read.get(..self.held).unwrap_or_default();
                    let step = decoder
                        .decode(input)
                        .map_err(|e| Error::Http(format!("a response's body: {e}")))?;
                    if let Some(data) = input.get(step.data.clone())
                        && !data.is_empty()
                    {
                        sink.write(data).await.map_err(Error::Output)?;
                        got = got.saturating_add(u64::try_from(data.len()).unwrap_or(u64::MAX));
                    }
                    self.take(step.consumed);
                    if step.consumed == 0 && !step.end && self.fill().await? == 0 {
                        return Err(short());
                    }
                }
                None => {
                    if self.held > 0 {
                        let input = self.read.get(..self.held).unwrap_or_default();
                        sink.write(input).await.map_err(Error::Output)?;
                        got = got.saturating_add(u64::try_from(input.len()).unwrap_or(u64::MAX));
                        self.take(self.held);
                    }
                    if self.fill().await? == 0 {
                        return Ok(got);
                    }
                }
            }
        }
    }

    /// The error a response that is not a success names: S3's `Error` document's code and
    /// message, or its status where it has no body.
    async fn refused<T>(&mut self, op: &str, answered: Answered) -> Result<T, Error> {
        let mut kept = Kept::new();
        self.finish(&answered, &mut kept).await?;
        Err(error_of(op, answered.status(), &kept.0))
    }

    /// Asks `ask`, which carries no body, for a document: its response's body, refused unless it
    /// is a success.
    async fn document(&mut self, ask: &Ask<'_>, body: Option<&[u8]>) -> Result<Vec<u8>, Error> {
        let mut sent: Sent<'_, Nothing> = match body {
            Some(bytes) => Sent::Document(bytes),
            None => Sent::None,
        };
        let answered = self.ask(ask, &mut sent).await?;
        if !(200..300).contains(&answered.status()) {
            return self.refused(ask.op, answered).await;
        }
        let mut kept = Kept::new();
        self.finish(&answered, &mut kept).await?;
        Ok(kept.0)
    }

    /// Asks `ask` and takes a success's head, its body read and let go.
    async fn done<B: Bytes>(
        &mut self,
        ask: &Ask<'_>,
        body: &mut Sent<'_, B>,
    ) -> Result<Answered, Error> {
        let answered = self.ask(ask, body).await?;
        if !(200..300).contains(&answered.status()) {
            return self.refused(ask.op, answered).await;
        }
        self.finish(&answered, &mut Kept::discard()).await?;
        Ok(answered)
    }
}

/// A sink that keeps up to [`DOCUMENT`] octets, or keeps none and counts them.
struct Kept(Vec<u8>, bool);

impl Kept {
    fn new() -> Self {
        Self(Vec::new(), true)
    }

    fn discard() -> Self {
        Self(Vec::new(), false)
    }
}

impl Sink for Kept {
    async fn write(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.1 {
            if self.0.len().saturating_add(bytes.len()) > DOCUMENT {
                return Err(io::Error::other(format!(
                    "a response's document past {DOCUMENT} octets"
                )));
            }
            self.0.extend_from_slice(bytes);
        }
        Ok(())
    }
}

fn crypto(e: impl std::fmt::Debug) -> Error {
    Error::Runtime(format!("{e:?}"))
}

/// A page whose continuation is the one it was asked from: the next would list it again, and the
/// listing would never end (CLAUDE.md §2).
fn repeated(op: &str) -> Error {
    Error::Http(format!("{op}: a page whose continuation does not move on"))
}

fn too_large() -> Error {
    Error::Usage("a length past what this platform addresses".into())
}

fn xml(op: &str) -> impl Fn(mantle_s3::xml::XmlError) -> Error + '_ {
    move |e| Error::Http(format!("{op}: its response's document: {e}"))
}

/// An `aws-chunked` body's octets on the wire for `length` bytes: each piece's chunk head, its
/// data and its end, the last chunk and the trailer, written by mantle's codec into `scratch` to be
/// counted (a CRC-64/NVME's base64 is twelve octets).
fn chunked_length(length: u64, scratch: &mut Vec<u8>) -> Result<u64, Error> {
    let piece = u64::try_from(PIECE).map_err(|_| too_large())?;
    let mut total = 0u64;
    let mut left = length;
    while left > 0 {
        let n = left.min(piece);
        scratch.clear();
        http_write::chunk(scratch, usize::try_from(n).map_err(|_| too_large())?);
        let framing = u64::try_from(scratch.len().saturating_add(http_write::CHUNK_END.len()))
            .map_err(|_| too_large())?;
        total = total
            .checked_add(n)
            .and_then(|t| t.checked_add(framing))
            .ok_or_else(too_large)?;
        left = left.saturating_sub(n);
    }
    scratch.clear();
    http_write::last(scratch, &[(TRAILER.header(), b"AAAAAAAAAAA=")])
        .map_err(|e| Error::Http(format!("the trailer: {e}")))?;
    total
        .checked_add(u64::try_from(scratch.len()).map_err(|_| too_large())?)
        .ok_or_else(too_large)
}

/// The error `status` and its body `document` name (research/05 §11.1): S3's `Error` document's
/// `Code` and `Message`, typed; the status alone where the body is empty, as a HEAD's is.
fn error_of(op: &str, status: u16, document: &[u8]) -> Error {
    if document.is_empty() {
        return Error::Status(op.to_owned(), status);
    }
    match error_document(document) {
        Ok((code, message)) => match Code::parse(&code) {
            Some(code) => Error::S3(op.to_owned(), code, message),
            None => Error::UnlistedCode(op.to_owned(), code, message),
        },
        Err(e) => Error::Http(format!("{op}: {status}, its error document: {e}")),
    }
}

/// An `Error` document's code and message.
fn error_document(document: &[u8]) -> Result<(String, String), mantle_s3::xml::XmlError> {
    let mut reader = Reader::open(document, DOCUMENT, "Error")?;
    let (mut code, mut message) = (String::new(), String::new());
    while let Some(name) = reader.child()? {
        let text = reader.text()?;
        match name {
            "Code" => code = text.into_owned(),
            "Message" => message = text.into_owned(),
            _ => {}
        }
    }
    reader.finish()?;
    Ok((code, message))
}

/// Reads the element just given whose children each hold text, as an `Owner` does: each child's
/// name and text, in order.
fn leaves<'x>(reader: &mut Reader<'x>) -> Result<Vec<(&'x str, String)>, mantle_s3::xml::XmlError> {
    let mut leaves = Vec::new();
    while let Some(name) = reader.child()? {
        leaves.push((name, reader.text()?.into_owned()));
    }
    Ok(leaves)
}

/// The leaf named `name` among `leaves`.
fn leaf(leaves: &[(&str, String)], name: &str) -> Option<String> {
    leaves
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, v)| v.clone())
}

/// What `ls` without a place lists: each bucket's creation and name, and where the next page
/// starts.
type Buckets = (Vec<(String, String)>, Option<String>);

fn list_buckets_document(doc: &[u8]) -> Result<Buckets, mantle_s3::xml::XmlError> {
    let mut reader = Reader::open(doc, DOCUMENT, "ListAllMyBucketsResult")?;
    let (mut buckets, mut next) = (Vec::new(), None);
    while let Some(name) = reader.child()? {
        match name {
            "Buckets" => {
                while reader.child()?.is_some() {
                    let bucket = leaves(&mut reader)?;
                    buckets.push((
                        leaf(&bucket, "CreationDate").unwrap_or_default(),
                        leaf(&bucket, "Name").unwrap_or_default(),
                    ));
                }
            }
            "Owner" => {
                leaves(&mut reader)?;
            }
            "ContinuationToken" => next = Some(reader.text()?.into_owned()),
            _ => {
                reader.text()?;
            }
        }
    }
    reader.finish()?;
    Ok((buckets, next.filter(|t| !t.is_empty())))
}

/// A page of a bucket's keys: its common prefixes, each key's time, size and name, and the token
/// of the next page where it is truncated.
struct KeysPage {
    prefixes: Vec<String>,
    keys: Vec<(String, u64, String)>,
    next: Option<String>,
}

fn list_objects_document(doc: &[u8]) -> Result<KeysPage, mantle_s3::xml::XmlError> {
    let mut reader = Reader::open(doc, DOCUMENT, "ListBucketResult")?;
    let mut page = KeysPage {
        prefixes: Vec::new(),
        keys: Vec::new(),
        next: None,
    };
    let mut truncated = false;
    while let Some(name) = reader.child()? {
        match name {
            "Contents" => {
                let (mut key, mut modified, mut size) = (String::new(), String::new(), 0u64);
                while let Some(field) = reader.child()? {
                    match field {
                        "Owner" | "RestoreStatus" => {
                            leaves(&mut reader)?;
                        }
                        "Key" => key = reader.text()?.into_owned(),
                        "LastModified" => modified = reader.text()?.into_owned(),
                        "Size" => {
                            size = reader
                                .text()?
                                .parse()
                                .map_err(|_| mantle_s3::xml::XmlError::Schema("a Size"))?;
                        }
                        _ => {
                            reader.text()?;
                        }
                    }
                }
                page.keys.push((modified, size, key));
            }
            "CommonPrefixes" => {
                let prefix = leaves(&mut reader)?;
                page.prefixes
                    .push(leaf(&prefix, "Prefix").unwrap_or_default());
            }
            "IsTruncated" => truncated = reader.text()? == "true",
            "NextContinuationToken" => page.next = Some(reader.text()?.into_owned()),
            _ => {
                reader.text()?;
            }
        }
    }
    reader.finish()?;
    if !truncated {
        page.next = None;
    } else if page.next.is_none() {
        return Err(mantle_s3::xml::XmlError::Schema(
            "a truncated page with no NextContinuationToken",
        ));
    }
    Ok(page)
}

/// A page of an upload's parts: each part's number, size and ETag (bare), and the marker the
/// next page starts after where it is truncated.
type PartsPage = (Vec<(u16, u64, String)>, Option<u16>);

fn list_parts_document(doc: &[u8]) -> Result<PartsPage, mantle_s3::xml::XmlError> {
    let schema = mantle_s3::xml::XmlError::Schema;
    let mut reader = Reader::open(doc, DOCUMENT, "ListPartsResult")?;
    let (mut parts, mut next, mut truncated) = (Vec::new(), None, false);
    while let Some(name) = reader.child()? {
        match name {
            "Part" => {
                let part = leaves(&mut reader)?;
                let number = leaf(&part, "PartNumber")
                    .and_then(|n| n.parse().ok())
                    .ok_or(schema("a PartNumber"))?;
                let size = leaf(&part, "Size")
                    .and_then(|n| n.parse().ok())
                    .ok_or(schema("a Size"))?;
                let etag = leaf(&part, "ETag").ok_or(schema("an ETag"))?;
                parts.push((number, size, etag.trim_matches('"').to_owned()));
            }
            "Initiator" | "Owner" => {
                leaves(&mut reader)?;
            }
            "IsTruncated" => truncated = reader.text()? == "true",
            "NextPartNumberMarker" => {
                next = Some(
                    reader
                        .text()?
                        .parse()
                        .map_err(|_| schema("a NextPartNumberMarker"))?,
                );
            }
            _ => {
                reader.text()?;
            }
        }
    }
    reader.finish()?;
    if truncated && next.is_none() {
        return Err(schema("a truncated page with no NextPartNumberMarker"));
    }
    Ok((parts, next.filter(|_| truncated)))
}

/// A page of a bucket's open uploads: each one's initiation, ID and key, and the key and ID
/// markers the next page starts after where it is truncated.
type UploadsPage = (Vec<(String, String, String)>, Option<(String, String)>);

fn list_uploads_document(doc: &[u8]) -> Result<UploadsPage, mantle_s3::xml::XmlError> {
    let mut reader = Reader::open(doc, DOCUMENT, "ListMultipartUploadsResult")?;
    let (mut uploads, mut truncated) = (Vec::new(), false);
    let (mut key, mut id) = (String::new(), String::new());
    while let Some(name) = reader.child()? {
        match name {
            "Upload" => {
                let (mut initiated, mut upload, mut at) =
                    (String::new(), String::new(), String::new());
                while let Some(field) = reader.child()? {
                    match field {
                        "Initiator" | "Owner" => {
                            leaves(&mut reader)?;
                        }
                        "Initiated" => initiated = reader.text()?.into_owned(),
                        "UploadId" => upload = reader.text()?.into_owned(),
                        "Key" => at = reader.text()?.into_owned(),
                        _ => {
                            reader.text()?;
                        }
                    }
                }
                uploads.push((initiated, upload, at));
            }
            "CommonPrefixes" => {
                leaves(&mut reader)?;
            }
            "IsTruncated" => truncated = reader.text()? == "true",
            "NextKeyMarker" => key = reader.text()?.into_owned(),
            "NextUploadIdMarker" => id = reader.text()?.into_owned(),
            _ => {
                reader.text()?;
            }
        }
    }
    reader.finish()?;
    Ok((uploads, if truncated { Some((key, id)) } else { None }))
}

/// The upload ID an `InitiateMultipartUploadResult` names.
fn initiated_document(doc: &[u8]) -> Result<String, mantle_s3::xml::XmlError> {
    let mut reader = Reader::open(doc, DOCUMENT, "InitiateMultipartUploadResult")?;
    let mut upload = None;
    while let Some(name) = reader.child()? {
        let text = reader.text()?;
        if name == "UploadId" {
            upload = Some(text.into_owned());
        }
    }
    reader.finish()?;
    upload.ok_or(mantle_s3::xml::XmlError::Schema("no UploadId"))
}

/// A `CompleteMultipartUploadResult`'s ETag (bare) and its full-object CRC-64/NVME where it
/// states one.
fn completed_document(doc: &[u8]) -> Result<(String, Option<String>), mantle_s3::xml::XmlError> {
    let mut reader = Reader::open(doc, DOCUMENT, "CompleteMultipartUploadResult")?;
    let (mut etag, mut crc) = (None, None);
    while let Some(name) = reader.child()? {
        let text = reader.text()?;
        if name == "ETag" {
            etag = Some(text.trim_matches('"').to_owned());
        } else if name == TRAILER.element() {
            crc = Some(text.into_owned());
        }
    }
    reader.finish()?;
    Ok((
        etag.ok_or(mantle_s3::xml::XmlError::Schema("no ETag"))?,
        crc,
    ))
}

/// What one command asks, its lines to `out`.
async fn command_on(
    door: &mut S3Door,
    dir: &Path,
    command: Command,
    out: &mut impl Write,
) -> Result<(), Error> {
    match command {
        Command::MakeBucket(bucket) => {
            // A region but S3's default is named in the request's configuration (S3 API
            // Reference, CreateBucket; research/05 §10.4).
            let configuration = (door.profile.region != crate::init::DEFAULT_REGION).then(|| {
                Writer::document("CreateBucketConfiguration", true, |w| {
                    w.text("LocationConstraint", &door.profile.region);
                })
                .into_bytes()
            });
            let mut sent: Sent<'_, Nothing> = match &configuration {
                Some(bytes) => Sent::Document(bytes),
                None => Sent::None,
            };
            door.done(&Ask::new("CreateBucket", "PUT", &bucket, None), &mut sent)
                .await?;
            line(out, format_args!("made s3://{bucket}"))
        }
        Command::RemoveBucket(bucket) => {
            door.done::<Nothing>(
                &Ask::new("DeleteBucket", "DELETE", &bucket, None),
                &mut Sent::None,
            )
            .await?;
            line(out, format_args!("removed s3://{bucket}"))
        }
        Command::List(None) => list_buckets(door, out).await,
        Command::List(Some(at)) => list_keys(door, &at, out).await,
        Command::Copy(from, to, size) => copy(door, dir, (from, to), size, out).await,
        Command::Remove(at) => {
            door.done::<Nothing>(
                &Ask::new("DeleteObject", "DELETE", &at.bucket, Some(&at.key)),
                &mut Sent::None,
            )
            .await?;
            line(out, format_args!("removed s3://{}/{}", at.bucket, at.key))
        }
        Command::Head(at) => head(door, &at, out).await,
        Command::CreateUpload(at) => {
            let upload = create_upload(door, &at).await?;
            line(out, format_args!("{upload}"))
        }
        Command::UploadPart {
            at,
            upload,
            number,
            from,
            length,
        } => {
            let etag = match (from, length) {
                (Place::File(path), None) => {
                    let length = std::fs::metadata(&path)
                        .map_err(|e| Error::File(path.clone(), e))?
                        .len();
                    let mut bytes = FileRange {
                        source: FileSource::open(&path, 0, Some(length))?,
                        offset: 0,
                    };
                    put_part(door, &at, (&upload, number), &mut bytes, length)
                        .await?
                        .0
                }
                (Place::Standard, Some(length)) => {
                    let mut bytes = Once(Stdin::take()?);
                    put_part(door, &at, (&upload, number), &mut bytes, length)
                        .await?
                        .0
                }
                _ => {
                    return Err(Error::Usage(
                        "a part comes from a file, or from standard input with its --length".into(),
                    ));
                }
            };
            line(out, format_args!("{number} {etag}"))
        }
        Command::Complete { at, upload, parts } => {
            let (etag, _) = complete(door, &at, &upload, &parts).await?;
            line(out, format_args!("etag \"{etag}\""))
        }
        Command::Abort { at, upload } => {
            abort(door, &at, &upload).await?;
            line(out, format_args!("aborted {upload}"))
        }
        Command::Parts { at, upload } => {
            for (number, size, etag) in all_parts(door, &at, &upload).await? {
                line(out, format_args!("{number} {size} \"{etag}\""))?;
            }
            Ok(())
        }
        Command::Uploads(bucket) => list_uploads(door, &bucket, out).await,
    }
}

/// ListBuckets, a page of [`mantle_protocol::object::MAX_BUCKETS`] at a time.
async fn list_buckets(door: &mut S3Door, out: &mut impl Write) -> Result<(), Error> {
    let mut token: Option<String> = None;
    loop {
        let mut ask = Ask::new("ListBuckets", "GET", "", None).query(
            "max-buckets",
            mantle_protocol::object::MAX_BUCKETS.to_string(),
        );
        let asked = token.take();
        if let Some(token) = &asked {
            ask = ask.query("continuation-token", token.clone());
        }
        let doc = door.document(&ask, None).await?;
        let (buckets, next) = list_buckets_document(&doc).map_err(xml(ask.op))?;
        for (created, bucket) in &buckets {
            line(out, format_args!("{created} {bucket}"))?;
        }
        match next {
            Some(next) if Some(&next) == asked.as_ref() => return Err(repeated(ask.op)),
            Some(next) => token = Some(next),
            None => return Ok(()),
        }
    }
}

/// ListObjectsV2 of a bucket's keys under a prefix, a level of `/` at a time, a page of
/// `mantle_s3::list::MAX_KEYS` at a time.
async fn list_keys(door: &mut S3Door, at: &Location, out: &mut impl Write) -> Result<(), Error> {
    let mut token: Option<String> = None;
    loop {
        let mut ask = Ask::new("ListObjectsV2", "GET", &at.bucket, None)
            .query("list-type", "2")
            .query("prefix", at.key.clone())
            .query("delimiter", "/")
            .query("max-keys", mantle_s3::list::MAX_KEYS.to_string());
        let asked = token.take();
        if let Some(token) = &asked {
            ask = ask.query("continuation-token", token.clone());
        }
        let doc = door.document(&ask, None).await?;
        let page = list_objects_document(&doc).map_err(xml(ask.op))?;
        for prefix in &page.prefixes {
            line(out, format_args!("PRE {prefix}"))?;
        }
        for (modified, size, key) in &page.keys {
            line(out, format_args!("{modified} {size} {key}"))?;
        }
        match page.next {
            Some(next) if Some(&next) == asked.as_ref() => return Err(repeated(ask.op)),
            Some(next) => token = Some(next),
            None => return Ok(()),
        }
    }
}

/// Every part of `upload` the cell holds, ListParts a page of S3's 1,000 parts at a time.
async fn all_parts(
    door: &mut S3Door,
    at: &Location,
    upload: &str,
) -> Result<Vec<(u16, u64, String)>, Error> {
    let mut all = Vec::new();
    let mut marker = 0u16;
    loop {
        let ask = Ask::new("ListParts", "GET", &at.bucket, Some(&at.key))
            .query("uploadId", upload)
            .query(
                "max-parts",
                mantle_protocol::object::MAX_PAGE_PARTS.to_string(),
            )
            .query("part-number-marker", marker.to_string());
        let doc = door.document(&ask, None).await?;
        let (parts, next) = list_parts_document(&doc).map_err(xml(ask.op))?;
        all.extend(parts);
        match next {
            Some(next) if next > marker => marker = next,
            Some(_) => return Err(repeated(ask.op)),
            None => return Ok(all),
        }
    }
}

/// ListMultipartUploads of a bucket, a page of S3's 1,000 uploads at a time.
async fn list_uploads(door: &mut S3Door, bucket: &str, out: &mut impl Write) -> Result<(), Error> {
    let mut markers: Option<(String, String)> = None;
    loop {
        let mut ask = Ask::new("ListMultipartUploads", "GET", bucket, None)
            .query("uploads", "")
            .query(
                "max-uploads",
                mantle_protocol::object::MAX_PAGE_PARTS.to_string(),
            );
        let asked = markers.take();
        if let Some((key, id)) = &asked {
            ask = ask
                .query("key-marker", key.clone())
                .query("upload-id-marker", id.clone());
        }
        let doc = door.document(&ask, None).await?;
        let (uploads, next) = list_uploads_document(&doc).map_err(xml(ask.op))?;
        for (initiated, id, key) in &uploads {
            line(out, format_args!("{initiated} {id} {key}"))?;
        }
        match next {
            Some(next) if Some(&next) == asked.as_ref() => return Err(repeated(ask.op)),
            Some(next) => markers = Some(next),
            None => return Ok(()),
        }
    }
}

/// HeadObject, its checksum asked for: its facts in the native door's lines.
async fn head(door: &mut S3Door, at: &Location, out: &mut impl Write) -> Result<(), Error> {
    let ask = Ask::new("HeadObject", "HEAD", &at.bucket, Some(&at.key))
        .header("x-amz-checksum-mode", "ENABLED");
    let answered = door.done::<Nothing>(&ask, &mut Sent::None).await?;
    let size: u64 = answered
        .field("content-length")
        .and_then(|l| l.parse().ok())
        .ok_or_else(|| Error::Http("HeadObject: no Content-Length".into()))?;
    line(out, format_args!("size {size}"))?;
    line(
        out,
        format_args!("etag \"{}\"", answered.etag().unwrap_or_default()),
    )?;
    let modified = answered
        .field("last-modified")
        .and_then(mantle_s3::time::parse_http_date)
        .and_then(|s| u64::try_from(s).ok())
        .ok_or_else(|| Error::Http("HeadObject: no Last-Modified".into()))?;
    line(out, format_args!("modified {}", date(modified)))?;
    if let Some(version) = answered.field("x-amz-version-id") {
        line(out, format_args!("version {version}"))?;
    }
    for algorithm in Algorithm::ALL {
        if let Some(value) = answered.field(algorithm.header()) {
            line(out, format_args!("checksum {} {value}", algorithm.name()))?;
        }
    }
    Ok(())
}

/// CreateMultipartUpload, its parts' CRC-64/NVMEs combined as one full-object value (S3:
/// CRC-64/NVME is a full-object type; research/73).
async fn create_upload(door: &mut S3Door, at: &Location) -> Result<String, Error> {
    let ask = Ask::new("CreateMultipartUpload", "POST", &at.bucket, Some(&at.key))
        .query("uploads", "")
        .header("x-amz-checksum-algorithm", TRAILER.name())
        .header("x-amz-checksum-type", "FULL_OBJECT");
    let doc = door.document(&ask, None).await?;
    initiated_document(&doc).map_err(xml(ask.op))
}

/// UploadPart `number` of `upload`: its ETag (bare) and the CRC-64/NVME of its bytes.
async fn put_part<B: Bytes>(
    door: &mut S3Door,
    at: &Location,
    (upload, number): (&str, u16),
    bytes: &mut B,
    length: u64,
) -> Result<(String, Checksum), Error> {
    let ask = Ask::new("UploadPart", "PUT", &at.bucket, Some(&at.key))
        .query("partNumber", number.to_string())
        .query("uploadId", upload);
    put(door, &ask, bytes, length).await
}

/// A PutObject or an UploadPart of `length` bytes: its ETag (bare) and the CRC-64/NVME of the
/// bytes the listener took, which it held them to.
async fn put<B: Bytes>(
    door: &mut S3Door,
    ask: &Ask<'_>,
    bytes: &mut B,
    length: u64,
) -> Result<(String, Checksum), Error> {
    if length > mantle_s3::body::MAX_UPLOAD {
        return Err(Error::Usage(format!(
            "{}: {length} bytes, past S3's 5 GiB a request carries",
            ask.op
        )));
    }
    door.sent = None;
    let answered = door.done(ask, &mut Sent::Object(bytes, length)).await?;
    // The CRC-64/NVME the trailer carried, of the attempt the listener answered, which held the
    // bytes to it, and the value the answer states where it states one.
    let sum = door
        .sent
        .take()
        .ok_or_else(|| Error::Http(format!("{}: answered before its body was sent", ask.op)))?;
    if answered
        .field(TRAILER.header())
        .is_some_and(|stated| stated != sum.to_base64())
    {
        return Err(Error::Corrupt(format!("{}'s bytes", ask.op)));
    }
    let etag = answered
        .etag()
        .ok_or_else(|| Error::Http(format!("{}: no ETag", ask.op)))?;
    Ok((etag, sum))
}

/// CompleteMultipartUpload of `parts`, each its number and ETag: the object's ETag (bare) and
/// its full-object CRC-64/NVME where the result states one. A success's document may still be
/// an `Error` (S3 API Reference, CompleteMultipartUpload).
async fn complete(
    door: &mut S3Door,
    at: &Location,
    upload: &str,
    parts: &[(u16, String)],
) -> Result<(String, Option<String>), Error> {
    let document = Writer::document("CompleteMultipartUpload", true, |w| {
        for (number, etag) in parts {
            w.element("Part", |w| {
                w.text("PartNumber", &number.to_string());
                w.text("ETag", &format!("\"{}\"", etag.trim_matches('"')));
            });
        }
    })
    .into_bytes();
    let ask = Ask::new("CompleteMultipartUpload", "POST", &at.bucket, Some(&at.key))
        .query("uploadId", upload);
    let doc = door.document(&ask, Some(&document)).await?;
    if Reader::open(&doc, DOCUMENT, "Error").is_ok() {
        return Err(error_of(ask.op, 200, &doc));
    }
    completed_document(&doc).map_err(xml(ask.op))
}

/// AbortMultipartUpload; an upload already gone is as good.
async fn abort(door: &mut S3Door, at: &Location, upload: &str) -> Result<(), Error> {
    let ask = Ask::new("AbortMultipartUpload", "DELETE", &at.bucket, Some(&at.key))
        .query("uploadId", upload);
    match door.done::<Nothing>(&ask, &mut Sent::None).await {
        Ok(_) | Err(Error::S3(_, Code::NoSuchUpload, _)) => Ok(()),
        Err(e) => Err(e),
    }
}

/// An object's bytes from `from` to `to`, one of them the cell's.
async fn copy(
    door: &mut S3Door,
    dir: &Path,
    (from, to): (Place, Place),
    size: Option<u64>,
    out: &mut impl Write,
) -> Result<(), Error> {
    if size.is_some() && from != Place::Standard {
        return Err(Error::Usage("--size states standard input's length".into()));
    }
    match (from, to) {
        (Place::File(path), Place::Cell(at)) => {
            let etag = file_in(door, dir, &path, &at).await?;
            line(
                out,
                format_args!(
                    "{} -> s3://{}/{} etag \"{etag}\"",
                    path.display(),
                    at.bucket,
                    at.key
                ),
            )
        }
        (Place::Standard, Place::Cell(at)) => {
            let etag = stdin_in(door, dir, &at, size).await?;
            line(
                out,
                format_args!("- -> s3://{}/{} etag \"{etag}\"", at.bucket, at.key),
            )
        }
        (Place::Cell(at), Place::File(path)) => {
            let temporary = client::temporary(&path);
            let file =
                std::fs::File::create(&temporary).map_err(|e| Error::File(temporary.clone(), e))?;
            let mut sink = FileSink::new(file);
            let kept = match get(door, &at, &mut sink).await {
                Ok(()) => sink.finish().await,
                Err(e) => Err(e),
            };
            client::placed(kept, &temporary, &path)?;
            line(
                out,
                format_args!("s3://{}/{} -> {}", at.bucket, at.key, path.display()),
            )
        }
        (Place::Cell(at), Place::Standard) => {
            let mut sink = Stdout::take()?;
            get(door, &at, &mut sink).await?;
            sink.flush().await
        }
        _ => Err(Error::Usage(
            "a copy is from a file or standard input to s3://BUCKET/KEY, or back".into(),
        )),
    }
}

/// GetObject into `sink`, its checksum asked for: every byte counted to the object's length and
/// held to its full-object CRC-64/NVME where it keeps one.
async fn get<K: Sink>(door: &mut S3Door, at: &Location, sink: &mut K) -> Result<(), Error> {
    let ask = Ask::new("GetObject", "GET", &at.bucket, Some(&at.key))
        .header("x-amz-checksum-mode", "ENABLED");
    let answered = door.ask::<Nothing>(&ask, &mut Sent::None).await?;
    if answered.status() != 200 {
        return door.refused(ask.op, answered).await;
    }
    let length: Option<u64> = answered
        .field("content-length")
        .and_then(|l| l.parse().ok());
    let full = answered
        .field(TRAILER.header())
        .filter(|_| answered.field("x-amz-checksum-type") != Some("COMPOSITE"))
        .map(str::to_owned);
    let mut hashed = Hashed {
        sink,
        hasher: full
            .as_ref()
            .map(|_| Hasher::new(TRAILER))
            .transpose()
            .map_err(crypto)?,
    };
    let got = door.finish(&answered, &mut hashed).await?;
    if length.is_some_and(|l| l != got) {
        return Err(Error::Corrupt(format!(
            "GetObject's {got} bytes, of the {} stated,",
            length.unwrap_or_default()
        )));
    }
    if let (Some(hasher), Some(kept)) = (hashed.hasher, full)
        && hasher.finish().map_err(crypto)?.to_base64() != kept
    {
        return Err(Error::Corrupt("GetObject's bytes".into()));
    }
    Ok(())
}

/// A sink that hashes what it passes on.
struct Hashed<'a, K> {
    sink: &'a mut K,
    hasher: Option<Hasher>,
}

impl<K: Sink> Sink for Hashed<'_, K> {
    async fn write(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.sink.write(bytes).await?;
        if let Some(hasher) = self.hasher.as_mut() {
            hasher
                .update(bytes)
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
        }
        Ok(())
    }
}

/// A file's copy into the cell: one PutObject below [`PART`], a multipart upload past it.
async fn file_in(
    door: &mut S3Door,
    dir: &Path,
    path: &Path,
    at: &Location,
) -> Result<String, Error> {
    let origin = Origin::file(path)?;
    let Origin::File { length, .. } = origin else {
        return Err(Error::Usage("a file's copy".into()));
    };
    if length < PART {
        let ask = Ask::new("PutObject", "PUT", &at.bucket, Some(&at.key));
        let mut bytes = FileRange {
            source: FileSource::open(path, 0, Some(length))?,
            offset: 0,
        };
        return Ok(put(door, &ask, &mut bytes, length).await?.0);
    }
    let part = file_part(length)?;
    let mut source = Parts::File {
        path: path.to_path_buf(),
        length,
        part,
    };
    multipart_in(door, dir, at, origin, &mut source).await
}

/// The part a file of `length` bytes is cut in: [`PART`], or as much more as S3's 10,000 parts
/// need, as the AWS CLI raises its `multipart_chunksize` (research/73 [10]).
fn file_part(length: u64) -> Result<u64, Error> {
    if length > mantle_s3::body::MAX_GET {
        return Err(Error::Usage(format!(
            "{length} bytes, past S3's largest object, 5 TiB"
        )));
    }
    let parts = u64::from(mantle_s3::body::MAX_PARTS);
    let least = length
        .checked_add(parts.saturating_sub(1))
        .and_then(|l| l.checked_div(parts))
        .ok_or_else(too_large)?;
    Ok(least.max(PART))
}

/// Standard input's copy into the cell: one PutObject where it ends within its first [`PART`],
/// a multipart upload of [`PART`] parts past it, each held until it is acknowledged, at most
/// 10,000 of them, as the AWS CLI's parts are (research/73 [10]).
async fn stdin_in(
    door: &mut S3Door,
    dir: &Path,
    at: &Location,
    size: Option<u64>,
) -> Result<String, Error> {
    let most = PART.saturating_mul(u64::from(mantle_s3::body::MAX_PARTS));
    if size.is_some_and(|s| s > most) {
        return Err(Error::Usage(format!(
            "--size past the {most} bytes 10,000 parts of {PART} carry from standard input"
        )));
    }
    let journal = Journal::open(&dir.join("s3"))?;
    let resuming = journal
        .find(&at.bucket, &at.key, &Origin::Standard)?
        .is_some();
    let mut stdin = Stdin::take()?;
    let mut held = Vec::new();
    held.try_reserve_exact(usize::try_from(PART).map_err(|_| too_large())?)
        .map_err(|_| Error::Runtime("a part's buffer".into()))?;
    let first = fill_part(&mut stdin, &mut held, PART).await?;
    let whole = u64::try_from(first).map_err(|_| too_large())? < PART;
    if whole && !resuming {
        let length = u64::try_from(first).map_err(|_| too_large())?;
        if size.is_some_and(|s| s != length) {
            return Err(Error::Usage(format!(
                "standard input held {length} bytes, not the {} --size states",
                size.unwrap_or_default()
            )));
        }
        let ask = Ask::new("PutObject", "PUT", &at.bucket, Some(&at.key));
        let mut bytes = Held {
            bytes: &held,
            at: 0,
        };
        return Ok(put(door, &ask, &mut bytes, length).await?.0);
    }
    let mut source = Parts::Stdin {
        stdin,
        held,
        primed: true,
        size,
        read: 0,
    };
    multipart_in(door, dir, at, Origin::Standard, &mut source).await
}

/// Reads standard input into `held` until it holds `part` bytes or the input ends: the bytes it
/// holds.
async fn fill_part(stdin: &mut Stdin, held: &mut Vec<u8>, part: u64) -> Result<usize, Error> {
    let want = usize::try_from(part).map_err(|_| too_large())?;
    held.clear();
    held.resize(want, 0);
    let mut filled = 0usize;
    while filled < want {
        let into = held.get_mut(filled..).unwrap_or_default();
        let n = stdin
            .read(into)
            .await
            .map_err(|e| Error::Http(format!("reading standard input: {e}")))?;
        if n == 0 {
            break;
        }
        filled = filled.saturating_add(n);
    }
    held.truncate(filled);
    Ok(filled)
}

/// A multipart copy's source, cut in parts.
enum Parts {
    /// A file of `length` bytes in parts of `part`.
    File {
        path: std::path::PathBuf,
        length: u64,
        part: u64,
    },
    /// Standard input in parts of [`PART`]: the part `held`, read ahead where `primed`, the bytes
    /// read so far, and the length `--size` states.
    Stdin {
        stdin: Stdin,
        held: Vec<u8>,
        primed: bool,
        size: Option<u64>,
        read: u64,
    },
}

impl Parts {
    /// Part `number`'s bytes, starting at `offset`: their length, none past the source's end.
    async fn next(&mut self, offset: u64) -> Result<u64, Error> {
        match self {
            Self::File { length, part, .. } => Ok(length.saturating_sub(offset).min(*part)),
            Self::Stdin {
                stdin,
                held,
                primed,
                size,
                read,
            } => {
                let n = if std::mem::take(primed) {
                    held.len()
                } else {
                    fill_part(stdin, held, PART).await?
                };
                let n = u64::try_from(n).map_err(|_| too_large())?;
                *read = read.saturating_add(n);
                if size.is_some_and(|s| *read > s) {
                    return Err(Error::Usage(format!(
                        "standard input held more than the {} bytes --size states",
                        size.unwrap_or_default()
                    )));
                }
                Ok(n)
            }
        }
    }

    /// Where the source ended, whether it is the length `--size` stated.
    fn ended(&self) -> Result<(), Error> {
        match self {
            Self::Stdin {
                size: Some(size),
                read,
                ..
            } if read != size => Err(Error::Usage(format!(
                "standard input held {read} bytes, not the {size} --size states"
            ))),
            _ => Ok(()),
        }
    }

    /// The MD5 and CRC-64/NVME of the part of `length` bytes at `offset`, read from the source
    /// (a part of standard input already read by [`Self::next`]).
    async fn digest(&mut self, offset: u64, length: u64) -> Result<([u8; 16], Checksum), Error> {
        let mut md5 = Hasher::new(Algorithm::Md5).map_err(crypto)?;
        let mut crc = Hasher::new(TRAILER).map_err(crypto)?;
        match self {
            Self::File { path, .. } => {
                let mut source = FileSource::open(path, offset, Some(length))?;
                let mut buf = vec![0u8; PIECE];
                let mut left = length;
                while left > 0 {
                    let want = usize::try_from(left).unwrap_or(usize::MAX).min(PIECE);
                    let into = buf.get_mut(..want).unwrap_or_default();
                    let n = source
                        .read(into)
                        .await
                        .map_err(|e| Error::File(path.clone(), e))?;
                    let read = buf.get(..n).unwrap_or_default();
                    md5.update(read).map_err(crypto)?;
                    crc.update(read).map_err(crypto)?;
                    left = left.saturating_sub(u64::try_from(n).unwrap_or(u64::MAX));
                }
            }
            Self::Stdin { held, .. } => {
                md5.update(held).map_err(crypto)?;
                crc.update(held).map_err(crypto)?;
            }
        }
        let md5 = md5.finish().map_err(crypto)?;
        let md5: [u8; 16] = md5
            .bytes
            .as_slice()
            .try_into()
            .map_err(|_| Error::Runtime("an MD5 of other than 16 bytes".into()))?;
        Ok((md5, crc.finish().map_err(crypto)?))
    }

    /// Sends the part of `length` bytes at `offset` as UploadPart `number`.
    async fn send(
        &mut self,
        door: &mut S3Door,
        at: &Location,
        (upload, number): (&str, u16),
        (offset, length): (u64, u64),
    ) -> Result<(String, Checksum), Error> {
        match self {
            Self::File { path, .. } => {
                let mut bytes = FileRange {
                    source: FileSource::open(path, offset, Some(length))?,
                    offset,
                };
                put_part(door, at, (upload, number), &mut bytes, length).await
            }
            Self::Stdin { held, .. } => {
                let mut bytes = Held { bytes: held, at: 0 };
                put_part(door, at, (upload, number), &mut bytes, length).await
            }
        }
    }
}

/// A multipart copy into the cell from `source`, kept in the S3 door's journal: the upload it
/// holds for the same object and source resumed past the parts the cell holds whose bytes are the
/// source's; one whose source changed aborted and a new one opened. Done or `BadDigest`, the
/// record goes; any other end leaves it for the copy run again.
async fn multipart_in(
    door: &mut S3Door,
    dir: &Path,
    at: &Location,
    origin: Origin,
    source: &mut Parts,
) -> Result<String, Error> {
    let journal = Journal::open(&dir.join("s3"))?;
    let named = format!("s3://{}/{}", at.bucket, at.key);
    let found = journal.find(&at.bucket, &at.key, &origin)?;
    let resumed = match found {
        Some(record) if record.origin == origin => Some(record),
        Some(record) => {
            abort(door, at, &record.upload).await?;
            journal.forget(&record)?;
            None
        }
        None => None,
    };
    let (record, held) = match resumed {
        Some(record) => match all_parts(door, at, &record.upload).await {
            Ok(held) => (record, held),
            Err(Error::S3(_, Code::NoSuchUpload, _)) => {
                journal.forget(&record)?;
                return Err(Error::S3(
                    "the upload".into(),
                    Code::NoSuchUpload,
                    "the upload the journal names is gone; run the copy again".into(),
                ));
            }
            Err(e) => return Err(e),
        },
        None => {
            let upload = create_upload(door, at).await?;
            let record = Record {
                bucket: at.bucket.clone(),
                key: at.key.clone(),
                origin,
                id: mantle_client::identity()?,
                upload,
            };
            journal.keep(&record)?;
            (record, Vec::new())
        }
    };
    let upload = record.upload.clone();
    let mut done: Vec<(u16, String)> = Vec::new();
    let mut sums: Vec<(Checksum, u64)> = Vec::new();
    let mut offset = 0u64;
    // A part read from the source to compare with one the cell holds, and not held: the next to
    // send.
    let mut pending: Option<u64> = None;
    // The parts the cell holds, from the first on, whose bytes the source still has: kept.
    for (number, size, etag) in &held {
        let expected = u16::try_from(done.len().saturating_add(1)).unwrap_or(u16::MAX);
        if *number != expected {
            break;
        }
        let length = source.next(offset).await?;
        if length != *size {
            pending = Some(length);
            break;
        }
        let (md5, crc) = source.digest(offset, length).await?;
        if checksum::etag(&md5).trim_matches('"') != etag {
            abort(door, at, &upload).await?;
            journal.forget(&record)?;
            return Err(Error::S3(
                "the upload".into(),
                Code::BadDigest,
                format!("part {number} the cell holds is not the source's bytes now"),
            ));
        }
        done.push((*number, etag.clone()));
        sums.push((crc, length));
        offset = offset.saturating_add(length);
    }
    if !held.is_empty() {
        writeln!(
            io::stderr().lock(),
            "{named}: resuming upload {upload} at {offset} bytes, {} parts",
            done.len()
        )
        .map_err(Error::Output)?;
    }
    loop {
        let length = match pending.take() {
            Some(length) => length,
            None => source.next(offset).await?,
        };
        if length == 0 && !done.is_empty() {
            break;
        }
        let number = u16::try_from(done.len().saturating_add(1))
            .ok()
            .filter(|n| *n <= mantle_s3::body::MAX_PARTS)
            .ok_or_else(|| Error::Usage("a source past S3's 10,000 parts".into()))?;
        let (etag, crc) = source
            .send(door, at, (&upload, number), (offset, length))
            .await?;
        offset = offset.saturating_add(length);
        done.push((number, etag));
        sums.push((crc, length));
        writeln!(
            io::stderr().lock(),
            "{named}: part {number} committed, {offset} bytes durable"
        )
        .map_err(Error::Output)?;
        if let Parts::File { length: whole, .. } = source
            && offset >= *whole
        {
            break;
        }
    }
    source.ended()?;
    let (etag, stated) = complete(door, at, &upload, &done).await?;
    let whole = checksum::full_object(&sums)
        .ok_or_else(|| Error::Runtime("the parts' CRC-64/NVMEs combined".into()))?;
    if stated.as_deref() != Some(whole.to_base64().as_str()) {
        journal.forget(&record)?;
        return Err(Error::Corrupt(format!(
            "{named}: its full-object CRC-64/NVME {stated:?}, where its parts' combine to {}, so it",
            whole.to_base64()
        )));
    }
    journal.forget(&record)?;
    Ok(etag)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mantle_s3::list::{Page, Request as ListRequest};
    use mantle_s3::response::{self as s3, Entry};

    /// Every document the listener writes for a listing or an upload reads back as written: its
    /// entries, its tokens and its markers (`mantle_s3::response`'s own writers).
    #[test]
    fn the_listeners_documents_read_back() {
        let buckets = [
            s3::Bucket {
                name: "a".into(),
                created: 1_700_000_000,
                region: "us-east-1".into(),
            },
            s3::Bucket {
                name: "b&<c".into(),
                created: 1_700_000_001,
                region: "us-east-1".into(),
            },
        ];
        let doc = s3::list_buckets(&s3::ListBuckets {
            owner: "o",
            buckets: &buckets,
            regions: true,
            continuation_token: Some("t1"),
            prefix: None,
        })
        .unwrap();
        let (listed, next) = list_buckets_document(doc.as_bytes()).unwrap();
        assert_eq!(
            listed,
            [
                (date(1_700_000_000), "a".to_owned()),
                (date(1_700_000_001), "b&<c".to_owned())
            ]
        );
        assert_eq!(next.as_deref(), Some("t1"));

        let part = s3::Part {
            number: 2,
            modified: 1_700_000_000,
            etag: "0123".into(),
            size: 5 << 20,
            checksum: None,
        };
        let doc = s3::list_parts(&s3::ListParts {
            bucket: "b",
            key: "k",
            upload_id: "u",
            part_number_marker: 1,
            next: Some(2),
            max_parts: 1,
            parts: std::slice::from_ref(&part),
            initiator: "i",
            owner: "o",
            checksum: None,
        })
        .unwrap();
        assert_eq!(
            list_parts_document(doc.as_bytes()).unwrap(),
            (vec![(2, 5 << 20, "0123".to_owned())], Some(2))
        );

        let upload = s3::Upload {
            key: "k \u{1}".into(),
            upload_id: "u1".into(),
            initiated: 1_700_000_000,
            initiator: "i".into(),
            owner: "o".into(),
            checksum: None,
        };
        let doc = s3::list_multipart_uploads(&s3::ListUploads {
            bucket: "b",
            prefix: "",
            delimiter: None,
            max_uploads: 1,
            key_marker: None,
            upload_id_marker: None,
            url: false,
            uploads: std::slice::from_ref(&upload),
            common_prefixes: &[],
            truncated: true,
            next: s3::Next {
                key: "k \u{1}",
                id: "u1",
            },
        })
        .unwrap();
        assert_eq!(
            list_uploads_document(doc.as_bytes()).unwrap(),
            (
                vec![(date(1_700_000_000), "u1".to_owned(), "k \u{1}".to_owned())],
                Some(("k \u{1}".to_owned(), "u1".to_owned()))
            )
        );

        let doc = s3::initiate_multipart_upload("b", "k", "u9");
        assert_eq!(initiated_document(doc.as_bytes()).unwrap(), "u9");
        let doc = s3::complete_multipart_upload(&s3::Completed {
            location: "/b/k",
            bucket: "b",
            key: "k",
            etag: "ab-2",
            checksum: Some(s3::ObjectChecksum {
                algorithm: Algorithm::Crc64Nvme,
                kind: mantle_s3::checksum::ChecksumType::FullObject,
                value: "AAAAAAAAAAA=",
            }),
        });
        assert_eq!(
            completed_document(doc.as_bytes()).unwrap(),
            ("ab-2".to_owned(), Some("AAAAAAAAAAA=".to_owned()))
        );
    }

    /// `n` octets the listing writer escapes at its widest, each a control character (`&#x1f;`).
    fn widest(n: usize) -> String {
        "\u{1f}".repeat(n)
    }

    /// The last second an S3 time holds, 9999-12-31T23:59:59Z, whose year is written in full.
    const LAST: i64 = 253_402_300_799;

    /// The widest checksum algorithm and type a listing names.
    const NAMED: Option<(Algorithm, mantle_s3::checksum::ChecksumType)> = Some((
        Algorithm::Crc64Nvme,
        mantle_s3::checksum::ChecksumType::FullObject,
    ));

    fn widest_entry() -> Entry {
        Entry {
            modified: LAST,
            etag: widest(mantle_protocol::object::MAX_TAG),
            size: u64::MAX,
            owner: widest(mantle_s3::account::MAX_OWNER),
            checksum: NAMED,
        }
    }

    fn widest_upload(key: String) -> s3::Upload {
        s3::Upload {
            key,
            upload_id: widest(mantle_protocol::object::MAX_TAG),
            initiated: LAST,
            initiator: widest(mantle_s3::account::MAX_OWNER),
            owner: widest(mantle_s3::account::MAX_OWNER),
            checksum: NAMED,
        }
    }

    /// A ListObjectsV2 page of `keys` with every element of its head at its widest.
    fn objects(keys: &[String]) -> String {
        let key = widest(mantle_s3::route::MAX_KEY);
        let token =
            mantle_s3::list::continuation_token(&mantle_s3::list::Resume::After(key.clone()));
        let page = Page {
            contents: keys.iter().map(|k| (k.clone(), widest_entry())).collect(),
            common_prefixes: Vec::new(),
            truncated: true,
            next: Some(mantle_s3::list::Resume::After(key.clone())),
        };
        let bucket = "b".repeat(mantle_s3::route::MAX_BUCKET);
        s3::list_objects_v2(
            &s3::ListV2 {
                bucket: &bucket,
                request: ListRequest {
                    prefix: &key,
                    delimiter: Some(&key),
                    start: None,
                    max_keys: mantle_s3::list::MAX_KEYS,
                    paging: mantle_s3::list::Paging::Token,
                },
                continuation_token: Some(&token),
                start_after: Some(&key),
                url: false,
                fetch_owner: true,
            },
            &page,
        )
        .unwrap()
    }

    /// A ListMultipartUploads page of uploads at `keys` with every element of its head at its
    /// widest.
    fn uploads(keys: &[String]) -> String {
        let key = widest(mantle_s3::route::MAX_KEY);
        let id = widest(mantle_protocol::object::MAX_TAG);
        let uploads: Vec<s3::Upload> = keys.iter().map(|k| widest_upload(k.clone())).collect();
        let bucket = "b".repeat(mantle_s3::route::MAX_BUCKET);
        s3::list_multipart_uploads(&s3::ListUploads {
            bucket: &bucket,
            prefix: &key,
            delimiter: Some(&key),
            max_uploads: mantle_s3::list::MAX_KEYS,
            key_marker: Some(&key),
            upload_id_marker: Some(&id),
            url: false,
            uploads: &uploads,
            common_prefixes: &[],
            truncated: true,
            next: s3::Next { key: &key, id: &id },
        })
        .unwrap()
    }

    /// A ListParts page of `parts` parts, each and its head at its widest.
    fn parts(parts: usize) -> String {
        let part = s3::Part {
            number: u16::MAX,
            modified: LAST,
            etag: widest(mantle_protocol::object::MAX_TAG),
            size: u64::MAX,
            checksum: Some(Checksum {
                algorithm: Algorithm::Sha512,
                bytes: vec![0; 64],
            }),
        };
        let all = vec![part; parts];
        let bucket = "b".repeat(mantle_s3::route::MAX_BUCKET);
        let owner = widest(mantle_s3::account::MAX_OWNER);
        s3::list_parts(&s3::ListParts {
            bucket: &bucket,
            key: &widest(mantle_s3::route::MAX_KEY),
            upload_id: &widest(mantle_protocol::object::MAX_TAG),
            part_number_marker: u16::MAX,
            next: Some(u16::MAX),
            max_parts: mantle_s3::list::MAX_KEYS,
            parts: &all,
            initiator: &owner,
            owner: &owner,
            checksum: NAMED,
        })
        .unwrap()
    }

    /// `ENTRY` is the widest entry the listing writer puts beside its key, a `Contents` or an
    /// `Upload` at its widest, octet for octet; and `HEAD` holds every listing's head at its widest.
    #[test]
    fn the_entry_and_head_bounds_are_the_writers_own() {
        let contents = objects(&[String::new()]).len() - objects(&[]).len();
        let upload = uploads(&[String::new()]).len() - uploads(&[]).len();
        assert_eq!(
            ENTRY,
            contents.max(upload),
            "Contents {contents}, Upload {upload}"
        );
        for (what, head) in [
            ("ListObjectsV2", objects(&[]).len()),
            ("ListMultipartUploads", uploads(&[]).len()),
            ("ListParts", parts(0).len()),
        ] {
            assert!(head <= HEAD, "{what}'s head of {head} octets past {HEAD}");
        }
    }

    /// The widest page of each listing the door reads fits `DOCUMENT`: 1,000 keys, uploads or parts
    /// at their widest, and `MAX_BUCKETS` buckets.
    #[test]
    fn every_widest_page_fits_the_document_bound() {
        let keys = vec![widest(mantle_s3::route::MAX_KEY); mantle_s3::list::MAX_KEYS];
        let buckets: Vec<s3::Bucket> = (0..mantle_protocol::object::MAX_BUCKETS)
            .map(|_| s3::Bucket {
                name: "b".repeat(mantle_s3::route::MAX_BUCKET),
                created: LAST,
                // A region of 63 octets, the most a location constraint names (mantle-s3's
                // `body::MAX_REGION`).
                region: "r".repeat(63),
            })
            .collect();
        let owner = widest(mantle_s3::account::MAX_OWNER);
        let token = widest(mantle_s3::route::MAX_KEY);
        for (what, page) in [
            ("ListObjectsV2", objects(&keys).len()),
            ("ListMultipartUploads", uploads(&keys).len()),
            ("ListParts", parts(mantle_s3::list::MAX_KEYS).len()),
            (
                "ListBuckets",
                s3::list_buckets(&s3::ListBuckets {
                    owner: &owner,
                    buckets: &buckets,
                    regions: true,
                    continuation_token: Some(&token),
                    prefix: Some(&token),
                })
                .unwrap()
                .len(),
            ),
        ] {
            assert!(
                page <= DOCUMENT,
                "{what}'s widest page of {page} octets past {DOCUMENT}"
            );
        }
    }

    /// A ListObjectsV2 page the listener writes: its prefixes, each key's time, size and name, and
    /// its next token only where it is truncated.
    #[test]
    fn a_page_of_keys_reads_back() {
        let entry = Entry {
            modified: 1_700_000_000,
            etag: "e".into(),
            size: 17,
            owner: "o".into(),
            checksum: None,
        };
        let page = Page {
            contents: vec![("p/k\r1".to_owned(), entry)],
            common_prefixes: vec!["p/q/".to_owned()],
            truncated: true,
            next: Some(mantle_s3::list::Resume::After("p/k\r1".to_owned())),
        };
        let doc = s3::list_objects_v2(
            &s3::ListV2 {
                bucket: "b",
                request: ListRequest {
                    prefix: "p/",
                    delimiter: Some("/"),
                    start: None,
                    max_keys: 2,
                    paging: mantle_s3::list::Paging::Token,
                },
                continuation_token: None,
                start_after: None,
                url: false,
                fetch_owner: true,
            },
            &page,
        )
        .unwrap();
        let read = list_objects_document(doc.as_bytes()).unwrap();
        assert_eq!(read.prefixes, ["p/q/"]);
        assert_eq!(read.keys, [(date(1_700_000_000), 17, "p/k\r1".to_owned())]);
        assert!(read.next.is_some());
    }

    /// An error document's code is typed as S3 names it, one S3 does not name is said as it came,
    /// and an error with no body is its status.
    #[test]
    fn an_error_is_typed_by_its_code() {
        let doc = |code: &str| {
            format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code>\
                 <Message>m</Message><Resource>/b</Resource><RequestId>r</RequestId></Error>"
            )
        };
        assert!(matches!(
            error_of("DeleteBucket", 409, doc("BucketNotEmpty").as_bytes()),
            Error::S3(_, Code::BucketNotEmpty, m) if m == "m"
        ));
        assert!(matches!(
            error_of("GetObject", 400, doc("NoSuchThing").as_bytes()),
            Error::UnlistedCode(_, c, _) if c == "NoSuchThing"
        ));
        assert!(matches!(
            error_of("HeadObject", 404, b""),
            Error::Status(_, 404)
        ));
    }

    /// An `aws-chunked` body's length is the octets `send_chunked` writes: each piece's head, its
    /// data and its end, and the last chunk with its trailer.
    #[test]
    fn a_chunked_body_is_as_long_as_it_is_written() {
        for length in [
            0u64,
            1,
            PIECE as u64,
            PIECE as u64 + 1,
            3 * PIECE as u64 + 7,
        ] {
            let mut scratch = Vec::new();
            let counted = chunked_length(length, &mut scratch).unwrap();
            let mut written = 0u64;
            let mut left = length;
            while left > 0 {
                let n = left.min(PIECE as u64);
                scratch.clear();
                http_write::chunk(&mut scratch, usize::try_from(n).unwrap());
                written += scratch.len() as u64 + n + 2;
                left -= n;
            }
            let sum = checksum::checksum(TRAILER, &[]).unwrap().to_base64();
            scratch.clear();
            http_write::last(&mut scratch, &[(TRAILER.header(), sum.as_bytes())]).unwrap();
            written += scratch.len() as u64;
            assert_eq!(counted, written, "{length}");
        }
    }

    /// A file's parts: S3's 8 MiB, or as many more as 10,000 parts need.
    #[test]
    fn a_files_part_fits_ten_thousand_parts() {
        assert_eq!(file_part(PART).unwrap(), PART);
        assert_eq!(file_part(10_000 * PART).unwrap(), PART);
        assert_eq!(file_part(10_000 * PART + 1).unwrap(), PART + 1);
        assert!(file_part(mantle_s3::body::MAX_GET + 1).is_err());
    }
}
