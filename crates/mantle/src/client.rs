//! `mantle`'s object commands (docs/design/gateway.md §6): the native client's command line
//! (`mantle_client`), run from a client directory `mantle init client` made, whose `config` names
//! the client's id, its certificate and key, the authority's certificate and each node where it
//! listens. An object is `s3://BUCKET/KEY`, a bucket `s3://BUCKET`; anything else is a local file,
//! and `-` standard input or output.
//!
//! **Its runtime** is one shard on this machine's calibration, kept in the client's directory as a
//! node keeps its own (`mantle_node::process::calibration`), sized by the client's sockets. Files are
//! read and written on one worker of hyper-rt's blocking pool, so the shard keeps the client's
//! connections moving while a disk is slow; standard input and output on hyper-rt's own threads, a
//! piece a buffer.
//!
//! **A copy into the cell** is a native upload (gateway.md §2.1; `mantle_client::Client::upload`):
//! runs the node cuts, each committed before the next is sent, the object committed at the end
//! only if every byte's CRC-64/NVME is the one the client read. The upload is kept in the client's
//! journal (`crate::journal`) before its first run, and a copy run again after a crash or a kill
//! finds it and goes on from the node's durable offset, no committed run sent again; each run's
//! commit is told on standard error. An object one run holds is put whole.
//!
//! **Its memory** is the client's (`mantle_client::memory`) and a piece a stream, whatever an
//! object's size, but where a source is read once (standard input): each run is held until it
//! commits, to be sent again if its exchange is lost, so such a copy holds one run, of the AWS
//! CLI's `multipart_chunksize` ([`PART`]), and a piece read past it.
//!
//! **A copy to a file** is written under a temporary name beside it, flushed (`sync_data`, the
//! platform's full flush), renamed over its place and its directory flushed (CLAUDE.md §6), so the
//! file is the whole object or untouched; a copy of more than one put's bytes is got in ranges of
//! at most one put's, each byte held to the object's full-object checksum where it keeps one.

use std::fmt;
use std::fs::File;
use std::io::{Read as _, Seek as _, SeekFrom, Write};
use std::path::{Path, PathBuf};

use hyper_rt::blocking;
use hyper_rt::machine::calibration::Policy;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::sync::OneshotReceiver;
use mantle_client::{
    CertificateDer, Client, ClientError, Credentials, Native, PrivateKeyDer, PrivatePkcs8KeyDer,
    Rewind, Settings, Sink, Source, TRAILER, Watch,
};
use mantle_protocol::object::{Headers, List, Listing, Op, Progress, Reply, Status};

use crate::journal::{Journal, JournalError, Origin, Record};
use mantle_s3::checksum::{Algorithm, Hasher};

/// The blocking pool's share a file's reads and writes run in, one at a time: an operation moves
/// one piece at a time.
const FILES: &str = "files";

/// The run a copy from standard input holds, its source read once: the AWS CLI's default
/// `multipart_chunksize` and `multipart_threshold` of 8 MB, a megabyte 1024² octets to it (AWS CLI
/// S3 configuration reference; docs/research/73). Its runs are this long, so a stream of unknown
/// length holds 10,000 of them, 80 GiB, as the AWS CLI's parts do.
pub const PART: u64 = 8 << 20;

/// What an object command does.
pub enum Command {
    /// CreateBucket.
    MakeBucket(String),
    /// DeleteBucket.
    RemoveBucket(String),
    /// ListBuckets, or ListObjectsV2 of a bucket's keys under a prefix, a level of `/` at a time.
    List(Option<Location>),
    /// An object's bytes from one place to another, one of them the cell; standard input's length
    /// where stated.
    Copy(Place, Place, Option<u64>),
    /// DeleteObject.
    Remove(Location),
    /// HeadObject.
    Head(Location),
    /// CreateMultipartUpload, its parts' checksum the client's trailer's as one full-object value.
    CreateUpload(Location),
    /// UploadPart: part `number` of `upload`, from a file, or `length` bytes of standard input.
    UploadPart {
        at: Location,
        upload: String,
        number: u16,
        from: Place,
        length: Option<u64>,
    },
    /// CompleteMultipartUpload of the parts listed, each its number and ETag.
    Complete {
        at: Location,
        upload: String,
        parts: Vec<(u16, String)>,
    },
    /// AbortMultipartUpload.
    Abort { at: Location, upload: String },
    /// ListParts.
    Parts { at: Location, upload: String },
    /// ListMultipartUploads.
    Uploads(String),
}

/// An object's or a bucket's place in the cell: `s3://BUCKET/KEY`, its key empty for a bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    pub bucket: String,
    pub key: String,
}

/// Where a copy's bytes come from or go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Place {
    Cell(Location),
    File(PathBuf),
    /// Standard input or output.
    Standard,
}

/// `s3://BUCKET[/KEY]`, or none for any other text.
pub fn location(text: &str) -> Option<Location> {
    let rest = text.strip_prefix("s3://")?;
    let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() {
        return None;
    }
    Some(Location {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
    })
}

/// A copy's place: the cell's for `s3://`, standard input or output for `-`, else a file.
pub fn place(text: &str) -> Place {
    match (text, location(text)) {
        ("-", _) => Place::Standard,
        (_, Some(location)) => Place::Cell(location),
        _ => Place::File(PathBuf::from(text)),
    }
}

/// Why an object command did not do what it was asked.
#[derive(Debug)]
pub enum Error {
    /// The client's directory, or a file it names, does not read.
    Config(PathBuf, String),
    Usage(String),
    Client(ClientError),
    /// A node answered, and not done: what was asked, and its status.
    Answered(String, Status),
    File(PathBuf, std::io::Error),
    Runtime(String),
    Output(std::io::Error),
    Journal(JournalError),
}

impl From<JournalError> for Error {
    fn from(e: JournalError) -> Self {
        Self::Journal(e)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(path, why) => write!(f, "{}: {why}", path.display()),
            Self::Usage(why) => f.write_str(why),
            Self::Client(e) => write!(f, "{e}"),
            Self::Answered(what, status) => write!(f, "{what}: {status:?}"),
            Self::File(path, e) => write!(f, "{}: {e}", path.display()),
            Self::Runtime(why) => write!(f, "the runtime: {why}"),
            Self::Output(e) => write!(f, "writing output: {e}"),
            Self::Journal(e) => write!(f, "the journal: {e}"),
        }
    }
}

impl From<ClientError> for Error {
    fn from(e: ClientError) -> Self {
        Self::Client(e)
    }
}

impl From<hyper_rt::RtError> for Error {
    fn from(e: hyper_rt::RtError) -> Self {
        Self::Runtime(format!("{e:?}"))
    }
}

/// A client's directory as its `config` names it.
struct Configured {
    dir: PathBuf,
    id: u64,
    credentials: Credentials,
    nodes: Vec<(u64, std::net::SocketAddr)>,
}

/// The client's directory `dir`: its `config`'s id, certificate, key (readable by its owner alone,
/// as a node's is: `mantle_node::secret`), the authority's certificate and each node.
fn configured(dir: &Path) -> Result<Configured, Error> {
    let path = dir.join("config");
    let bad = |why: String| Error::Config(path.clone(), why);
    let text = std::fs::read_to_string(&path).map_err(|e| bad(e.to_string()))?;
    let (mut id, mut authority, mut certificate, mut key) = (None, None, None, None);
    let mut nodes = Vec::new();
    for line in text
        .lines()
        .map(|l| l.split('#').next().unwrap_or("").trim())
        .filter(|l| !l.is_empty())
    {
        let (name, value) = line
            .split_once('=')
            .map(|(n, v)| (n.trim(), v.trim()))
            .ok_or_else(|| bad(format!("not KEY = VALUE: {line}")))?;
        let file = || dir.join(value);
        match name {
            "id" => id = Some(value.parse().map_err(|_| bad(format!("an id: {value}")))?),
            "authority" => authority = Some(file()),
            "certificate" => certificate = Some(file()),
            "key" => key = Some(file()),
            "node" => {
                let (node, quic) = value
                    .split_once(char::is_whitespace)
                    .ok_or_else(|| bad(format!("node = ID ADDRESS: {value}")))?;
                nodes.push((
                    node.parse()
                        .map_err(|_| bad(format!("a node's id: {node}")))?,
                    quic.trim()
                        .parse()
                        .map_err(|_| bad(format!("a node's address: {quic}")))?,
                ));
            }
            other => return Err(bad(format!("a key no client states: {other}"))),
        }
    }
    let (Some(id), Some(authority), Some(certificate), Some(key)) =
        (id, authority, certificate, key)
    else {
        return Err(bad(
            "id, authority, certificate and key are each stated".into()
        ));
    };
    if nodes.is_empty() {
        return Err(bad("no node".into()));
    }
    let read = |path: &Path| std::fs::read(path).map_err(|e| Error::File(path.to_path_buf(), e));
    let key = mantle_node::secret::read(&key).map_err(|e| Error::Config(key, e.to_string()))?;
    Ok(Configured {
        dir: dir.to_path_buf(),
        id,
        credentials: Credentials {
            chain: vec![CertificateDer::from(read(&certificate)?)],
            key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key)),
            roots: vec![CertificateDer::from(read(&authority)?)],
        },
        nodes,
    })
}

/// One shard on this machine's calibration, kept in `dir`, its interests the client's sockets'.
fn runtime(dir: &Path, nodes: usize) -> Result<LocalRuntime, Error> {
    let budget = mantle_node::derive::probe_budget().map_err(|e| Error::Runtime(e.to_string()))?;
    let calibration = mantle_node::process::calibration(dir, budget)
        .map_err(|e| Error::Runtime(e.to_string()))?;
    let constants = calibration
        .constants(&Policy {
            reserved_cores: 0,
            lateness_tolerance_ns: None,
            latency_objective_ns: None,
        })
        .map_err(|unmet| Error::Runtime(format!("{unmet:?}")))?;
    // The client's one task, its waits' timer and its driver's.
    let mut config = RuntimeConfig::from_calibration(&calibration, &constants, 1, 2);
    config.shards = 1;
    config.pin = false;
    config.cores.clear();
    let sockets = mantle_client::sockets(nodes)?;
    config.interests_per_shard = config.interests_per_shard.max(sockets.saturating_mul(2));
    Ok(LocalRuntime::new(&config)?)
}

/// Runs `command` as the client of the directory `dir`, its lines to `out`.
pub fn run(dir: &Path, command: Command, mut out: impl Write + 'static) -> Result<(), Error> {
    let configured = configured(dir)?;
    let mut rt = runtime(&configured.dir, configured.nodes.len())?;
    blocking::start(blocking::Config {
        workers: 1,
        shares: vec![blocking::Share::new(FILES, 1)],
    })?;
    let ran = rt
        .block_on(async move {
            let Configured {
                dir,
                id,
                credentials,
                nodes,
            } = configured;
            let transport_bytes = mantle_client::least_transport(nodes.len())?;
            let mut client = Client::open(Settings {
                id,
                bind: unspecified(&nodes),
                credentials,
                nodes,
                transport_bytes,
            })
            .await?;
            command_on(&mut client, &dir, command, &mut out).await
        })
        .map_err(Error::from);
    blocking::stop()?;
    ran?
}

/// The address the client binds: any of its family's, the port the OS gives.
fn unspecified(nodes: &[(u64, std::net::SocketAddr)]) -> std::net::SocketAddr {
    match nodes.first() {
        Some((_, std::net::SocketAddr::V6(_))) => "[::]:0".parse(),
        _ => "0.0.0.0:0".parse(),
    }
    .unwrap_or(std::net::SocketAddr::from(([0, 0, 0, 0], 0)))
}

/// No deadline: every wait is bounded by the transport, an exchange by its progress period and a
/// dial by its handshake's idle timeout, and every ask by the client's attempts.
const NO_DEADLINE: u64 = u64::MAX;

/// What a node answered, done or the status it answered as the error.
fn done(what: &str, reply: Reply) -> Result<Reply, Error> {
    if reply.status == Status::Done {
        Ok(reply)
    } else {
        Err(Error::Answered(what.to_owned(), reply.status))
    }
}

fn line(out: &mut impl Write, text: std::fmt::Arguments<'_>) -> Result<(), Error> {
    out.write_fmt(text).map_err(Error::Output)?;
    out.write_all(b"\n").map_err(Error::Output)
}

/// Whole seconds as ISO 8601, as S3's listings date.
fn date(seconds: u64) -> String {
    i64::try_from(seconds)
        .ok()
        .and_then(|s| s.checked_mul(1000))
        .and_then(mantle_s3::time::iso8601)
        .unwrap_or_else(|| seconds.to_string())
}

async fn command_on(
    client: &mut Client,
    dir: &Path,
    command: Command,
    out: &mut impl Write,
) -> Result<(), Error> {
    match command {
        Command::MakeBucket(bucket) => {
            let op = Op::Create {
                bucket: bucket.clone(),
                id: mantle_client::identity()?,
            };
            done("CreateBucket", client.call(&op, NO_DEADLINE).await?)?;
            line(out, format_args!("made s3://{bucket}"))
        }
        Command::RemoveBucket(bucket) => {
            let op = Op::DeleteBucket {
                bucket: bucket.clone(),
                id: mantle_client::identity()?,
            };
            done("DeleteBucket", client.call(&op, NO_DEADLINE).await?)?;
            line(out, format_args!("removed s3://{bucket}"))
        }
        Command::List(None) => list_buckets(client, out).await,
        Command::List(Some(at)) => list_keys(client, &at, out).await,
        Command::Copy(from, to, size) => copy(client, dir, (from, to), size, out).await,
        Command::Remove(at) => {
            let op = Op::Delete {
                bucket: at.bucket.clone(),
                key: at.key.clone(),
                id: mantle_client::identity()?,
                headers: Headers::default(),
            };
            done("DeleteObject", client.call(&op, NO_DEADLINE).await?)?;
            line(out, format_args!("removed s3://{}/{}", at.bucket, at.key))
        }
        Command::Head(at) => {
            let op = Op::Head {
                bucket: at.bucket.clone(),
                key: at.key.clone(),
                headers: Headers::default(),
            };
            let reply = done("HeadObject", client.call(&op, NO_DEADLINE).await?)?;
            line(out, format_args!("size {}", reply.size))?;
            line(out, format_args!("etag \"{}\"", reply.etag))?;
            line(out, format_args!("modified {}", date(reply.modified)))?;
            if !reply.version.is_empty() {
                line(out, format_args!("version {}", reply.version))?;
            }
            if let Some(kept) = &reply.checksum
                && let Some(algorithm) = Algorithm::from_code(kept.algorithm)
            {
                let value = mantle_s3::checksum::Checksum {
                    algorithm,
                    bytes: kept.value.clone(),
                }
                .to_base64();
                let parts = if kept.parts == 0 {
                    String::new()
                } else {
                    format!("-{}", kept.parts)
                };
                line(
                    out,
                    format_args!("checksum {} {value}{parts}", algorithm.name()),
                )?;
            }
            Ok(())
        }
        Command::CreateUpload(at) => {
            let reply = create_upload(client, &at).await?;
            line(out, format_args!("{}", reply.upload))
        }
        Command::UploadPart {
            at,
            upload,
            number,
            from,
            length,
        } => {
            let reply = match (from, length) {
                (Place::File(path), None) => {
                    let mut source = FileSource::open(&path, 0, None)?;
                    let length = source.left;
                    put_part(client, &at, (&upload, number), &mut source, length).await?
                }
                (Place::Standard, Some(length)) => {
                    let mut source = Stdin::take()?;
                    put_part(client, &at, (&upload, number), &mut source, length).await?
                }
                _ => {
                    return Err(Error::Usage(
                        "a part comes from a file, or from standard input with its --length".into(),
                    ));
                }
            };
            line(out, format_args!("{number} {}", reply.etag))
        }
        Command::Complete { at, upload, parts } => {
            let reply = complete(client, &at, &upload, parts).await?;
            line(out, format_args!("etag \"{}\"", reply.etag))
        }
        Command::Abort { at, upload } => {
            let op = Op::Abort {
                bucket: at.bucket,
                key: at.key,
                upload: upload.clone(),
                id: mantle_client::identity()?,
            };
            done("AbortMultipartUpload", client.call(&op, NO_DEADLINE).await?)?;
            line(out, format_args!("aborted {upload}"))
        }
        Command::Parts { at, upload } => list_parts(client, &at, &upload, out).await,
        Command::Uploads(bucket) => list_uploads(client, &bucket, out).await,
    }
}

async fn list_buckets(client: &mut Client, out: &mut impl Write) -> Result<(), Error> {
    let mut after = None;
    loop {
        let op = Op::ListBuckets {
            prefix: String::new(),
            after: after.take(),
            max: u32::try_from(mantle_protocol::object::MAX_BUCKETS).unwrap_or(u32::MAX),
        };
        let reply = done("ListBuckets", client.call(&op, NO_DEADLINE).await?)?;
        let Some(Listing::Buckets(page)) = reply.listing else {
            return Err(Error::Answered("ListBuckets".into(), reply.status));
        };
        for (bucket, created) in &page.buckets {
            line(out, format_args!("{} {bucket}", date(*created)))?;
        }
        match page.next {
            Some(next) => after = Some(next),
            None => return Ok(()),
        }
    }
}

async fn list_keys(client: &mut Client, at: &Location, out: &mut impl Write) -> Result<(), Error> {
    let mut start = None;
    loop {
        let op = Op::List(List {
            bucket: at.bucket.clone(),
            prefix: at.key.clone(),
            delimiter: "/".into(),
            start: start.take(),
            max_keys: u32::try_from(mantle_s3::list::MAX_KEYS).unwrap_or(u32::MAX),
            fetch_owner: false,
        });
        let reply = done("ListObjectsV2", client.call(&op, NO_DEADLINE).await?)?;
        let Some(Listing::Keys(page)) = reply.listing else {
            return Err(Error::Answered("ListObjectsV2".into(), reply.status));
        };
        for prefix in &page.common_prefixes {
            line(out, format_args!("PRE {prefix}"))?;
        }
        for listed in &page.contents {
            line(
                out,
                format_args!("{} {} {}", date(listed.modified), listed.size, listed.key),
            )?;
        }
        match (page.truncated, page.next) {
            (true, Some(next)) => start = Some(next),
            _ => return Ok(()),
        }
    }
}

async fn list_parts(
    client: &mut Client,
    at: &Location,
    upload: &str,
    out: &mut impl Write,
) -> Result<(), Error> {
    let mut after = 0u16;
    loop {
        let op = Op::ListParts {
            bucket: at.bucket.clone(),
            key: at.key.clone(),
            upload: upload.to_owned(),
            after,
            max: u32::try_from(mantle_protocol::object::MAX_PAGE_PARTS).unwrap_or(u32::MAX),
        };
        let reply = done("ListParts", client.call(&op, NO_DEADLINE).await?)?;
        let Some(Listing::Parts(page)) = reply.listing else {
            return Err(Error::Answered("ListParts".into(), reply.status));
        };
        for part in &page.parts {
            line(
                out,
                format_args!("{} {} \"{}\"", part.number, part.size, part.etag),
            )?;
        }
        match (page.truncated, page.next) {
            (true, Some(next)) => after = next,
            _ => return Ok(()),
        }
    }
}

async fn list_uploads(
    client: &mut Client,
    bucket: &str,
    out: &mut impl Write,
) -> Result<(), Error> {
    let (mut key_marker, mut id_marker) = (None, None);
    loop {
        let op = Op::ListUploads {
            bucket: bucket.to_owned(),
            prefix: String::new(),
            delimiter: String::new(),
            key_marker: key_marker.take(),
            id_marker: id_marker.take(),
            max: u32::try_from(mantle_protocol::object::MAX_PAGE_PARTS).unwrap_or(u32::MAX),
        };
        let reply = done("ListMultipartUploads", client.call(&op, NO_DEADLINE).await?)?;
        let Some(Listing::Uploads(page)) = reply.listing else {
            return Err(Error::Answered("ListMultipartUploads".into(), reply.status));
        };
        for listed in &page.uploads {
            line(
                out,
                format_args!("{} {} {}", date(listed.initiated), listed.id, listed.key),
            )?;
        }
        match (page.truncated, page.next) {
            (true, Some((key, id))) => {
                key_marker = Some(key);
                id_marker = Some(id);
            }
            _ => return Ok(()),
        }
    }
}

/// A multipart upload at `at`, its parts carrying the client's trailer, combined as one
/// full-object value (S3: CRC-64/NVME is a full-object type; docs/research/73).
async fn create_upload(client: &mut Client, at: &Location) -> Result<Reply, Error> {
    let op = Op::CreateUpload {
        bucket: at.bucket.clone(),
        key: at.key.clone(),
        id: mantle_client::identity()?,
        checksum: Some((TRAILER, true)),
    };
    done(
        "CreateMultipartUpload",
        client.call(&op, NO_DEADLINE).await?,
    )
}

async fn put_part<S: Source>(
    client: &mut Client,
    at: &Location,
    (upload, number): (&str, u16),
    source: &mut S,
    length: u64,
) -> Result<Reply, Error> {
    let op = Op::UploadPart {
        bucket: at.bucket.clone(),
        key: at.key.clone(),
        upload: upload.to_owned(),
        number,
        id: mantle_client::identity()?,
        length,
        checksum: None,
        md5: None,
        trailing: Some(TRAILER),
    };
    done("UploadPart", client.put(&op, source, NO_DEADLINE).await?)
}

async fn complete(
    client: &mut Client,
    at: &Location,
    upload: &str,
    parts: Vec<(u16, String)>,
) -> Result<Reply, Error> {
    let op = Op::Complete {
        bucket: at.bucket.clone(),
        key: at.key.clone(),
        upload: upload.to_owned(),
        id: mantle_client::identity()?,
        headers: Headers::default(),
        parts,
    };
    done(
        "CompleteMultipartUpload",
        client.call(&op, NO_DEADLINE).await?,
    )
}

/// An object's bytes from `from` to `to`, one of them the cell's.
async fn copy(
    client: &mut Client,
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
            let origin = Origin::file(&path)?;
            let Origin::File { length, .. } = origin else {
                return Err(Error::Usage("a file's copy".into()));
            };
            let mut source = FileSource::open(&path, 0, None)?;
            let reply = upload(client, dir, &at, origin, |client, native, watch| {
                Box::pin(async move {
                    client
                        .upload(native, (&mut source, length), watch, NO_DEADLINE)
                        .await
                })
            })
            .await?;
            line(
                out,
                format_args!(
                    "{} -> s3://{}/{} etag \"{}\"",
                    path.display(),
                    at.bucket,
                    at.key,
                    reply.etag
                ),
            )
        }
        (Place::Standard, Place::Cell(at)) => {
            let mut stdin = Stdin::take()?;
            let reply = upload(
                client,
                dir,
                &at,
                Origin::Standard,
                |client, native, watch| {
                    Box::pin(async move {
                        client
                            .upload_once(native, (&mut stdin, size), (PART, watch), NO_DEADLINE)
                            .await
                    })
                },
            )
            .await?;
            line(
                out,
                format_args!("- -> s3://{}/{} etag \"{}\"", at.bucket, at.key, reply.etag),
            )
        }
        (Place::Cell(at), Place::File(path)) => {
            get_to_file(client, &at, &path).await?;
            line(
                out,
                format_args!("s3://{}/{} -> {}", at.bucket, at.key, path.display()),
            )
        }
        (Place::Cell(at), Place::Standard) => {
            let mut sink = Stdout::take()?;
            get_object(client, &at, &mut sink).await?;
            sink.flush().await
        }
        _ => Err(Error::Usage(
            "a copy is from a file or standard input to s3://BUCKET/KEY, or back".into(),
        )),
    }
}

/// What a copy's upload runs: the client's resumable upload of its source, given the client, the
/// upload's object and the journal's watch.
type Uploading<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Reply, ClientError>> + 'a>>;

/// A copy into the cell at `at` from `origin`'s source, as a native upload the journal under `dir`
/// keeps: the upload it holds for the same destination and source resumed, where the source is as
/// its record says; one whose source changed aborted and a new one opened. Done or `BadDigest`,
/// the record goes; any other end leaves it for the copy run again.
async fn upload<'c, F>(
    client: &'c mut Client,
    dir: &Path,
    at: &Location,
    origin: Origin,
    go: F,
) -> Result<Reply, Error>
where
    F: for<'b> FnOnce(&'b mut Client, &'b Native, &'b mut Journaled) -> Uploading<'b>,
{
    let journal = Journal::open(dir)?;
    let found = journal.find(&at.bucket, &at.key, &origin)?;
    let resumed = match found {
        Some(record) if record.origin == origin => Some(record),
        Some(record) => {
            abort_upload(client, at, &record.upload).await?;
            journal.forget(&record)?;
            None
        }
        None => None,
    };
    let record = match resumed {
        Some(record) => record,
        None => Record {
            bucket: at.bucket.clone(),
            key: at.key.clone(),
            origin,
            id: mantle_client::identity()?,
            upload: String::new(),
        },
    };
    let native = Native {
        bucket: at.bucket.clone(),
        key: at.key.clone(),
        id: record.id,
        upload: (!record.upload.is_empty()).then(|| record.upload.clone()),
        checksum: TRAILER,
        headers: Headers::default(),
    };
    let mut watch = Journaled {
        journal,
        record,
        at: format!("s3://{}/{}", at.bucket, at.key),
        resumed: native.upload.is_some(),
    };
    let reply = go(client, &native, &mut watch).await?;
    let Journaled {
        journal, record, ..
    } = watch;
    match reply.status {
        Status::Done => {
            journal.forget(&record)?;
            Ok(reply)
        }
        // A source whose bytes are not the runs': nothing committed, and the upload, holding
        // another source's runs, is aborted.
        Status::BadDigest => {
            abort_upload(client, at, &record.upload).await?;
            journal.forget(&record)?;
            Err(Error::Answered("the upload".into(), Status::BadDigest))
        }
        Status::NoSuchUpload => {
            journal.forget(&record)?;
            Err(Error::Answered("the upload".into(), Status::NoSuchUpload))
        }
        status => Err(Error::Answered("the upload".into(), status)),
    }
}

/// An upload the journal named, aborted where it is still open.
async fn abort_upload(client: &mut Client, at: &Location, upload: &str) -> Result<(), Error> {
    if upload.is_empty() {
        return Ok(());
    }
    let op = Op::Abort {
        bucket: at.bucket.clone(),
        key: at.key.clone(),
        upload: upload.to_owned(),
        id: mantle_client::identity()?,
    };
    let reply = client.call(&op, NO_DEADLINE).await?;
    match reply.status {
        Status::Done | Status::NoSuchUpload => Ok(()),
        status => Err(Error::Answered("AbortMultipartUpload".into(), status)),
    }
}

/// The journal's side of an upload: its record kept once the upload is opened, and each run's
/// commit told on standard error.
struct Journaled {
    journal: Journal,
    record: Record,
    at: String,
    resumed: bool,
}

impl Watch for Journaled {
    async fn opened(&mut self, upload: &str) -> std::io::Result<()> {
        upload.clone_into(&mut self.record.upload);
        self.journal
            .keep(&self.record)
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    async fn progressed(&mut self, progress: &Progress) -> std::io::Result<()> {
        let mut err = std::io::stderr().lock();
        if self.resumed {
            self.resumed = false;
            writeln!(
                err,
                "{}: resuming upload {} at {} bytes, {} runs",
                self.at, self.record.upload, progress.offset, progress.runs
            )?;
        } else if progress.runs > 0 {
            writeln!(
                err,
                "{}: run {} committed, {} bytes durable",
                self.at, progress.runs, progress.offset
            )?;
        }
        Ok(())
    }
}

/// An object into `sink`: one get where it is one put's bytes or fewer, else in ranges of that,
/// each byte held to the object's full-object checksum where it keeps one.
async fn get_object<K: Sink>(
    client: &mut Client,
    at: &Location,
    sink: &mut K,
) -> Result<(), Error> {
    let head = Op::Head {
        bucket: at.bucket.clone(),
        key: at.key.clone(),
        headers: Headers::default(),
    };
    let facts = done("HeadObject", client.call(&head, NO_DEADLINE).await?)?;
    if facts.size <= mantle_s3::body::MAX_UPLOAD {
        let get = Op::Get {
            bucket: at.bucket.clone(),
            key: at.key.clone(),
            headers: Headers::default(),
        };
        done("GetObject", client.get(&get, sink, NO_DEADLINE).await?)?;
        return Ok(());
    }
    // Ranges of the version headed (a strong If-Match), held together to its checksum.
    let full = facts
        .checksum
        .as_ref()
        .filter(|c| c.parts == 0)
        .and_then(|c| Algorithm::from_code(c.algorithm).map(|a| (a, c.value.clone())));
    let mut hashed = Hashed {
        sink,
        hasher: full
            .as_ref()
            .map(|(algorithm, _)| Hasher::new(*algorithm))
            .transpose()
            .map_err(|e| Error::Runtime(format!("{e:?}")))?,
    };
    let mut first = 0u64;
    while first < facts.size {
        let last = first
            .saturating_add(mantle_s3::body::MAX_UPLOAD)
            .min(facts.size)
            .saturating_sub(1);
        let get = Op::Get {
            bucket: at.bucket.clone(),
            key: at.key.clone(),
            headers: Headers {
                if_match: Some(format!("\"{}\"", facts.etag)),
                range: Some(format!("bytes={first}-{last}")),
                ..Headers::default()
            },
        };
        done(
            "GetObject",
            client.get(&get, &mut hashed, NO_DEADLINE).await?,
        )?;
        first = last.saturating_add(1);
    }
    if let (Some(hasher), Some((_, kept))) = (hashed.hasher, full) {
        let sum = hasher
            .finish()
            .map_err(|e| Error::Runtime(format!("{e:?}")))?;
        if sum.bytes != kept {
            return Err(Error::Client(ClientError::Corrupt("the get")));
        }
    }
    Ok(())
}

/// A sink that hashes what it passes on.
struct Hashed<'a, K> {
    sink: &'a mut K,
    hasher: Option<Hasher>,
}

impl<K: Sink> Sink for Hashed<'_, K> {
    async fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.sink.write(bytes).await?;
        if let Some(hasher) = self.hasher.as_mut() {
            hasher
                .update(bytes)
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidData))?;
        }
        Ok(())
    }
}

/// An object into the file at `path`: written under a temporary name beside it, flushed, renamed
/// over its place and its directory flushed, so the file is the whole object or untouched.
async fn get_to_file(client: &mut Client, at: &Location, path: &Path) -> Result<(), Error> {
    let mut name = path.as_os_str().to_owned();
    name.push(".part");
    let temporary = PathBuf::from(name);
    let file = File::create(&temporary).map_err(|e| Error::File(temporary.clone(), e))?;
    let mut sink = FileSink::new(file);
    let got = get_object(client, at, &mut sink).await;
    let kept = match got {
        Ok(()) => sink.finish().await,
        Err(e) => Err(e),
    };
    if let Err(e) = kept {
        let _ = std::fs::remove_file(&temporary);
        return Err(e);
    }
    std::fs::rename(&temporary, path).map_err(|e| Error::File(path.to_path_buf(), e))?;
    let dir = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    };
    hyper_block::file::sync_dir(&dir).map_err(|e| Error::Runtime(e.to_string()))
}

/// What a file job hands back: the file, its buffer, and what the job did.
type Job = (File, Vec<u8>, std::io::Result<usize>);

/// `left` bytes of a file from an offset, read a piece at a time on the blocking pool's worker. A
/// read the client dropped leaves its job out, whose bytes the next read returns.
struct FileSource {
    path: PathBuf,
    /// The file and its piece, while no read is out.
    idle: Option<(File, Vec<u8>)>,
    pending: Option<OneshotReceiver<Job>>,
    /// The piece's bytes read and not yet given.
    start: usize,
    end: usize,
    left: u64,
    /// The bytes it reads at most, from its first offset: what a rewind counts from.
    size: u64,
}

impl FileSource {
    /// The file at `path` from `offset`, `length` bytes of it or all past it.
    fn open(path: &Path, offset: u64, length: Option<u64>) -> Result<Self, Error> {
        let fail = |e| Error::File(path.to_path_buf(), e);
        let mut file = File::open(path).map_err(fail)?;
        let size = file.metadata().map_err(fail)?.len();
        file.seek(SeekFrom::Start(offset)).map_err(fail)?;
        let mut piece = Vec::new();
        piece
            .try_reserve_exact(mantle_client::PIECE)
            .map_err(|_| Error::Runtime("a file's piece".into()))?;
        Ok(Self {
            path: path.to_path_buf(),
            idle: Some((file, piece)),
            pending: None,
            start: 0,
            end: 0,
            left: length.unwrap_or_else(|| size.saturating_sub(offset)),
            size,
        })
    }
}

impl Rewind for FileSource {
    /// Waits for a read out, which a dropped read left, then seeks: what it read is the past.
    async fn rewind(&mut self, offset: u64) -> std::io::Result<()> {
        if let Some(pending) = self.pending.as_mut() {
            let finished = pending.await;
            self.pending = None;
            let (file, piece, _) = finished.map_err(|e| std::io::Error::other(format!("{e:?}")))?;
            self.idle = Some((file, piece));
        }
        let Some((file, _)) = self.idle.as_mut() else {
            return Err(std::io::Error::other("a file read lost its file"));
        };
        file.seek(SeekFrom::Start(offset))?;
        self.start = 0;
        self.end = 0;
        self.left = self.size.saturating_sub(offset);
        Ok(())
    }
}

impl Source for FileSource {
    async fn read(&mut self, into: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if self.start < self.end
                && let Some((_, piece)) = &self.idle
            {
                let held = piece.get(self.start..self.end).unwrap_or(&[]);
                let n = held.len().min(into.len());
                if let (Some(into), Some(from)) = (into.get_mut(..n), held.get(..n)) {
                    into.copy_from_slice(from);
                }
                self.start = self.start.saturating_add(n);
                return Ok(n);
            }
            if self.pending.is_none() {
                let Some((mut file, mut piece)) = self.idle.take() else {
                    return Err(std::io::Error::other("a file read lost its file"));
                };
                let want = usize::try_from(self.left)
                    .unwrap_or(usize::MAX)
                    .min(piece.capacity());
                if want == 0 {
                    self.idle = Some((file, piece));
                    return Ok(0);
                }
                piece.resize(want, 0);
                let job = blocking::run(FILES, move || {
                    let read = file.read(&mut piece);
                    (file, piece, read)
                })
                .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
                self.pending = Some(job);
            }
            let Some(pending) = self.pending.as_mut() else {
                return Err(std::io::Error::other("a file read lost its job"));
            };
            let finished = pending.await;
            self.pending = None;
            let (file, mut piece, read) =
                finished.map_err(|e| std::io::Error::other(format!("{e:?}")))?;
            let n = read?;
            piece.truncate(n);
            self.left = self
                .left
                .saturating_sub(u64::try_from(n).unwrap_or(u64::MAX));
            self.idle = Some((file, piece));
            self.start = 0;
            self.end = n;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("{} ended before its stated bytes", self.path.display()),
                ));
            }
        }
    }
}

/// A file written a piece at a time on the blocking pool's worker. A write the client dropped
/// leaves its job out, which the write asked again with the same bytes awaits rather than writing
/// them twice.
struct FileSink {
    idle: Option<(File, Vec<u8>)>,
    pending: Option<OneshotReceiver<Job>>,
}

impl FileSink {
    fn new(file: File) -> Self {
        Self {
            idle: Some((file, Vec::new())),
            pending: None,
        }
    }

    /// Waits for a write out, then flushes the file to its device (`sync_data`: the platform's
    /// full flush, `F_FULLFSYNC` on macOS).
    async fn finish(&mut self) -> Result<(), Error> {
        self.settle()
            .await
            .map_err(|e| Error::File(PathBuf::from("-"), e))?;
        let Some((file, piece)) = self.idle.take() else {
            return Err(Error::Runtime("a file write lost its file".into()));
        };
        let job = blocking::run(FILES, move || {
            let synced = file.sync_data().map(|()| 0);
            (file, piece, synced)
        })?;
        let (file, piece, synced) = job.await.map_err(|e| Error::Runtime(format!("{e:?}")))?;
        self.idle = Some((file, piece));
        synced
            .map(|_| ())
            .map_err(|e| Error::File(PathBuf::from("-"), e))
    }

    /// The write out, if any, taken back.
    async fn settle(&mut self) -> std::io::Result<()> {
        if let Some(pending) = self.pending.as_mut() {
            let finished = pending.await;
            self.pending = None;
            let (file, piece, wrote) =
                finished.map_err(|e| std::io::Error::other(format!("{e:?}")))?;
            self.idle = Some((file, piece));
            wrote?;
        }
        Ok(())
    }
}

impl Sink for FileSink {
    async fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        if self.pending.is_none() {
            let Some((mut file, mut piece)) = self.idle.take() else {
                return Err(std::io::Error::other("a file write lost its file"));
            };
            piece.clear();
            piece
                .try_reserve(bytes.len())
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
            piece.extend_from_slice(bytes);
            let job = blocking::run(FILES, move || {
                let wrote = file.write_all(&piece).map(|()| piece.len());
                (file, piece, wrote)
            })
            .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
            self.pending = Some(job);
        }
        self.settle().await
    }
}

/// Standard input, read on hyper-rt's reader thread, a piece a buffer.
struct Stdin(hyper_rt::stdio::Stdin);

impl Stdin {
    fn take() -> Result<Self, Error> {
        Ok(Self(hyper_rt::stdio::stdin(2, mantle_client::PIECE)?))
    }
}

impl Source for Stdin {
    async fn read(&mut self, into: &mut [u8]) -> std::io::Result<usize> {
        self.0
            .read(into)
            .await
            .map_err(|e| std::io::Error::other(format!("{e:?}")))
    }
}

/// Standard output, written on hyper-rt's writer thread, a piece a buffer: a write the client
/// dropped took no buffer and wrote nothing.
struct Stdout(hyper_rt::stdio::Stdout);

impl Stdout {
    fn take() -> Result<Self, Error> {
        Ok(Self(hyper_rt::stdio::stdout(2, mantle_client::PIECE)?))
    }

    async fn flush(&mut self) -> Result<(), Error> {
        self.0.flush().await.map_err(Error::from)
    }
}

impl Sink for Stdout {
    async fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.0
            .write_all(bytes)
            .await
            .map_err(|e| std::io::Error::other(format!("{e:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_place_is_the_cells_a_file_or_standard_io() {
        assert_eq!(
            place("s3://b/a/k"),
            Place::Cell(Location {
                bucket: "b".into(),
                key: "a/k".into()
            })
        );
        assert_eq!(
            location("s3://b"),
            Some(Location {
                bucket: "b".into(),
                key: String::new()
            })
        );
        assert_eq!(location("s3://"), None);
        assert_eq!(place("-"), Place::Standard);
        assert_eq!(place("./s3:/x"), Place::File(PathBuf::from("./s3:/x")));
    }
}
