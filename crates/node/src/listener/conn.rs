//! A connection's requests, one at a time and in order (RFC 9112 §9.3; node.md §4.2): a head read
//! and parsed, what it asks checked and handed to the host, its body read only as the host's
//! driver wants it, the reply written, and the next request read where the body ended.
//!
//! A request refused before its body is read is answered with `Connection: close`, and the
//! connection's sending half ended: what the client sends after cannot be told from a next
//! request (RFC 9112 §6.3, §9.6). A put's body is read only after the host says its driver wants
//! it, which is after every check that could refuse it, so `100 Continue` goes only then and a
//! refusal goes in its place (RFC 9110 §10.1.1; node.md §4.5).

use bytes::Bytes;
use hyper_rt::sync::{Sender, channel};
use mantle_http::write::{self, Length};
use mantle_http::{self as http, Framing, Head, Refusal, Version};
use mantle_s3::checksum::{Algorithm, Checksum, Hasher};
use mantle_s3::route::Query;

use super::Endpoint;
use super::io::{Io, IoError};
use super::request::{self, Parts, Plan, Refused, Streamed, Upload};
use super::respond::{self, Echo, Ids, Response};
use crate::door::{self, Answer, Piece, Submitter};
use mantle_protocol::object::{Op, Status};

/// The octets a connection reads at once, and a body's piece: the native door's, the transport's
/// receive chunk (`crate::node::PIECE`), so a put's driver takes the same pieces by either door.
pub const PIECE: usize = hyper_transport::RECEIVE_CHUNK;

/// A body's pieces in flight to the host: one the driver takes while the next is read.
const BODY_PIECES: usize = 2;

/// A reply's answers in flight from the host: its head, or a get's piece while the last is sent.
const REPLY_PIECES: usize = 2;

/// How a connection ended.
enum Ended {
    /// The client closed it between requests.
    Closed,
    /// A response said `Connection: close`: the sending half ends, and what the client still
    /// sends is read and dropped until it closes.
    Close,
    /// It failed: the socket, TLS, or a reply the host could not finish.
    Broken,
}

/// The octets read and not yet taken: `buf[start..end]`. Initialized once, so a read needs no
/// zeroing of its own.
struct Input {
    buf: Vec<u8>,
    start: usize,
    end: usize,
}

impl Input {
    fn unread(&self) -> &[u8] {
        self.buf.get(self.start..self.end).unwrap_or_default()
    }

    /// Takes `n` octets of what is unread.
    fn take(&mut self, n: usize) {
        self.start = self.start.saturating_add(n).min(self.end);
        if self.start == self.end {
            self.start = 0;
            self.end = 0;
        }
    }

    /// Reads more: the octets read, none at the client's end. What is unread moves to the
    /// buffer's start when the read would find no room.
    async fn fill(&mut self, io: &mut Io) -> Result<usize, IoError> {
        if self.end == self.buf.len() && self.start > 0 {
            self.buf.copy_within(self.start..self.end, 0);
            self.end = self.end.saturating_sub(self.start);
            self.start = 0;
        }
        let Some(room) = self.buf.get_mut(self.end..).filter(|room| !room.is_empty()) else {
            return Err(IoError::Stalled);
        };
        let n = io.read(room).await?;
        self.end = self.end.saturating_add(n).min(self.buf.len());
        Ok(n)
    }
}

/// Why a body stopped before its end.
enum Stopped {
    /// The listener refused it: what to answer, and the status the host fails the job with.
    Refused(Refused),
    /// The host ended the job while its body came: its reply says why.
    Host,
    Broken,
}

struct Connection {
    io: Io,
    input: Input,
    /// The response being written: its head, and a document.
    output: Vec<u8>,
    endpoint: &'static Endpoint,
    door: Submitter,
}

/// Serves `io`'s requests until the connection ends, then gives its descriptor back.
pub(super) async fn serve(io: Io, endpoint: &'static Endpoint, door: Submitter) {
    let mut connection = Connection {
        io,
        input: Input {
            buf: vec![0; PIECE],
            start: 0,
            end: 0,
        },
        output: Vec::new(),
        endpoint,
        door,
    };
    let ended = connection.requests().await;
    connection.end(ended).await;
    drop(connection);
    endpoint.closed.notify_one();
}

/// Unix seconds now, as SigV4 checks a request's time.
fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_secs()).ok())
        .unwrap_or_default()
}

/// A request's id: 16 hexadecimal digits from the OS's random source, as S3's are.
fn request_id() -> String {
    let mut drawn = [0u8; 8];
    // A draw that fails names the request by zeros: the id is for the client's report only.
    let _ = getrandom::fill(&mut drawn);
    format!("{:016X}", u64::from_le_bytes(drawn))
}

/// Whether the framing carries a body.
fn has_body(framing: Framing) -> bool {
    !matches!(framing, Framing::None | Framing::Length(0))
}

/// The answer to a request the codec refused: its status, S3's code where S3 names one.
fn refused_head(refusal: Refusal) -> Refused {
    let code = match refusal {
        Refusal::HeadTooLarge => "RequestHeaderSectionTooLarge",
        Refusal::TargetTooLong | Refusal::Grammar(_) | Refusal::Host => "InvalidRequest",
        Refusal::Version => "HttpVersionNotSupported",
        Refusal::Coding => "NotImplemented",
        Refusal::Expectation => "InvalidRequest",
        Refusal::Framing(_) | Refusal::Chunked(_) => "InvalidRequest",
    };
    Refused::new(code, refusal.status(), refusal.to_string())
}

impl Connection {
    async fn requests(&mut self) -> Ended {
        let mut octets = Vec::with_capacity(http::MAX_HEAD);
        loop {
            let head = loop {
                match http::parse(self.input.unread()) {
                    Ok(Some(head)) => break head,
                    Ok(None) => match self.input.fill(&mut self.io).await {
                        Ok(0) if self.input.unread().is_empty() => return Ended::Closed,
                        Ok(0) | Err(_) => return Ended::Broken,
                        Ok(_) => {}
                    },
                    Err(refusal) => {
                        let ids = self.ids();
                        let response = respond::error(&refused_head(refusal), "/", &ids, false);
                        return match self.respond(&response, Version::Http11, true).await {
                            Ok(()) => Ended::Close,
                            Err(()) => Ended::Broken,
                        };
                    }
                }
            };
            // The head is kept apart, so the body after it can be read on.
            octets.clear();
            octets.extend_from_slice(self.input.unread().get(..head.len()).unwrap_or_default());
            self.input.take(head.len());
            match self.request(&head, &octets).await {
                Ok(true) => {}
                Ok(false) => return Ended::Close,
                Err(()) => return Ended::Broken,
            }
        }
    }

    fn ids(&self) -> Ids {
        Ids {
            request: request_id(),
            host: self.endpoint.host.clone(),
        }
    }

    /// Serves one request: whether the connection persists past it.
    async fn request(&mut self, head: &Head, octets: &[u8]) -> Result<bool, ()> {
        let version = head.version();
        let framing = head.framing();
        let ids = self.ids();
        let is_head = head.method(octets) == b"HEAD";
        let parts = match Parts::of(head, octets) {
            Ok(parts) => parts,
            Err(refused) => {
                return self
                    .refuse(&refused, "/", &ids, (version, is_head), has_body(framing))
                    .await;
            }
        };
        let asked = match request::ask(&parts, self.endpoint, now(), framing) {
            Ok(asked) => asked,
            Err(refused) => {
                let resource = parts.path.to_owned();
                return self
                    .refuse(
                        &refused,
                        &resource,
                        &ids,
                        (version, is_head),
                        has_body(framing),
                    )
                    .await;
            }
        };
        let request::Asked {
            route,
            principal,
            plan,
        } = asked;
        let query = Query::parse(parts.query);
        let echo = Echo {
            route: &route,
            query: &query,
            principal: &principal,
            region: &self.endpoint.region,
            head: is_head,
            checksums: parts
                .header("x-amz-checksum-mode")
                .is_some_and(|m| m.eq_ignore_ascii_case("ENABLED")),
        };
        let resource = respond::resource(&route);
        let persists = head.persists();
        let continues = head.continues();
        let (op, body) = match plan {
            Plan::Host { op, body } => (op, body),
            Plan::Document {
                limit,
                upload,
                make,
            } => {
                if continues && self.io.write(write::CONTINUE).await.is_err() {
                    return Err(());
                }
                let document = match self.document(framing, upload, limit).await {
                    Ok(document) => document,
                    Err(Stopped::Refused(refused)) => {
                        return self
                            .refuse(&refused, &resource, &ids, (version, is_head), true)
                            .await;
                    }
                    Err(Stopped::Host | Stopped::Broken) => return Err(()),
                };
                match request::made(make, &document, &self.endpoint.region) {
                    Ok(op) => (op, None),
                    Err(refused) => {
                        return self
                            .refuse(&refused, &resource, &ids, (version, is_head), false)
                            .await
                            .map(|persisted| persisted && persists);
                    }
                }
            }
        };
        let exchanged = self
            .exchange(
                (op, principal.clone()),
                body,
                (framing, continues),
                (&echo, &ids, &resource),
                (version, is_head),
            )
            .await?;
        Ok(persists && exchanged)
    }

    /// Answers `refused`; whether the connection persists: not past a body left unread.
    async fn refuse(
        &mut self,
        refused: &Refused,
        resource: &str,
        ids: &Ids,
        (version, head): (Version, bool),
        unread: bool,
    ) -> Result<bool, ()> {
        let response = respond::error(refused, resource, ids, head);
        self.respond(&response, version, unread).await?;
        Ok(!unread)
    }

    /// Writes `response`'s head and document, `Connection: close` when `close`.
    async fn respond(
        &mut self,
        response: &Response,
        version: Version,
        close: bool,
    ) -> Result<(), ()> {
        self.output.clear();
        let mut fields: Vec<(&str, &[u8])> = response
            .fields
            .iter()
            .map(|(name, value)| (*name, value.as_slice()))
            .collect();
        if close {
            fields.push(("Connection", b"close"));
        }
        write::head(
            &mut self.output,
            version,
            response.status,
            respond::reason(response.status),
            &fields,
            response.length,
        )
        .map_err(|_| ())?;
        self.output.extend_from_slice(&response.document);
        self.io.write(&self.output).await.map_err(|_| ())
    }

    /// Hands `op` to the host and answers what it replies: the body streamed when the host's
    /// driver wants it, a get's bytes as the host gives them. Whether the connection may persist.
    async fn exchange(
        &mut self,
        (op, principal): (Op, String),
        body: Option<Streamed>,
        (framing, continues): (Framing, bool),
        (echo, ids, resource): (&Echo<'_>, &Ids, &str),
        (version, head): (Version, bool),
    ) -> Result<bool, ()> {
        let mut unread = has_body(framing) && body.is_some();
        let slow = || respond::refusal(Status::SlowDown);
        let (Ok((pieces, taken)), Ok((answers, mut replies))) = (
            channel::<Piece>(BODY_PIECES),
            channel::<Answer>(REPLY_PIECES),
        ) else {
            return self
                .refuse(&slow(), resource, ids, (version, head), unread)
                .await;
        };
        let request = door::Request {
            op,
            principal,
            trailing: body.as_ref().and_then(|(_, _, trailing)| *trailing),
            body: taken,
            reply: answers,
        };
        // A door at its bound refuses before any of the body is read (node.md §4.5).
        if self.door.submit(request).is_err() {
            return self
                .refuse(&slow(), resource, ids, (version, head), unread)
                .await;
        }
        let mut body = body;
        let mut refused: Option<Refused> = None;
        loop {
            let answer = replies.recv().await;
            // The host may hold answers for the room this one leaves.
            self.door.wake();
            match answer {
                Ok(Answer::Ready) => {
                    let Some((upload, _, _)) = body.take() else {
                        continue;
                    };
                    if continues && self.io.write(write::CONTINUE).await.is_err() {
                        return Err(());
                    }
                    match self.stream(framing, upload, &pieces).await {
                        Ok(()) => unread = false,
                        Err(Stopped::Refused(why)) => refused = Some(why),
                        // The host ended the job: its reply follows, and the body's rest is unread.
                        Err(Stopped::Host) => {}
                        Err(Stopped::Broken) => return Err(()),
                    }
                }
                Ok(Answer::Head(reply)) => {
                    if let Some(why) = refused {
                        // The listener refused the body as it came: its answer, not the host's.
                        return self
                            .refuse(&why, resource, ids, (version, head), true)
                            .await;
                    }
                    let response = match respond::reply(echo, &reply, ids) {
                        Ok(response) => response,
                        Err(why) => respond::error(&why, resource, ids, head),
                    };
                    self.respond(&response, version, unread).await?;
                    if response.object && !head {
                        self.object(&mut replies, response.length).await?;
                    }
                    return Ok(!unread);
                }
                // A reply's end, or bytes, with no head before them: the host's word is not
                // whole, and nothing of it can be answered as done.
                Ok(Answer::Bytes(_) | Answer::End | Answer::Failed) | Err(_) => {
                    let failed = respond::refusal(Status::Failed);
                    return self
                        .refuse(&failed, resource, ids, (version, head), unread)
                        .await;
                }
            }
        }
    }

    /// A get's bytes, as the host gives them, until their end: `length` of them, as the head
    /// said. A reply that fails or ends short ends the connection with the response unfinished,
    /// so no client takes a short body for the object's (node.md §4.4).
    async fn object(
        &mut self,
        replies: &mut hyper_rt::sync::ChannelReceiver<Answer>,
        length: Length,
    ) -> Result<(), ()> {
        let Length::Known(length) = length else {
            return Err(());
        };
        let mut sent = 0u64;
        loop {
            let answer = replies.recv().await;
            self.door.wake();
            match answer {
                Ok(Answer::Bytes(bytes)) => {
                    sent = u64::try_from(bytes.len())
                        .ok()
                        .and_then(|n| sent.checked_add(n))
                        .filter(|sent| *sent <= length)
                        .ok_or(())?;
                    self.io.write(&bytes).await.map_err(|_| ())?;
                }
                Ok(Answer::End) if sent == length => return Ok(()),
                _ => return Err(()),
            }
        }
    }

    /// The next of the body's octets the framing gives, read as they come: the framing's
    /// octets taken, and its data's span of what is unread. Each pass decodes or reads at least an
    /// octet, so it ends within the body's framing or at the client's end.
    async fn step(&mut self, framing: &mut http::Body) -> Result<http::Step, Stopped> {
        loop {
            if !self.input.unread().is_empty() {
                let step = framing
                    .decode(self.input.unread())
                    .map_err(|refusal| Stopped::Refused(refused_head(refusal)))?;
                if step.consumed > 0 || step.end {
                    return Ok(step);
                }
            }
            match self.input.fill(&mut self.io).await {
                Ok(0) => {
                    return Err(Stopped::Refused(respond::refusal(Status::IncompleteBody)));
                }
                Ok(_) => {}
                Err(_) => return Err(Stopped::Broken),
            }
        }
    }

    /// A body's octets as `upload` reads them: given to `out` as they are decoded.
    fn decode(
        upload: &mut Upload,
        data: &[u8],
        sha256: &mut Option<Hasher>,
        out: &mut Vec<u8>,
    ) -> Result<(), Refused> {
        match upload {
            Upload::Plain(_) => {
                if let Some(hasher) = sha256 {
                    hasher
                        .update(data)
                        .map_err(|_| Refused::new("InternalError", 500, "a body's digest"))?;
                }
                out.extend_from_slice(data);
                Ok(())
            }
            Upload::Chunked(decoder) => decoder
                .feed(data, out)
                .map_err(|e| Refused::new(e.code().0, e.code().1, e.to_string())),
        }
    }

    /// The body's end as `upload` reads it: the signed digest compared, or aws-chunked's trailer
    /// and its checksum.
    fn ended(upload: Upload, sha256: Option<Hasher>) -> Result<Option<Checksum>, Refused> {
        match upload {
            Upload::Plain(Some(expected)) => {
                let digest = sha256
                    .map(Hasher::finish)
                    .transpose()
                    .ok()
                    .flatten()
                    .ok_or_else(|| Refused::new("InternalError", 500, "a body's digest"))?;
                if digest.bytes.as_slice() != expected.as_slice() {
                    return Err(Refused::new(
                        "XAmzContentSHA256Mismatch",
                        400,
                        "The provided 'x-amz-content-sha256' header does not match what was computed.",
                    ));
                }
                Ok(None)
            }
            Upload::Plain(None) => Ok(None),
            Upload::Chunked(decoder) => {
                let trailer = decoder
                    .finish()
                    .map_err(|e| Refused::new(e.code().0, e.code().1, e.to_string()))?;
                let Some(trailer) = trailer else {
                    return Ok(None);
                };
                let checksum = Algorithm::from_header(&trailer.name)
                    .and_then(|algorithm| Checksum::from_base64(algorithm, &trailer.value))
                    .ok_or_else(|| {
                        Refused::new(
                            "InvalidRequest",
                            400,
                            format!("Value for {} trailing header is invalid.", trailer.name),
                        )
                    })?;
                Ok(Some(checksum))
            }
        }
    }

    fn hasher(upload: &Upload) -> Result<Option<Hasher>, Stopped> {
        match upload {
            Upload::Plain(Some(_)) => Hasher::new(Algorithm::Sha256).map(Some).map_err(|_| {
                Stopped::Refused(Refused::new("InternalError", 500, "a body's digest"))
            }),
            _ => Ok(None),
        }
    }

    /// Streams a put's body to the host, a piece at a time as it takes them, so TCP's window
    /// carries the backpressure; its end only once its digests and signatures held.
    async fn stream(
        &mut self,
        framing: Framing,
        mut upload: Upload,
        pieces: &Sender<Piece>,
    ) -> Result<(), Stopped> {
        let mut sha256 = Self::hasher(&upload)?;
        let mut body = http::Body::new(framing);
        let refuse = |why: Refused, status: Status| async move {
            // The job fails with the nearest status the host keeps; the listener's own answer is
            // the one written.
            let _ = pieces.send(Piece::Refused(status)).await;
            Err(Stopped::Refused(why))
        };
        while !body.done() {
            let step = match self.step(&mut body).await {
                Ok(step) => step,
                Err(Stopped::Refused(why)) => {
                    self.door.wake();
                    return refuse(why, Status::IncompleteBody).await;
                }
                Err(other) => return Err(other),
            };
            let mut out = Vec::new();
            let data = self
                .input
                .unread()
                .get(step.data.clone())
                .unwrap_or_default();
            if let Err(why) = Self::decode(&mut upload, data, &mut sha256, &mut out) {
                self.door.wake();
                let status = if why.status == 403 {
                    Status::AccessDenied
                } else {
                    Status::BadDigest
                };
                return refuse(why, status).await;
            }
            self.input.take(step.consumed);
            if out.is_empty() {
                continue;
            }
            if pieces.send(Piece::Bytes(Bytes::from(out))).await.is_err() {
                return Err(Stopped::Host);
            }
            self.door.wake();
        }
        let end = Self::ended(upload, sha256);
        let piece = match &end {
            Ok(trailer) => Piece::End(trailer.clone()),
            Err(why) if why.status == 403 => Piece::Refused(Status::AccessDenied),
            Err(_) => Piece::Refused(Status::BadDigest),
        };
        let sent = pieces.send(piece).await;
        self.door.wake();
        match (end, sent) {
            (Err(why), _) => Err(Stopped::Refused(why)),
            (Ok(_), Err(_)) => Err(Stopped::Host),
            (Ok(_), Ok(())) => Ok(()),
        }
    }

    /// A document's body read whole, at most `limit` octets of it, its digests checked as a put's.
    async fn document(
        &mut self,
        framing: Framing,
        mut upload: Upload,
        limit: usize,
    ) -> Result<Vec<u8>, Stopped> {
        let mut sha256 = Self::hasher(&upload)?;
        let mut body = http::Body::new(framing);
        let mut document = Vec::new();
        while !body.done() {
            let step = self.step(&mut body).await?;
            let data = self
                .input
                .unread()
                .get(step.data.clone())
                .unwrap_or_default();
            Self::decode(&mut upload, data, &mut sha256, &mut document)
                .map_err(Stopped::Refused)?;
            self.input.take(step.consumed);
            if document.len() > limit {
                return Err(Stopped::Refused(Refused::new(
                    "MaxMessageLengthExceeded",
                    400,
                    "Your request was too large.",
                )));
            }
        }
        Self::ended(upload, sha256).map_err(Stopped::Refused)?;
        Ok(document)
    }

    /// Ends the connection as `ended` says. After a response that said `Connection: close`, its
    /// sending half ends and what the client still sends is read and dropped until it closes, at
    /// most the largest body a request may carry, so the response is not lost to a reset
    /// (RFC 9112 §9.6).
    async fn end(&mut self, ended: Ended) {
        if !matches!(ended, Ended::Close) || self.io.close().await.is_err() {
            return;
        }
        let mut drained = 0u64;
        while drained <= request::MAX_BODY {
            match self.io.read(&mut self.input.buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => drained = drained.saturating_add(u64::try_from(n).unwrap_or(u64::MAX)),
            }
        }
    }
}
