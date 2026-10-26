//! A signed S3 client of the listener's own, for its tests: each request signed with SigV4 by the
//! operator's profile key (`mantle_s3::sigv4`, the functions S3's own clients' signatures are
//! checked against), written over a keep-alive TCP connection opened again after a response that
//! closed it, and its response read whole.

use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::path::Path;

use mantle_s3::sigv4;

/// An access key as the operator's profile holds it (`OUT/s3/credentials`).
#[derive(Clone)]
pub(crate) struct Key {
    pub(crate) id: String,
    pub(crate) secret: String,
}

impl Key {
    /// The key in `out`'s profile.
    pub(crate) fn of(out: &Path) -> Self {
        let credentials = std::fs::read_to_string(out.join("s3").join("credentials")).unwrap();
        let value = |name: &str| {
            credentials
                .lines()
                .find_map(|line| line.strip_prefix(name))
                .unwrap()
                .to_owned()
        };
        Self {
            id: value("aws_access_key_id = "),
            secret: value("aws_secret_access_key = "),
        }
    }
}

/// A response, read whole.
#[derive(Debug)]
pub(crate) struct Response {
    pub(crate) status: u16,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
}

impl Response {
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub(crate) fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    /// The text of the first `element` the body's XML holds.
    pub(crate) fn element(&self, element: &str) -> Option<String> {
        let text = self.text();
        let open = format!("<{element}>");
        let start = text.find(&open)? + open.len();
        let end = start + text.get(start..)?.find(&format!("</{element}>"))?;
        Some(text.get(start..end)?.to_owned())
    }

    /// Whether the server said it closes the connection after this response.
    pub(crate) fn closes(&self) -> bool {
        self.header("connection")
            .is_some_and(|c| c.eq_ignore_ascii_case("close"))
    }
}

/// `YYYYMMDDTHHMMSSZ` and `YYYYMMDD` now, as SigV4 signs a request's time.
pub(crate) fn timestamp() -> (String, String) {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    // `2009-10-12T17:50:30.000Z` without its separators and fraction.
    let iso = mantle_s3::time::iso8601(i64::try_from(millis).unwrap()).unwrap();
    let compact: String = iso
        .get(..19)
        .unwrap()
        .chars()
        .filter(|c| *c != '-' && *c != ':')
        .collect();
    let stamp = format!("{compact}Z");
    let date = stamp.get(..8).unwrap().to_owned();
    (stamp, date)
}

/// A client of the listener at one address.
pub(crate) struct Client {
    pub(crate) at: SocketAddr,
    pub(crate) key: Key,
    pub(crate) region: String,
    stream: Option<TcpStream>,
    /// Octets read past the last response.
    read: Vec<u8>,
}

impl Client {
    pub(crate) fn new(at: SocketAddr, key: Key, region: &str) -> Self {
        Self {
            at,
            key,
            region: region.to_owned(),
            stream: None,
            read: Vec::new(),
        }
    }

    /// The connection, opened if none is.
    pub(crate) fn stream(&mut self) -> &mut TcpStream {
        if self.stream.is_none() {
            self.read.clear();
            self.stream = Some(TcpStream::connect(self.at).unwrap());
        }
        self.stream.as_mut().unwrap()
    }

    /// Ends the connection.
    pub(crate) fn drop_connection(&mut self) {
        if let Some(stream) = self.stream.take() {
            let _ = stream.shutdown(Shutdown::Both);
        }
        self.read.clear();
    }

    /// A request's head, signed: `target` is its path and query as sent, `headers` beside the
    /// ones signing adds, the body's SHA-256 as `payload` names it.
    pub(crate) fn head(
        &self,
        method: &str,
        target: &str,
        headers: &[(&str, &str)],
        payload: &str,
    ) -> String {
        let (stamp, date) = timestamp();
        let host = self.at.to_string();
        let mut all: Vec<(String, String)> = vec![
            ("host".into(), host),
            ("x-amz-date".into(), stamp.clone()),
            ("x-amz-content-sha256".into(), payload.to_owned()),
        ];
        all.extend(
            headers
                .iter()
                .map(|(n, v)| (n.to_ascii_lowercase(), (*v).to_owned())),
        );
        let unsigned = [
            "content-length",
            "expect",
            "connection",
            "transfer-encoding",
        ];
        let mut signed: Vec<&str> = all
            .iter()
            .map(|(n, _)| n.as_str())
            .filter(|n| !unsigned.contains(n))
            .collect();
        signed.sort_unstable();
        signed.dedup();
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        let pairs: Vec<(&str, &str)> = all.iter().map(|(n, v)| (n.as_str(), v.as_str())).collect();
        let request = sigv4::Request {
            method,
            path,
            query,
            headers: &pairs,
        };
        let canonical = sigv4::canonical_request(&request, &signed, payload, None).unwrap();
        let signature =
            sigv4::sign(&self.key.secret, &stamp, &date, &self.region, &canonical).unwrap();
        let mut head = format!("{method} {target} HTTP/1.1\r\n");
        for (name, value) in &all {
            head.push_str(&format!("{name}: {value}\r\n"));
        }
        head.push_str(&format!(
            "authorization: {} Credential={}/{}, SignedHeaders={}, Signature={}\r\n\r\n",
            sigv4::ALGORITHM,
            self.key.id,
            sigv4::scope(&date, &self.region, "s3"),
            signed.join(";"),
            sigv4::hex(&signature)
        ));
        head
    }

    /// Sends a request whose body's SHA-256 is signed, and reads its response.
    pub(crate) fn send(
        &mut self,
        method: &str,
        target: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Response {
        let payload = sigv4::hex(&mantle_s3::crypto::sha256(body).unwrap());
        self.send_as(method, target, headers, body, &payload)
    }

    /// Sends a request whose `x-amz-content-sha256` is `payload`: a body's hex SHA-256, or one of
    /// the streaming kinds whose body is `aws-chunked`.
    pub(crate) fn send_as(
        &mut self,
        method: &str,
        target: &str,
        headers: &[(&str, &str)],
        body: &[u8],
        payload: &str,
    ) -> Response {
        let length = body.len().to_string();
        let mut with_length: Vec<(&str, &str)> = headers.to_vec();
        if !body.is_empty() || matches!(method, "PUT" | "POST") {
            with_length.push(("content-length", &length));
        }
        let head = self.head(method, target, &with_length, payload);
        let stream = self.stream();
        stream.write_all(head.as_bytes()).unwrap();
        stream.write_all(body).unwrap();
        self.response(method == "HEAD")
    }

    /// Reads a response whole, the connection dropped after one that closes it.
    pub(crate) fn response(&mut self, head_request: bool) -> Response {
        let response = self.read_response(head_request);
        if response.closes() {
            self.drop_connection();
        }
        response
    }

    /// Reads the next response, an interim one included.
    pub(crate) fn read_response(&mut self, head_request: bool) -> Response {
        let end = loop {
            if let Some(end) = self.read.windows(4).position(|w| w == b"\r\n\r\n") {
                break end + 4;
            }
            self.fill();
        };
        let head = String::from_utf8(self.read[..end].to_vec()).unwrap();
        self.read.drain(..end);
        let mut lines = head.split("\r\n");
        let status: u16 = lines
            .next()
            .unwrap()
            .split(' ')
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        let headers: Vec<(String, String)> = lines
            .filter(|l| !l.is_empty())
            .map(|l| {
                let (n, v) = l.split_once(':').unwrap();
                (n.trim().to_owned(), v.trim().to_owned())
            })
            .collect();
        let length = headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case("content-length"))
            .map(|(_, v)| v.parse::<usize>().unwrap());
        let bodiless =
            head_request || (100..200).contains(&status) || status == 204 || status == 304;
        let length = if bodiless { 0 } else { length.unwrap_or(0) };
        while self.read.len() < length {
            self.fill();
        }
        let body = self.read.drain(..length).collect();
        Response {
            status,
            headers,
            body,
        }
    }

    /// Reads more from the connection; the server's close is a test's failure here.
    fn fill(&mut self) {
        let mut buf = vec![0u8; 64 << 10];
        let n = self.stream().read(&mut buf).unwrap();
        assert!(n > 0, "the listener closed the connection mid-response");
        self.read.extend_from_slice(&buf[..n]);
    }

    /// Whether the server has closed the connection: a read finds its end.
    pub(crate) fn closed(&mut self) -> bool {
        let mut buf = [0u8; 1024];
        loop {
            match self.stream().read(&mut buf) {
                Ok(0) => return true,
                Ok(_) => continue,
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return true,
                Err(e) => panic!("reading for the close: {e}"),
            }
        }
    }
}

/// `data` as `aws-chunked` with unsigned chunks of `chunk` octets, the last of none, and a trailing
/// checksum field `trailer` (05 §1.8–§1.9).
pub(crate) fn aws_chunked(data: &[u8], chunk: usize, trailer: (&str, &str)) -> Vec<u8> {
    let mut out = Vec::new();
    for piece in data.chunks(chunk) {
        out.extend_from_slice(format!("{:x}\r\n", piece.len()).as_bytes());
        out.extend_from_slice(piece);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(format!("0\r\n{}:{}\r\n\r\n", trailer.0, trailer.1).as_bytes());
    out
}
