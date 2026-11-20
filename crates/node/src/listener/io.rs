//! A connection's octets, plain or under TLS (node.md §4.1): plaintext read into the connection's
//! buffer and written from its own, the socket's readiness awaited through the shard's driver, so
//! a client that sends or reads slowly yields the shard rather than holding it, and each try of
//! the socket charged to the connection's turn (`hyper_rt::coop`), so one that sends or reads as
//! fast as the shard serves yields it at the shard's quantum. Under TLS the
//! records are hyper-tls's, read from and written to the non-blocking socket, the handshake run by
//! the first read; the connection's configuration is its own and lent to each call that can
//! advance the handshake.

use std::io::{self, IoSlice, Read as _, Write as _};
use std::time::Instant;

use hyper_rt::RtError;
use hyper_rt::tcp::{Shutdown, TcpStream};
use hyper_tls::{ServerConfig, ServerConnection};

/// Why a connection's octets stopped.
#[derive(Debug, thiserror::Error)]
pub enum IoError {
    #[error("the socket: {0:?}")]
    Socket(RtError),
    #[error("the socket: {0}")]
    Io(io::Error),
    #[error("TLS: {0}")]
    Tls(hyper_tls::Error),
    #[error("a write the socket took none of")]
    Stalled,
}

/// What a client took of a write once the socket had no room for the rest of it: when it first had
/// none, which the socket says by taking less than it was offered or refusing the offer whole
/// (`EAGAIN`, `WSAEWOULDBLOCK`), and the octets the socket took after, which the client made room
/// for, so its pace with no buffer of the socket's or the client's holding it back
/// (`crate::window`, `host::Drain`). A write the socket took whole says only that its room was
/// there. Under TLS the octets are the plaintext the connection took into records after, the
/// record being sent as the socket filled left out.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Drained {
    pub full: Option<Instant>,
    pub octets: u64,
}

impl Drained {
    /// The socket, or the connection's records, took `n` octets more.
    fn took(&mut self, n: usize) {
        if self.full.is_some() {
            self.octets = self
                .octets
                .saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
        }
    }
}

/// What a plain connection offers its socket of a body at a time. A Linux or macOS socket takes
/// no more of an offer than its send buffer has room for and says how much it took (`write(2)`:
/// a socket subject to flow control may write fewer bytes than requested), so the whole of the
/// rest is offered. A Windows socket takes all of an offer while it is under its quota, however far
/// past the quota that leaves it, and all of one more past it while it holds a single send; then
/// it refuses (`WSAEWOULDBLOCK`) until enough is sent (Microsoft KB214397, "Winsock can buffer more
/// than the SO_SNDBUF buffer size"); and the quota follows TCP's ideal send backlog, up to 16 MB,
/// unless the socket sets `SO_SNDBUF` (Microsoft, `SIO_IDEAL_SEND_BACKLOG_QUERY`, dynamic send
/// buffering). Offered whole blocks, it took a get's first two at once, never a part, so no piece
/// said its socket was full and no client of a plain connection on Windows was paced; and once
/// full, it would take a whole block for the little room its client made. Offered a record's
/// plaintext at a time ([`super::RECORD_PLAINTEXT`]), as the TLS arm offers its socket on every
/// platform, it takes at most two records past its client's room.
#[cfg(windows)]
fn offer(body: &[u8]) -> &[u8] {
    body.get(..super::RECORD_PLAINTEXT).unwrap_or(body)
}

/// See the Windows arm.
#[cfg(not(windows))]
fn offer(body: &[u8]) -> &[u8] {
    body
}

/// A connection's transport: plain HTTP on loopback, TLS elsewhere.
pub enum Io {
    Plain(TcpStream),
    Tls(Box<Tls>),
}

impl Io {
    /// The client's address, where the socket names it.
    pub fn peer(&self) -> Option<std::net::IpAddr> {
        let stream = match self {
            Self::Plain(stream) => stream,
            Self::Tls(tls) => &tls.stream,
        };
        stream.peer_addr().ok().map(|at| at.ip())
    }
}

/// A TLS connection: its socket, its state and the configuration its handshake reads.
pub struct Tls {
    stream: TcpStream,
    connection: ServerConnection,
    config: ServerConfig,
}

impl Tls {
    /// The connection's records are encrypted one at a time: a write takes at most one record's
    /// plaintext (`listener::RECORD_PLAINTEXT`) and is sent before the next is taken, so it holds
    /// one record unsent (`listener::TLS_RECORD`), where hyper-tls's default would encrypt 64 KiB
    /// of a response's bytes ahead of the socket.
    pub fn new(stream: TcpStream, config: ServerConfig) -> Result<Self, IoError> {
        let mut connection = ServerConnection::new(&config).map_err(IoError::Tls)?;
        connection.set_buffer_limit(Some(super::RECORD_PLAINTEXT));
        Ok(Self {
            stream,
            connection,
            config,
        })
    }

    /// Sends what the connection holds of records, waiting while the socket is full, `full` set to
    /// when it first was, where it was not already. Each write the socket is tried for is a charge
    /// of the connection's turn on its shard (`hyper_rt::coop`), as a plain stream's write is.
    async fn flush(&mut self, full: &mut Option<Instant>) -> Result<(), IoError> {
        while self.connection.wants_write() {
            hyper_rt::coop::consume_budget().await;
            match self.connection.write_tls(&mut Socket(&self.stream)) {
                Ok(0) => return Err(IoError::Stalled),
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    full.get_or_insert_with(Instant::now);
                    self.stream.writable().await.map_err(IoError::Socket)?;
                }
                Err(e) => return Err(IoError::Io(e)),
            }
        }
        Ok(())
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
            Err(e) => Err(io::Error::other(e)),
        }
    }
}

impl io::Write for Socket<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.0.try_write(buf) {
            Ok(Some(n)) => Ok(n),
            Ok(None) => Err(io::ErrorKind::WouldBlock.into()),
            Err(e) => Err(io::Error::other(e)),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Io {
    /// Reads plaintext into `buf`, waiting until some comes: the octets read, none once the
    /// client's side has ended. Under TLS, a close without `close_notify` ends it too: HTTP's own
    /// framing tells a whole message from a cut one (RFC 9112 §9.8). Each read the socket is tried
    /// for is a charge of the connection's turn on its shard (`hyper_rt::coop`), so a client whose
    /// octets are always there yields the shard at its quantum, as one that waits does at its wait.
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize, IoError> {
        match self {
            Self::Plain(stream) => stream.read(buf).await.map_err(IoError::Socket),
            Self::Tls(tls) => loop {
                match tls.connection.reader().read(buf) {
                    Ok(n) => return Ok(n),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(0),
                    Err(e) => return Err(IoError::Io(e)),
                }
                // What the handshake has to say goes before more is read: the server's flight.
                tls.flush(&mut None).await?;
                hyper_rt::coop::consume_budget().await;
                match tls.connection.read_tls(&mut Socket(&tls.stream)) {
                    Ok(_) => {}
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        tls.stream.readable().await.map_err(IoError::Socket)?;
                        continue;
                    }
                    Err(e) => return Err(IoError::Io(e)),
                }
                if let Err(e) = tls.connection.process_new_packets(&mut tls.config) {
                    // The alert that says why goes to the client before the connection ends.
                    let _ = tls.flush(&mut None).await;
                    return Err(IoError::Tls(e));
                }
            },
        }
    }

    /// Writes all of `bytes`, waiting while the socket is full.
    pub async fn write(&mut self, bytes: &[u8]) -> Result<(), IoError> {
        match self {
            Self::Plain(stream) => stream.write_all(bytes).await.map_err(IoError::Socket),
            Self::Tls(tls) => {
                let mut at = 0usize;
                while let Some(rest) = bytes.get(at..).filter(|rest| !rest.is_empty()) {
                    let took = tls.connection.writer().write(rest).map_err(IoError::Io)?;
                    // After a flush the connection takes a record's plaintext; none taken would
                    // never end.
                    if took == 0 {
                        return Err(IoError::Stalled);
                    }
                    at = at.checked_add(took).ok_or(IoError::Stalled)?;
                    tls.flush(&mut None).await?;
                }
                tls.flush(&mut None).await
            }
        }
    }

    /// Writes all of `first` and then all of `second`, a response's head and its body, in as few
    /// writes as the socket takes: one vectored write each time it is writable, so a head and the
    /// body after it go out together, neither copied into the other. Under TLS both go into the
    /// connection's records before they are flushed. What its client took of them once the
    /// socket was full ([`Drained`]). Each write the socket is tried for is a charge of the
    /// connection's turn on its shard (`hyper_rt::coop`), as a record's is under TLS.
    pub async fn write_pair(&mut self, first: &[u8], second: &[u8]) -> Result<Drained, IoError> {
        let mut drained = Drained::default();
        match self {
            Self::Plain(stream) => {
                let (mut a, mut b) = (first, second);
                while !a.is_empty() || !b.is_empty() {
                    let body = offer(b);
                    let slices = [IoSlice::new(a), IoSlice::new(body)];
                    hyper_rt::coop::consume_budget().await;
                    // The socket refused the offer whole: it is full, and what it takes from
                    // here its client makes room for. A Windows socket says so only this way: it
                    // takes all of an offer while it is under its quota (`offer`).
                    let Some(n) = stream
                        .try_write_vectored(&slices)
                        .map_err(IoError::Socket)?
                    else {
                        drained.full.get_or_insert_with(Instant::now);
                        stream.writable().await.map_err(IoError::Socket)?;
                        continue;
                    };
                    if n == 0 {
                        return Err(IoError::Stalled);
                    }
                    let offered = a.len().saturating_add(body.len());
                    drained.took(n);
                    // The socket took less than it was offered: it is full, as one that refused.
                    if n < offered {
                        drained.full.get_or_insert_with(Instant::now);
                    }
                    let of_a = n.min(a.len());
                    a = a.get(of_a..).unwrap_or_default();
                    b = b.get(n.saturating_sub(of_a)..).unwrap_or_default();
                }
            }
            Self::Tls(tls) => {
                for bytes in [first, second] {
                    let mut at = 0usize;
                    while let Some(rest) = bytes.get(at..).filter(|rest| !rest.is_empty()) {
                        let took = tls.connection.writer().write(rest).map_err(IoError::Io)?;
                        if took == 0 {
                            return Err(IoError::Stalled);
                        }
                        at = at.checked_add(took).ok_or(IoError::Stalled)?;
                        drained.took(took);
                        tls.flush(&mut drained.full).await?;
                    }
                }
                tls.flush(&mut drained.full).await?;
            }
        }
        Ok(drained)
    }

    /// Ends the sending half: TLS's `close_notify`, then TCP's FIN; what the client still sends
    /// may be read after.
    pub async fn close(&mut self) -> Result<(), IoError> {
        let stream = match self {
            Self::Plain(stream) => stream,
            Self::Tls(tls) => {
                tls.connection.send_close_notify();
                tls.flush(&mut None).await?;
                &tls.stream
            }
        };
        stream.shutdown(Shutdown::Write).map_err(IoError::Socket)
    }
}
