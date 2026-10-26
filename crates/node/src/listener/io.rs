//! A connection's octets, plain or under TLS (node.md §4.1): plaintext read into the connection's
//! buffer and written from its own, the socket's readiness awaited through the shard's driver, so
//! a client that sends or reads slowly yields the shard rather than holding it. Under TLS the
//! records are hyper-tls's, read from and written to the non-blocking socket, the handshake run by
//! the first read; the connection's configuration is its own and lent to each call that can
//! advance the handshake.

use std::io::{self, Read as _, Write as _};

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

/// A connection's transport: plain HTTP on loopback, TLS elsewhere.
pub enum Io {
    Plain(TcpStream),
    Tls(Box<Tls>),
}

/// A TLS connection: its socket, its state and the configuration its handshake reads.
pub struct Tls {
    stream: TcpStream,
    connection: ServerConnection,
    config: ServerConfig,
}

impl Tls {
    pub fn new(stream: TcpStream, config: ServerConfig) -> Result<Self, IoError> {
        let connection = ServerConnection::new(&config).map_err(IoError::Tls)?;
        Ok(Self {
            stream,
            connection,
            config,
        })
    }

    /// Sends what the connection holds of records, waiting while the socket is full.
    async fn flush(&mut self) -> Result<(), IoError> {
        while self.connection.wants_write() {
            match self.connection.write_tls(&mut Socket(&self.stream)) {
                Ok(0) => return Err(IoError::Stalled),
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
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
    /// framing tells a whole message from a cut one (RFC 9112 §9.8).
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize, IoError> {
        match self {
            Self::Plain(stream) => loop {
                if let Some(n) = stream.try_read(buf).map_err(IoError::Socket)? {
                    return Ok(n);
                }
                stream.readable().await.map_err(IoError::Socket)?;
            },
            Self::Tls(tls) => loop {
                match tls.connection.reader().read(buf) {
                    Ok(n) => return Ok(n),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(0),
                    Err(e) => return Err(IoError::Io(e)),
                }
                // What the handshake has to say goes before more is read: the server's flight.
                tls.flush().await?;
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
                    let _ = tls.flush().await;
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
                    at = at.checked_add(took).ok_or(IoError::Stalled)?;
                    tls.flush().await?;
                }
                tls.flush().await
            }
        }
    }

    /// Ends the sending half: TLS's `close_notify`, then TCP's FIN; what the client still sends
    /// may be read after.
    pub async fn close(&mut self) -> Result<(), IoError> {
        let stream = match self {
            Self::Plain(stream) => stream,
            Self::Tls(tls) => {
                tls.connection.send_close_notify();
                tls.flush().await?;
                &tls.stream
            }
        };
        stream.shutdown(Shutdown::Write).map_err(IoError::Socket)
    }
}
