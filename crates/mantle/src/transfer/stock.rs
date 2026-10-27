//! The HTTP door of `mantle bench transfer`: boto3 through a node's S3 listener, one process a
//! case (`boto.py`) that puts and gets the objects the harness asks for, their bytes made by the
//! generator as they are written to it and checked against the generator as they are read back,
//! so neither side holds more than one small object at a time. Its credentials are the profile
//! `mantle init` wrote; its config names the node's endpoint, which init leaves out for a founder
//! whose port is chosen as it binds (node.md §1.5), as the stock-client tests write it.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::SocketAddr;
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use super::generate::Object;
use super::native::Came;

/// The driver, written out beside the case's cell and run by the host's Python.
const DRIVER: &str = include_str!("boto.py");

/// The Python a host has: `python` on Windows, `python3` elsewhere, as the stock-client tests run.
const PYTHON: &str = if cfg!(windows) { "python" } else { "python3" };

#[derive(Debug)]
pub enum StockError {
    Io(String, std::io::Error),
    /// The driver said what this door cannot read, or ended.
    Said(String),
}

impl std::fmt::Display for StockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(what, e) => write!(f, "{what}: {e}"),
            Self::Said(what) => write!(f, "boto3's driver: {what}"),
        }
    }
}

fn io(what: &str) -> impl FnOnce(std::io::Error) -> StockError + '_ {
    move |e| StockError::Io(what.to_owned(), e)
}

/// A running boto3 driver for one node's listener.
pub struct Boto {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    piece: Vec<u8>,
}

impl Boto {
    /// Starts the driver in `dir` for `bucket` through the listener at `endpoint`, signing for
    /// `region` with the profile `mantle init` wrote in `cell`.
    pub fn start(
        dir: &Path,
        cell: &Path,
        (endpoint, region): (SocketAddr, &str),
        bucket: &str,
    ) -> Result<Self, StockError> {
        let script = dir.join("boto.py");
        std::fs::write(&script, DRIVER).map_err(io("boto3's driver"))?;
        let config = dir.join("aws-config");
        std::fs::write(
            &config,
            format!(
                "[profile mantle]\nregion = {region}\nendpoint_url = http://{endpoint}\ns3 =\n  \
                 addressing_style = path\n  preferred_transfer_client = classic\n"
            ),
        )
        .map_err(io("the S3 profile's config"))?;
        let mut child = Command::new(PYTHON)
            .arg("-I")
            .arg(&script)
            .arg(format!("http://{endpoint}"))
            .arg(region)
            .arg(bucket)
            .env("AWS_CONFIG_FILE", &config)
            .env(
                "AWS_SHARED_CREDENTIALS_FILE",
                cell.join("s3").join("credentials"),
            )
            .env("AWS_PROFILE", "mantle")
            // No instance metadata to look for.
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(io("boto3's driver"))?;
        let input = child
            .stdin
            .take()
            .ok_or_else(|| StockError::Said("no standard input".into()))?;
        let output = child
            .stdout
            .take()
            .map(BufReader::new)
            .ok_or_else(|| StockError::Said("no standard output".into()))?;
        let mut boto = Self {
            child,
            input,
            output,
            piece: vec![0u8; mantle_client::PIECE],
        };
        // Its start-up, Python's and boto3's, is no put's or get's to be timed.
        let said = boto.line()?;
        if said != "ready" {
            return Err(StockError::Said(said));
        }
        Ok(boto)
    }

    /// The driver's next line.
    fn line(&mut self) -> Result<String, StockError> {
        let mut line = String::new();
        let n = self
            .output
            .read_line(&mut line)
            .map_err(io("boto3's driver's output"))?;
        if n == 0 {
            return Err(StockError::Said("its output ended".into()));
        }
        Ok(line.trim_end().to_owned())
    }

    /// Puts `object`, `length` bytes, at `key`, the bytes made a piece at a time as they are
    /// written to the driver.
    pub fn put(&mut self, key: &str, object: &Object<'_>, length: u64) -> Result<Came, StockError> {
        writeln!(self.input, "put {key} {length}").map_err(io("boto3's driver's input"))?;
        let mut at = 0u64;
        while at < length {
            let n = usize::try_from(length.saturating_sub(at))
                .unwrap_or(usize::MAX)
                .min(self.piece.len());
            let piece = self
                .piece
                .get_mut(..n)
                .ok_or_else(|| StockError::Said("a piece past its buffer".into()))?;
            object
                .fill(at, piece)
                .map_err(|e| StockError::Said(e.to_string()))?;
            self.input
                .write_all(piece)
                .map_err(io("boto3's driver's input"))?;
            at = at.saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
        }
        self.input.flush().map_err(io("boto3's driver's input"))?;
        let said = self.line()?;
        Ok(match said.strip_prefix("error ") {
            None if said == "ok" => Came::Done,
            None => return Err(StockError::Said(said)),
            Some(code) => Came::Refused(super::status_of_code(code)),
        })
    }

    /// Gets `key` and checks it is `object`'s `length` bytes, a piece at a time as it is read.
    pub fn get(&mut self, key: &str, object: &Object<'_>, length: u64) -> Result<Came, StockError> {
        writeln!(self.input, "get {key}").map_err(io("boto3's driver's input"))?;
        self.input.flush().map_err(io("boto3's driver's input"))?;
        let said = self.line()?;
        if let Some(code) = said.strip_prefix("error ") {
            return Ok(Came::Refused(super::status_of_code(code)));
        }
        let got: u64 = said
            .strip_prefix("len ")
            .and_then(|n| n.parse().ok())
            .ok_or_else(|| StockError::Said(said.clone()))?;
        // The driver's bytes are read whole whatever they hold, so its next line is a line.
        let mut scratch = vec![0u8; self.piece.len()];
        let mut wrong = None;
        let mut at = 0u64;
        while at < got {
            let n = usize::try_from(got.saturating_sub(at))
                .unwrap_or(usize::MAX)
                .min(self.piece.len());
            let piece = self
                .piece
                .get_mut(..n)
                .ok_or_else(|| StockError::Said("a piece past its buffer".into()))?;
            self.output
                .read_exact(piece)
                .map_err(io("boto3's driver's output"))?;
            if wrong.is_none() && at < length {
                let mine = usize::try_from(length.saturating_sub(at))
                    .unwrap_or(usize::MAX)
                    .min(n);
                let checked = piece
                    .get(..mine)
                    .ok_or_else(|| StockError::Said("a piece past its buffer".into()))?;
                wrong = object
                    .verify(at, checked, &mut scratch)
                    .map_err(|e| StockError::Said(e.to_string()))?;
            }
            at = at.saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
        }
        Ok(match wrong {
            Some(at) => Came::Wrong(at),
            None if got != length => Came::Wrong(got.min(length)),
            None => Came::Done,
        })
    }
}

impl Drop for Boto {
    fn drop(&mut self) {
        let _ = writeln!(self.input, "quit");
        let _ = self.input.flush();
        let _ = self.child.wait();
    }
}
