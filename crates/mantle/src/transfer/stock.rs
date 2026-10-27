//! The HTTP door of `mantle bench transfer`, a node's S3 listener asked by stock clients: boto3,
//! one process a case (`boto.py`) that puts and gets the objects the harness asks for, small ones
//! whole and large ones in explicit parts; and the AWS CLI, a process a copy, streaming a large
//! object from its standard input and to its standard output. Their bytes are made by the
//! generator as they are written to them and checked against the generator as they are read back,
//! so neither side holds more than a small object or a part at a time. Their credentials are the
//! profile `mantle init` wrote; their config names the node's endpoint, which init leaves out for a
//! founder whose port is chosen as it binds (node.md §1.5), as the stock-client tests write it.

use std::ffi::OsString;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
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

/// The operator's S3 profile for the listener at `endpoint`, written in `dir`: the environment a
/// stock client runs in, the credentials `mantle init` wrote in `cell` beside a config naming the
/// endpoint, path-style, and botocore's own transfer client.
fn profile(
    dir: &Path,
    cell: &Path,
    (endpoint, region): (SocketAddr, &str),
) -> Result<Vec<(&'static str, OsString)>, StockError> {
    let config = dir.join("aws-config");
    std::fs::write(
        &config,
        format!(
            "[profile mantle]\nregion = {region}\nendpoint_url = http://{endpoint}\ns3 =\n  \
             addressing_style = path\n  preferred_transfer_client = classic\n"
        ),
    )
    .map_err(io("the S3 profile's config"))?;
    Ok(vec![
        ("AWS_CONFIG_FILE", config.into_os_string()),
        (
            "AWS_SHARED_CREDENTIALS_FILE",
            cell.join("s3").join("credentials").into_os_string(),
        ),
        ("AWS_PROFILE", "mantle".into()),
        // No instance metadata to look for, no pager to wait on.
        ("AWS_EC2_METADATA_DISABLED", "true".into()),
        ("AWS_PAGER", OsString::new()),
    ])
}

/// Writes `object`'s bytes from `from` up to `to` to `into`, a piece of `piece` at a time as the
/// generator makes them.
fn feed(
    into: &mut impl Write,
    object: &Object<'_>,
    (from, to): (u64, u64),
    piece: &mut [u8],
) -> Result<(), StockError> {
    let mut at = from;
    while at < to {
        let n = usize::try_from(to.saturating_sub(at))
            .unwrap_or(usize::MAX)
            .min(piece.len());
        let made = piece
            .get_mut(..n)
            .ok_or_else(|| StockError::Said("a piece past its buffer".into()))?;
        object
            .fill(at, made)
            .map_err(|e| StockError::Said(e.to_string()))?;
        into.write_all(made).map_err(io("a stock client's input"))?;
        at = at.saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
    }
    Ok(())
}

/// Reads `got` bytes from `from` a piece of `piece` at a time, each checked against `object` as
/// far as its `length` goes: the first offset not the generator's, if any.
fn check(
    from: &mut impl Read,
    object: &Object<'_>,
    (got, length): (u64, u64),
    (piece, scratch): (&mut [u8], &mut [u8]),
) -> Result<Option<u64>, StockError> {
    let mut wrong = None;
    let mut at = 0u64;
    while at < got {
        let n = usize::try_from(got.saturating_sub(at))
            .unwrap_or(usize::MAX)
            .min(piece.len());
        let read = piece
            .get_mut(..n)
            .ok_or_else(|| StockError::Said("a piece past its buffer".into()))?;
        from.read_exact(read)
            .map_err(io("a stock client's output"))?;
        if wrong.is_none() && at < length {
            let mine = usize::try_from(length.saturating_sub(at))
                .unwrap_or(usize::MAX)
                .min(n);
            let checked = read
                .get(..mine)
                .ok_or_else(|| StockError::Said("a piece past its buffer".into()))?;
            wrong = object
                .verify(at, checked, scratch)
                .map_err(|e| StockError::Said(e.to_string()))?;
        }
        at = at.saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
    }
    Ok(wrong)
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
        let env = profile(dir, cell, (endpoint, region))?;
        let mut child = Command::new(PYTHON)
            .arg("-I")
            .arg(&script)
            .arg(format!("http://{endpoint}"))
            .arg(region)
            .arg(bucket)
            .envs(env)
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

    /// The driver's answer to a step: none for `ok`, its refusal for `error CODE`.
    fn answered(&mut self) -> Result<Option<Came>, StockError> {
        let said = self.line()?;
        match said.strip_prefix("error ") {
            None if said == "ok" => Ok(None),
            None => Err(StockError::Said(said)),
            Some(code) => Ok(Some(Came::Refused(super::status_of_code(code)))),
        }
    }

    /// Puts `object`, `length` bytes, at `key`, the bytes made a piece at a time as they are
    /// written to the driver.
    pub fn put(&mut self, key: &str, object: &Object<'_>, length: u64) -> Result<Came, StockError> {
        writeln!(self.input, "put {key} {length}").map_err(io("boto3's driver's input"))?;
        feed(&mut self.input, object, (0, length), &mut self.piece)?;
        self.input.flush().map_err(io("boto3's driver's input"))?;
        Ok(self.answered()?.unwrap_or(Came::Done))
    }

    /// Puts `object`, `length` bytes, at `key` as a multipart upload of parts of `part` bytes,
    /// each part's bytes made and written once the driver took the step before: a part refused
    /// ends the upload, aborted, with nothing more written.
    pub fn mput(
        &mut self,
        key: &str,
        object: &Object<'_>,
        (length, part): (u64, u64),
    ) -> Result<Came, StockError> {
        writeln!(self.input, "mput {key} {length} {part}").map_err(io("boto3's driver's input"))?;
        self.input.flush().map_err(io("boto3's driver's input"))?;
        if let Some(refused) = self.answered()? {
            return Ok(refused);
        }
        let mut at = 0u64;
        while at < length {
            let to = at.saturating_add(part).min(length);
            feed(&mut self.input, object, (at, to), &mut self.piece)?;
            self.input.flush().map_err(io("boto3's driver's input"))?;
            if let Some(refused) = self.answered()? {
                return Ok(refused);
            }
            at = to;
        }
        Ok(self.answered()?.unwrap_or(Came::Done))
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
        let wrong = check(
            &mut self.output,
            object,
            (got, length),
            (&mut self.piece, &mut scratch),
        )?;
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

/// The AWS CLI v2 as an operator runs it, a process a copy: `aws s3 cp` from its standard input,
/// its length stated so the CLI sizes its parts for S3's 10,000, and to its standard output. What
/// it says of a failure goes to a file of the case's, read once it exits, so no pipe it fills can
/// stall the copy; S3's code for a refusal is read from it.
pub struct Cli {
    env: Vec<(&'static str, OsString)>,
    errors: PathBuf,
    piece: Vec<u8>,
    scratch: Vec<u8>,
}

/// The AWS CLI's program, found on the PATH as the stock-client tests run it.
const AWS: &str = "aws";

impl Cli {
    /// The CLI for the listener at `endpoint`, signing for `region` with the profile `mantle
    /// init` wrote in `cell`, its config and what it says of failures kept in `dir`.
    pub fn new(
        dir: &Path,
        cell: &Path,
        (endpoint, region): (SocketAddr, &str),
    ) -> Result<Self, StockError> {
        Ok(Self {
            env: profile(dir, cell, (endpoint, region))?,
            errors: dir.join("aws-errors"),
            piece: vec![0u8; mantle_client::PIECE],
            scratch: vec![0u8; mantle_client::PIECE],
        })
    }

    fn command(&self, args: &[&str]) -> Result<Command, StockError> {
        let errors = std::fs::File::create(&self.errors).map_err(io("the CLI's errors"))?;
        let mut command = Command::new(AWS);
        command
            .args(args)
            .envs(self.env.iter().map(|(k, v)| (k, v)))
            .stderr(errors);
        Ok(command)
    }

    /// A copy's command: errors alone said, no progress drawn.
    fn copy(&self, args: &[&str]) -> Result<Command, StockError> {
        let mut command = self.command(&["s3", "cp"])?;
        command
            .args(args)
            .args(["--only-show-errors", "--no-progress"]);
        Ok(command)
    }

    /// The keys `aws s3 ls` lists in `bucket`, its last word a line. The CLI exits 1 where it
    /// lists nothing.
    pub fn list(&mut self, bucket: &str) -> Result<Vec<String>, StockError> {
        let listed = self
            .command(&["s3", "ls", &format!("s3://{bucket}/")])?
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .output()
            .map_err(io("the AWS CLI"))?;
        let said = std::fs::read_to_string(&self.errors).map_err(io("the CLI's errors"))?;
        if !listed.status.success() && !(listed.stdout.is_empty() && said.trim().is_empty()) {
            return Err(StockError::Said(format!(
                "aws s3 ls exited {}: {said}",
                listed.status
            )));
        }
        Ok(String::from_utf8_lossy(&listed.stdout)
            .lines()
            .filter_map(|line| line.split_whitespace().last().map(str::to_owned))
            .collect())
    }

    /// How a copy that exited `status` came out: done, or the refusal S3's code in what it said
    /// names; a failure with no code is the run's, not the cell's.
    fn came(&self, status: std::process::ExitStatus) -> Result<Came, StockError> {
        if status.success() {
            return Ok(Came::Done);
        }
        let said = std::fs::read_to_string(&self.errors).map_err(io("the CLI's errors"))?;
        // botocore's ClientError says "An error occurred (CODE) when calling the OP operation".
        let code = said
            .split_once("An error occurred (")
            .and_then(|(_, rest)| rest.split_once(')'))
            .map(|(code, _)| code);
        match code {
            Some(code) => Ok(Came::Refused(super::status_of_code(code))),
            None => Err(StockError::Said(format!("aws exited {status}: {said}"))),
        }
    }

    /// Puts `object`, `length` bytes, at `key` in `bucket` from the CLI's standard input, the
    /// bytes made a piece at a time as they are written to it.
    pub fn put(
        &mut self,
        (bucket, key): (&str, &str),
        object: &Object<'_>,
        length: u64,
    ) -> Result<Came, StockError> {
        let to = format!("s3://{bucket}/{key}");
        let size = length.to_string();
        let mut child = self
            .copy(&["-", &to, "--expected-size", &size])?
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .map_err(io("the AWS CLI"))?;
        let mut input = child
            .stdin
            .take()
            .ok_or_else(|| StockError::Said("the CLI has no standard input".into()))?;
        let fed = feed(&mut input, object, (0, length), &mut self.piece);
        // Its input ends, so the CLI's last part goes and it completes the upload.
        drop(input);
        let status = child.wait().map_err(io("the AWS CLI"))?;
        match (fed, self.came(status)?) {
            // A CLI that refused stopped reading: what it said is the answer.
            (_, refused @ Came::Refused(_)) => Ok(refused),
            (Err(e), _) => Err(e),
            (Ok(()), came) => Ok(came),
        }
    }

    /// Gets `key` in `bucket` to the CLI's standard output and checks it is `object`'s `length`
    /// bytes, a piece at a time as it is read.
    pub fn get(
        &mut self,
        (bucket, key): (&str, &str),
        object: &Object<'_>,
        length: u64,
    ) -> Result<Came, StockError> {
        let from = format!("s3://{bucket}/{key}");
        let mut child = self
            .copy(&[&from, "-"])?
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .spawn()
            .map_err(io("the AWS CLI"))?;
        let mut output = child
            .stdout
            .take()
            .ok_or_else(|| StockError::Said("the CLI has no standard output".into()))?;
        // Read to its end, whatever it holds: a wrong length is a wrong object.
        let mut wrong = None;
        let mut got = 0u64;
        loop {
            let n = output
                .read(&mut self.piece)
                .map_err(io("the CLI's output"))?;
            if n == 0 {
                break;
            }
            let read = self
                .piece
                .get(..n)
                .ok_or_else(|| StockError::Said("a piece past its buffer".into()))?;
            if wrong.is_none() && got < length {
                let mine = usize::try_from(length.saturating_sub(got))
                    .unwrap_or(usize::MAX)
                    .min(n);
                let checked = read
                    .get(..mine)
                    .ok_or_else(|| StockError::Said("a piece past its buffer".into()))?;
                wrong = object
                    .verify(got, checked, &mut self.scratch)
                    .map_err(|e| StockError::Said(e.to_string()))?;
            }
            got = got.saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
        }
        drop(output);
        let status = child.wait().map_err(io("the AWS CLI"))?;
        Ok(match self.came(status)? {
            refused @ Came::Refused(_) => refused,
            _ => match wrong {
                Some(at) => Came::Wrong(at),
                None if got != length => Came::Wrong(got.min(length)),
                None => Came::Done,
            },
        })
    }
}
