//! A fresh cell of three for one case of `mantle bench transfer`: made by `mantle init` in a
//! directory of the run's, on loopback, each founder's volume stated at the bytes the case gives
//! it and in a failure domain of its own, and each founder run as `mantle serve` from what init
//! wrote, a child of this process. Each founder states its addresses with port 0, so each binds
//! what the operating system gives it, and once every one is bound each is told where the others
//! listen (`learn`), as an operator tells a node its peers; every founder pins the harness's gateway
//! before it starts. A child's status lines (`bound`, `s3-bound`, `enlisted`, each range's
//! `status`, and `chunks` when asked) are read on a thread of its own, one for each node and no
//! more, and every wait ends on the line it waits for or on a process's output ending, never on a
//! guess of time.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, Sender, channel};

use super::place::Gateway;

/// The founders a case's cell has: three, one failure domain each, so a block's three copies sit
/// on three nodes (durability.md §5; research/76).
pub const FOUNDERS: usize = 3;

#[derive(Debug)]
pub enum CellError {
    Io(String, std::io::Error),
    /// `mantle init` failed, or said what this runner cannot read.
    Init(String),
    /// A node's process ended before it said what it was waited for.
    Ended(u64),
}

impl std::fmt::Display for CellError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(what, e) => write!(f, "{what}: {e}"),
            Self::Init(why) => write!(f, "mantle init: {why}"),
            Self::Ended(id) => write!(f, "node {id}'s process ended"),
        }
    }
}

fn io(what: &str) -> impl FnOnce(std::io::Error) -> CellError + '_ {
    move |e| CellError::Io(what.to_owned(), e)
}

/// A node's chunks as it answers `chunks`: the bytes received from peers over QUIC, put on its own
/// volumes in-process, held by its volumes, and of the first two kept by none; and its volumes'
/// room, in the two things that refuse a put `Full`: the fragments their indexes hold and the most
/// they may, and the bytes of their free segments.
#[derive(Clone, Copy, Debug)]
pub struct Chunks {
    pub received: u64,
    pub written: u64,
    pub held: u64,
    pub unkept: u64,
    pub fragments: u64,
    pub budget: u64,
    pub free: u64,
}

/// What a node's process said, as this runner reads it.
enum Said {
    /// Its QUIC listener's address and its datagram plane's, once bound.
    Bound(SocketAddr, SocketAddr),
    /// Its S3 listener's address, once bound.
    S3(SocketAddr),
    /// The region its S3 endpoint serves and its clients sign for.
    Region(String),
    /// The root range took its enlistment.
    Enlisted,
    /// Its chunks, asked.
    Chunks(Chunks),
    /// A range's leader as the node knows it now, none while it knows of none.
    Status(u128, u64),
    /// Its output ended: the process is gone.
    Ended,
}

fn said(line: &str) -> Option<Said> {
    let fields: Vec<&str> = line.split_whitespace().collect();
    match fields.as_slice() {
        ["bound", quic, plane] => Some(Said::Bound(quic.parse().ok()?, plane.parse().ok()?)),
        ["s3-bound", at, _connections] => at.parse().ok().map(Said::S3),
        ["s3", _listen, region, _ids] => Some(Said::Region((*region).to_owned())),
        ["enlisted"] => Some(Said::Enlisted),
        ["status", group, _term, leader, ..] => Some(Said::Status(
            u128::from_str_radix(group, 16).ok()?,
            leader.parse().ok()?,
        )),
        [
            "chunks",
            received,
            written,
            held,
            unkept,
            fragments,
            budget,
            free,
        ] => Some(Said::Chunks(Chunks {
            received: received.parse().ok()?,
            written: written.parse().ok()?,
            held: held.parse().ok()?,
            unkept: unkept.parse().ok()?,
            fragments: fragments.parse().ok()?,
            budget: budget.parse().ok()?,
            free: free.parse().ok()?,
        })),
        _ => None,
    }
}

/// A founder: its id and directory as init wrote them, and its process while it runs.
struct Node {
    id: u64,
    dir: PathBuf,
    child: Option<Child>,
    /// Its standard input, held open: a node reads its operator's commands there.
    input: Option<ChildStdin>,
    /// Which of its processes this is, so a line a killed one said is not taken as its successor's.
    generation: u64,
    quic: Option<SocketAddr>,
    plane: Option<SocketAddr>,
    s3: Option<SocketAddr>,
    region: Option<String>,
    enlisted: bool,
    chunks: Option<Chunks>,
    /// Each range's leader as its process last said, zero for none.
    leaders: BTreeMap<u128, u64>,
}

/// A running cell of three, its processes this runner's children.
pub struct Cell {
    exe: PathBuf,
    out: PathBuf,
    nodes: Vec<Node>,
    tell: Sender<(usize, u64, Said)>,
    heard: Receiver<(usize, u64, Said)>,
    /// The gateway every founder pins, which the harness reads the cell's metadata as.
    gateway: Gateway,
}

impl Cell {
    /// A cell made in `dir`, which holds nothing yet, each founder's volume stated at
    /// `volume_bytes`, every founder started and enlisted.
    pub fn make(dir: &Path, volume_bytes: u64) -> Result<Self, CellError> {
        let exe = std::env::current_exe().map_err(io("this program's path"))?;
        let machines = dir.join("machines.conf");
        let mut text = format!("max_nodes = {FOUNDERS}\nmax_volumes = {FOUNDERS}\n");
        for n in 0..FOUNDERS {
            let volume = dir.join(format!("volume-{n}"));
            std::fs::create_dir_all(&volume).map_err(io("a founder's volume directory"))?;
            text.push_str(&format!(
                "[node]\nquic = 127.0.0.1:0\nplane = 127.0.0.1:0\ns3 = 127.0.0.1:0\n\
                 domain = rack-{n}\nvolume = {} {volume_bytes}\n",
                volume.display()
            ));
        }
        std::fs::write(&machines, text).map_err(io("the machines file"))?;
        let out = dir.join("cell");
        let made = Command::new(&exe)
            .arg("init")
            .arg(&out)
            .arg("--founders")
            .arg(&machines)
            .output()
            .map_err(io("mantle init"))?;
        let stdout = String::from_utf8_lossy(&made.stdout);
        if !made.status.success() {
            return Err(CellError::Init(format!(
                "{stdout}{}",
                String::from_utf8_lossy(&made.stderr)
            )));
        }
        let mut nodes = Vec::new();
        for line in stdout.lines() {
            let Some(rest) = line.strip_prefix("node ") else {
                continue;
            };
            let (id, rest) = rest
                .split_once(' ')
                .ok_or_else(|| CellError::Init(format!("a node line: {line}")))?;
            let (founder, _) = rest
                .split_once(": ")
                .ok_or_else(|| CellError::Init(format!("a node line: {line}")))?;
            nodes.push(Node {
                id: id
                    .parse()
                    .map_err(|_| CellError::Init(format!("a node's id: {line}")))?,
                dir: PathBuf::from(founder),
                child: None,
                input: None,
                generation: 0,
                quic: None,
                plane: None,
                s3: None,
                region: None,
                enlisted: false,
                chunks: None,
                leaders: BTreeMap::new(),
            });
        }
        if nodes.len() != FOUNDERS {
            return Err(CellError::Init(format!(
                "{} founders said, not {FOUNDERS}: {stdout}",
                nodes.len()
            )));
        }
        // Pinned before any founder starts: a founder reads its peers once, at its start.
        let founders: Vec<(u64, PathBuf)> = nodes.iter().map(|n| (n.id, n.dir.clone())).collect();
        let gateway = Gateway::pin(&out, &founders)
            .map_err(|e| CellError::Init(format!("the gateway: {e}")))?;
        let (tell, heard) = channel();
        let mut cell = Self {
            exe,
            out,
            nodes,
            tell,
            heard,
            gateway,
        };
        // Started together, then waited for: each measures its machine and its device at its
        // first `mantle serve` before it binds (node.md §1.7), and those measurements overlap.
        for at in 0..FOUNDERS {
            cell.spawn(at)?;
        }
        for at in 0..FOUNDERS {
            cell.listening(at)?;
        }
        cell.introduce()?;
        for at in 0..FOUNDERS {
            cell.until(at, |node| node.enlisted)?;
        }
        Ok(cell)
    }

    /// The directory init made the cell in.
    pub fn out(&self) -> &Path {
        &self.out
    }

    /// The gateway every founder pins.
    pub fn gateway(&self) -> &Gateway {
        &self.gateway
    }

    /// Each node's id and where its QUIC listener listens.
    pub fn quic(&self) -> Vec<(u64, SocketAddr)> {
        self.nodes
            .iter()
            .filter_map(|n| n.quic.map(|q| (n.id, q)))
            .collect()
    }

    /// Each node's S3 listener and the region its endpoint signs for, once bound.
    pub fn s3(&self) -> Vec<(SocketAddr, String)> {
        self.nodes
            .iter()
            .filter_map(|n| Some((n.s3?, n.region.clone()?)))
            .collect()
    }

    /// Starts node `at`'s process again on its directory, tells every running node where the
    /// others listen now, its own ports chosen anew, and waits until the root range took its
    /// enlistment.
    pub fn restart(&mut self, at: usize) -> Result<(), CellError> {
        self.spawn(at)?;
        self.listening(at)?;
        self.introduce()?;
        self.until(at, |node| node.enlisted)
    }

    /// Waits until node `at`'s listeners both listen.
    fn listening(&mut self, at: usize) -> Result<(), CellError> {
        self.until(at, |node| {
            node.quic.is_some() && node.s3.is_some() && node.region.is_some()
        })
    }

    /// Starts node `at`'s process, its output read on a thread of its own.
    fn spawn(&mut self, at: usize) -> Result<(), CellError> {
        let tell = self.tell.clone();
        let node = self
            .nodes
            .get_mut(at)
            .ok_or_else(|| CellError::Init(format!("no founder {at}")))?;
        let mut child = Command::new(&self.exe)
            .arg("serve")
            .arg(node.dir.join("config"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(io("mantle serve"))?;
        let output = child.stdout.take().ok_or_else(|| {
            CellError::Io(
                "mantle serve's output".into(),
                std::io::Error::from(std::io::ErrorKind::BrokenPipe),
            )
        })?;
        node.input = child.stdin.take();
        node.child = Some(child);
        node.generation = node.generation.saturating_add(1);
        node.quic = None;
        node.plane = None;
        node.s3 = None;
        node.region = None;
        node.enlisted = false;
        node.leaders.clear();
        let generation = node.generation;
        std::thread::Builder::new()
            .name(format!("node-{}", node.id))
            .spawn(move || {
                // A node says a range's status each time its commit moves, several times an
                // object, and the cell takes what its threads tell only as it waits: each range's
                // leader is told as it changes, the one fact of a status the cell waits on, so what
                // waits to be taken is bounded by the cell's events, not by its objects.
                let mut leaders: BTreeMap<u128, u64> = BTreeMap::new();
                for line in BufReader::new(output).lines() {
                    let Ok(line) = line else { break };
                    let Some(said) = said(&line) else {
                        continue;
                    };
                    if let Said::Status(group, leader) = said
                        && leaders.insert(group, leader) == Some(leader)
                    {
                        continue;
                    }
                    if tell.send((at, generation, said)).is_err() {
                        return;
                    }
                }
                let _ = tell.send((at, generation, Said::Ended));
            })
            .map_err(io("a node's reader thread"))?;
        Ok(())
    }

    /// Waits until node `at` holds `done`, taking every node's lines meanwhile; a running node's
    /// process ending first, its own or another's, is an error.
    fn until(&mut self, at: usize, done: impl Fn(&Node) -> bool) -> Result<(), CellError> {
        loop {
            let node = self
                .nodes
                .get(at)
                .ok_or_else(|| CellError::Init(format!("no founder {at}")))?;
            if done(node) {
                return Ok(());
            }
            self.hear()?;
        }
    }

    /// Tells each running node where every other listens, its QUIC and its plane: `learn ID QUIC
    /// PLANE` on its standard input, a line a peer.
    fn introduce(&mut self) -> Result<(), CellError> {
        let bound: Vec<(u64, SocketAddr, SocketAddr)> = self
            .nodes
            .iter()
            .filter_map(|n| Some((n.id, n.quic?, n.plane?)))
            .collect();
        for node in &mut self.nodes {
            let Some(input) = node.input.as_mut() else {
                continue;
            };
            for (peer, quic, plane) in bound.iter().filter(|(peer, ..)| *peer != node.id) {
                writeln!(input, "learn {peer} {quic} {plane}").map_err(io("a node's input"))?;
            }
            input.flush().map_err(io("a node's input"))?;
        }
        Ok(())
    }

    /// Node `at`'s chunks, asked now on its standard input ([`Chunks`]).
    pub fn chunks(&mut self, at: usize) -> Result<Chunks, CellError> {
        let node = self
            .nodes
            .get_mut(at)
            .ok_or_else(|| CellError::Init(format!("no founder {at}")))?;
        node.chunks = None;
        let input = node.input.as_mut().ok_or(CellError::Ended(node.id))?;
        input.write_all(b"chunks\n").map_err(io("a node's input"))?;
        input.flush().map_err(io("a node's input"))?;
        self.until(at, |node| node.chunks.is_some())?;
        self.nodes
            .get(at)
            .and_then(|node| node.chunks)
            .ok_or_else(|| CellError::Init(format!("no founder {at}")))
    }

    /// Waits until every running node names a running node as the leader of every range any of
    /// them knows: after a kill, until each range the killed node led has elected another, the
    /// fact a read through the others needs.
    pub fn led(&mut self) -> Result<(), CellError> {
        loop {
            let running: Vec<u64> = self
                .nodes
                .iter()
                .filter(|n| n.child.is_some())
                .map(|n| n.id)
                .collect();
            let groups: std::collections::BTreeSet<u128> = self
                .nodes
                .iter()
                .filter(|n| n.child.is_some())
                .flat_map(|n| n.leaders.keys().copied())
                .collect();
            let led = self.nodes.iter().filter(|n| n.child.is_some()).all(|n| {
                groups.iter().all(|g| {
                    n.leaders
                        .get(g)
                        .is_some_and(|leader| running.contains(leader))
                })
            });
            if led && !groups.is_empty() {
                return Ok(());
            }
            self.hear()?;
        }
    }

    /// Takes the next line any node's process said; a running node's process ending is an
    /// error.
    fn hear(&mut self) -> Result<(), CellError> {
        let (from, generation, said) = self
            .heard
            .recv()
            .map_err(|_| CellError::Init("every node's reader ended".into()))?;
        let Some(node) = self.nodes.get_mut(from) else {
            return Ok(());
        };
        if generation != node.generation {
            return Ok(());
        }
        match said {
            Said::Bound(quic, plane) => {
                node.quic = Some(quic);
                node.plane = Some(plane);
            }
            Said::S3(s3) => node.s3 = Some(s3),
            Said::Region(region) => node.region = Some(region),
            Said::Enlisted => node.enlisted = true,
            Said::Chunks(chunks) => node.chunks = Some(chunks),
            Said::Status(group, leader) => {
                node.leaders.insert(group, leader);
            }
            Said::Ended => {
                node.input = None;
                if node.child.take().is_some() {
                    return Err(CellError::Ended(node.id));
                }
            }
        }
        Ok(())
    }

    /// Kills node `at`'s process, `SIGKILL` on Unix and `TerminateProcess` on Windows, and reaps
    /// it.
    pub fn kill(&mut self, at: usize) -> Result<(), CellError> {
        let node = self
            .nodes
            .get_mut(at)
            .ok_or_else(|| CellError::Init(format!("no founder {at}")))?;
        node.input = None;
        if let Some(mut child) = node.child.take() {
            child.kill().map_err(io("a node's kill"))?;
            child.wait().map_err(io("a node's end"))?;
        }
        Ok(())
    }
}

impl Drop for Cell {
    fn drop(&mut self) {
        for at in 0..self.nodes.len() {
            let _ = self.kill(at);
        }
    }
}
