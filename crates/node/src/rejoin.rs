//! A node the cell wrote off coming back as a new member (docs/design/membership.md §5.1): its
//! request for a new identity ([`ask`], step 3), the wipe (step 4) and the identity's switch (step
//! 5), each an act done whole and durable before the next, and the resume table a start reads its
//! place in them from, its files alone.
//!
//! The node's identity is kept in one of two places in the `identity` directory beside its
//! configuration, `node.der` and `node.key` or `next.der` and `next.key`: the configuration names
//! one, and the next identity is written to the other (steps 3 and 4), so a node written off again
//! after a rejoin writes its next identity where its last switch left room. Which place the
//! configuration names is read by the fingerprint of the certificate it names, not by how its path
//! is spelled. An identity the configuration names elsewhere, at a path its operator wrote, is left
//! there by the switch: nothing here removes a path a configuration's value names, only one built
//! from the node's layout and these names.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::time::Duration;

use hyper_rt::combine::{Either, race2};
use hyper_rt_transport::Driver;
use hyper_transport::tls::Tls;
use hyper_transport::{Config, Endpoint, Event, ExchangeId, Fixed, PeerId, Progress, Refusal};
use mantle_cert::KeyPair;
use mantle_meta::record::FINGERPRINT;
use mantle_protocol::bounds::{self, Stated};
use mantle_protocol::classes::{self, Kind, NodeClasses, Role};
use mantle_protocol::enroll as wire;

use crate::directory::Peers;
use crate::files;
use crate::layout::{LIVE, LOG, Layout, LayoutError, RUN, STOPPED};
use crate::secret::{self, SecretError};
use crate::storage::{VOLUME_NEW, mantle_dir, volume_path};

/// The directory beside the configuration that keeps the node's identity (`mantle init`).
const IDENTITY: &str = "identity";

/// A place an identity is kept in [`IDENTITY`]: its certificate's name and its key's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Place {
    certificate: &'static str,
    key: &'static str,
}

/// The place `mantle init` writes a node's identity to.
const NODE: Place = Place {
    certificate: "node.der",
    key: "node.key",
};
/// The other place.
const NEXT: Place = Place {
    certificate: "next.der",
    key: "next.key",
};

impl Place {
    /// Its certificate's file beside the configuration at `file`.
    fn certificate(self, file: &Path) -> PathBuf {
        identity(file).join(self.certificate)
    }

    /// Its key's file beside the configuration at `file`.
    fn key(self, file: &Path) -> PathBuf {
        identity(file).join(self.key)
    }
}

/// The identity directory beside the configuration at `file`, which names its paths from its own
/// directory (`process::Lines`).
fn identity(file: &Path) -> PathBuf {
    file.parent().unwrap_or(Path::new("")).join(IDENTITY)
}

/// What the rejoin reads of a node's configuration, `mantle serve`'s or `mantle node`'s.
#[derive(Debug, Clone)]
pub struct Configured {
    /// The configuration's file, which the switch rewrites.
    pub file: PathBuf,
    /// The node it names.
    pub id: u64,
    /// The node's directory.
    pub dir: PathBuf,
    /// The certificate it names.
    pub certificate: PathBuf,
    /// Each range the node hosts.
    pub ranges: Vec<u128>,
    /// Each volume's device directory.
    pub volumes: Vec<PathBuf>,
}

/// Where a start stands in a rejoin, from its files alone (§5.1's resume table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resume {
    /// No marker: a member's start.
    Start,
    /// The marker and no next certificate: step 3, the ask, under the certificate whose
    /// fingerprint the marker holds.
    Ask([u8; FINGERPRINT]),
    /// The next certificate, the configuration naming the old identity: steps 4 and 5.
    Wipe,
    /// The configuration naming the new identity, the marker kept: the rest of step 5, then step
    /// 6, the join.
    Finish,
}

/// One act of steps 4 and 5, whole and durable when it is said.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Done {
    /// A member's file deleted (step 4), or the old identity's (step 5).
    Deleted(PathBuf),
    /// The configuration rewritten to name the new identity, the node it names (step 5).
    Switched(u64),
    /// The marker removed: steps 4 and 5 are done, and the next start is a member's.
    Unmarked,
}

/// Why a step of the rejoin could not be taken.
#[derive(Debug, thiserror::Error)]
pub enum RejoinError {
    #[error("{0}")]
    Layout(#[from] LayoutError),
    #[error("{op} {}: {source}", .path.display())]
    Io {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{0}")]
    Disk(#[from] hyper_block::DiskError),
    #[error("{}: {}", .0.display(), .1)]
    Certificate(PathBuf, mantle_cert::CertError),
    #[error("{}: {}", .0.display(), .1)]
    Secret(PathBuf, SecretError),
    #[error("{}: names no node", .0.display())]
    Unnamed(PathBuf),
    #[error("{}: states `{}` {} times, not once", .0.display(), .1, .2)]
    Stated(PathBuf, &'static str, usize),
    #[error("{}: no next key beside it", .0.display())]
    Keyless(PathBuf),
    #[error("{}: certifies another key than the next key beside it", .0.display())]
    Mismatched(PathBuf),
    #[error("the rejoin stands at {0:?}, not where {1} is taken")]
    Order(Resume, &'static str),
    #[error("{0} acts did not finish the rejoin's wipe and switch")]
    Unfinished(usize),
}

fn io(op: &'static str, path: &Path) -> impl FnOnce(std::io::Error) -> RejoinError {
    let path = path.to_path_buf();
    move |source| RejoinError::Io { op, path, source }
}

/// The file at `path`, none where there is none.
fn read(path: &Path) -> Result<Option<Vec<u8>>, RejoinError> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io("read", path)(e)),
    }
}

/// The fingerprint of the certificate at `path`, none where there is none.
fn fingerprint(path: &Path) -> Result<Option<[u8; FINGERPRINT]>, RejoinError> {
    Ok(read(path)?.map(|der| mantle_cert::fingerprint(&der)))
}

/// The place the configuration does not name, whose own certificate is `configured`'s
/// fingerprint: where the next identity is written before the switch, and where the old one is
/// left after it. A configuration naming neither place leaves [`NEXT`].
fn other(file: &Path, configured: [u8; FINGERPRINT]) -> Result<Place, RejoinError> {
    if fingerprint(&NEXT.certificate(file))? == Some(configured) {
        return Ok(NODE);
    }
    Ok(NEXT)
}

/// The configured certificate's fingerprint.
fn configured_fingerprint(configured: &Configured) -> Result<[u8; FINGERPRINT], RejoinError> {
    fingerprint(&configured.certificate)?
        .ok_or_else(|| io("read", &configured.certificate)(std::io::ErrorKind::NotFound.into()))
}

/// Where a start of `configured` stands in a rejoin (§5.1's resume table): no marker, a member's
/// start; the marker and no next certificate, the ask; a next certificate and the configuration
/// naming the old identity, the marker's, the wipe and the switch; the configuration naming another
/// identity, the rest of the switch and the join. A next certificate the marker names is the old
/// identity's, not the cell's answer, and asks again.
pub fn state(configured: &Configured) -> Result<Resume, RejoinError> {
    let Some(marker) = Layout::new(&configured.dir).retired()? else {
        return Ok(Resume::Start);
    };
    let named = configured_fingerprint(configured)?;
    if named != marker {
        return Ok(Resume::Finish);
    }
    let next = other(&configured.file, named)?.certificate(&configured.file);
    match fingerprint(&next)? {
        Some(answer) if answer != marker => Ok(Resume::Wipe),
        _ => Ok(Resume::Ask(marker)),
    }
}

/// The member's files the wipe deletes (§5.1 step 4): its log, each range's store, its run and
/// stop records and its readiness lock's file in its directory, and each volume, laid out or being
/// laid out. Each is a name of the node's layout in a directory the configuration names, never a
/// path a value names alone. Kept: the configuration, the cell's keys, the S3 endpoint's
/// certificate, the machine's calibration, each device's profile and every lock.
fn inventory(configured: &Configured) -> Vec<PathBuf> {
    let dir = &configured.dir;
    let mut files = Vec::with_capacity(
        configured
            .ranges
            .len()
            .saturating_add(configured.volumes.len().saturating_mul(2))
            .saturating_add(4),
    );
    files.push(dir.join(LOG));
    files.extend(
        configured
            .ranges
            .iter()
            .map(|group| crate::process::store_path(dir, *group)),
    );
    files.extend([RUN, STOPPED, LIVE].map(|name| dir.join(name)));
    for volume in &configured.volumes {
        files.push(volume_path(volume));
        files.push(mantle_dir(volume).join(VOLUME_NEW));
    }
    files
}

/// The directories the wipe deletes in: the node's and each volume's `mantle`.
fn wiped_dirs(configured: &Configured) -> impl Iterator<Item = PathBuf> {
    std::iter::once(configured.dir.clone()).chain(configured.volumes.iter().map(|v| mantle_dir(v)))
}

/// Removes the file at `path`: false where none was there.
fn remove(path: &Path) -> Result<bool, RejoinError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(io("remove", path)(e)),
    }
}

/// The acts steps 4 and 5 take at most, and the reading that finds none left: each of the
/// inventory's deletions, the switch, the old identity's two deletions and the marker's removal.
fn budget(configured: &Configured) -> Result<usize, RejoinError> {
    let acts = inventory(configured).len();
    acts.checked_add(5).ok_or(RejoinError::Unfinished(acts))
}

/// One act of steps 4 and 5, the first the files call for, whole and durable when it returns:
/// none where they call for none, at a member's start or the ask.
///
/// The wipe deletes one member's file an act; once none is left, every directory it deleted in is
/// flushed before the switch, so a crash between a deletion and its directory's flush, which a
/// resumed wipe finds deleted, leaves no member's file to come back under the new identity
/// (CLAUDE.md §6). The switch is the one act at which the identity changes: the configuration
/// rewritten whole under a temporary name, flushed, renamed over itself and its directory flushed
/// (`files::write`). Then the old identity's certificate and key go, their directory flushed, and
/// last the marker.
pub fn act(configured: &Configured) -> Result<Option<Done>, RejoinError> {
    match state(configured)? {
        Resume::Start | Resume::Ask(_) => Ok(None),
        Resume::Wipe => {
            for path in inventory(configured) {
                if remove(&path)? {
                    return Ok(Some(Done::Deleted(path)));
                }
            }
            for dir in wiped_dirs(configured) {
                if dir.try_exists().map_err(io("look for", &dir))? {
                    hyper_block::file::sync_dir(&dir)?;
                }
            }
            switch(configured).map(|id| Some(Done::Switched(id)))
        }
        Resume::Finish => {
            let file = &configured.file;
            let old = other(file, configured_fingerprint(configured)?)?;
            for path in [old.certificate(file), old.key(file)] {
                if remove(&path)? {
                    return Ok(Some(Done::Deleted(path)));
                }
            }
            hyper_block::file::sync_dir(&identity(file))?;
            Layout::new(&configured.dir).unretire()?;
            Ok(Some(Done::Unmarked))
        }
    }
}

/// Steps 4 and 5 to their end from where the files left them, each act's configuration read
/// afresh by `read`, at most [`budget`]'s acts: the configuration as the last act left it.
pub fn resume<E: From<RejoinError>>(
    read: impl Fn() -> Result<Configured, E>,
) -> Result<Configured, E> {
    let mut configured = read()?;
    let budget = budget(&configured)?;
    for _ in 0..budget {
        if act(&configured)?.is_none() {
            return Ok(configured);
        }
        configured = read()?;
    }
    Err(RejoinError::Unfinished(budget).into())
}

/// Rewrites the configuration to name the next identity, the one place it changes (step 5): the
/// node its certificate names, the certificate and its key, each line of them replaced and every
/// other kept. Refused, the configuration untouched, unless the certificate names a node and the
/// key reads, owner-only, as a node's, the one the certificate certifies.
fn switch(configured: &Configured) -> Result<u64, RejoinError> {
    let file = &configured.file;
    let next = other(file, configured_fingerprint(configured)?)?;
    let at = next.certificate(file);
    let certificate = read(&at)?.ok_or_else(|| RejoinError::Unnamed(at.clone()))?;
    let names = mantle_cert::node_names(&certificate)
        .map_err(|e| RejoinError::Certificate(at.clone(), e))?;
    let id =
        mantle_protocol::node_id(names.name).ok_or_else(|| RejoinError::Unnamed(at.clone()))?;
    certifies(&certificate, &at, &next.key(file))?;
    let text = std::fs::read_to_string(file).map_err(io("read", file))?;
    let rewritten = switched(&text, file, (id, next))?;
    files::write(file, rewritten.as_bytes())?;
    Ok(id)
}

/// Whether `certificate`, kept at `at`, certifies the key at `key`, read owner-only as a node's:
/// a certificate for another key would be an identity whose handshakes no peer takes.
fn certifies(certificate: &[u8], at: &Path, key: &Path) -> Result<(), RejoinError> {
    let pkcs8 = secret::read(key).map_err(|e| RejoinError::Secret(key.to_path_buf(), e))?;
    let pair =
        KeyPair::from_pkcs8(&pkcs8).map_err(|e| RejoinError::Certificate(key.to_path_buf(), e))?;
    let certified = mantle_cert::public_key(certificate)
        .map_err(|e| RejoinError::Certificate(at.to_path_buf(), e))?;
    if certified != pair.public_key() {
        return Err(RejoinError::Mismatched(at.to_path_buf()));
    }
    Ok(())
}

/// `text`, a configuration's lines, naming node `id` and the identity at `place`: its `id`,
/// `certificate` and `key` lines replaced, each stated once, and every other line kept.
fn switched(text: &str, file: &Path, (id, place): (u64, Place)) -> Result<String, RejoinError> {
    let mut stated = [
        ("id", id.to_string(), 0usize),
        (
            "certificate",
            format!("{IDENTITY}/{}", place.certificate),
            0,
        ),
        ("key", format!("{IDENTITY}/{}", place.key), 0),
    ];
    let mut rewritten = String::with_capacity(text.len());
    for line in text.lines() {
        // A line's key as the configuration's reader takes it: before a comment and an `=`.
        let key = line
            .split('#')
            .next()
            .and_then(|line| line.split_once('='))
            .map(|(key, _)| key.trim());
        match stated.iter_mut().find(|(name, _, _)| Some(*name) == key) {
            Some((name, value, times)) => {
                *times = times.saturating_add(1);
                rewritten.push_str(name);
                rewritten.push_str(" = ");
                rewritten.push_str(value);
            }
            None => rewritten.push_str(line),
        }
        rewritten.push('\n');
    }
    for (name, _, times) in stated {
        if times != 1 {
            return Err(RejoinError::Stated(file.to_path_buf(), name, times));
        }
    }
    Ok(rewritten)
}

/// The next identity's key (step 3): the one kept in the place the configuration does not name,
/// else one made now and kept there, owner-only, whole and durable before it is returned, so every
/// ask, after any crash, asks for the same key. Taken only at the ask.
pub fn next_key(configured: &Configured) -> Result<KeyPair, RejoinError> {
    let stands = state(configured)?;
    let Resume::Ask(marker) = stands else {
        return Err(RejoinError::Order(stands, "the ask"));
    };
    let path = other(&configured.file, marker)?.key(&configured.file);
    if path.try_exists().map_err(io("look for", &path))? {
        let pkcs8 = secret::read(&path).map_err(|e| RejoinError::Secret(path.clone(), e))?;
        return KeyPair::from_pkcs8(&pkcs8).map_err(|e| RejoinError::Certificate(path, e));
    }
    let key = KeyPair::generate().map_err(|e| RejoinError::Certificate(path.clone(), e))?;
    files::create_dir(&identity(&configured.file))?;
    files::write_secret(&path, key.pkcs8())?;
    Ok(key)
}

/// Keeps `certificate`, the cell's answer for the next key, beside that key (step 4's first act),
/// whole and durable: from here a start resumes at the wipe. Refused at any step but the ask, where
/// no next key is kept, or where it names no node.
pub fn keep_certificate(configured: &Configured, certificate: &[u8]) -> Result<(), RejoinError> {
    let stands = state(configured)?;
    let Resume::Ask(marker) = stands else {
        return Err(RejoinError::Order(stands, "the answer"));
    };
    let next = other(&configured.file, marker)?;
    let (at, key) = (
        next.certificate(&configured.file),
        next.key(&configured.file),
    );
    if !key.try_exists().map_err(io("look for", &key))? {
        return Err(RejoinError::Keyless(at));
    }
    let names = mantle_cert::node_names(certificate)
        .map_err(|e| RejoinError::Certificate(at.clone(), e))?;
    if mantle_protocol::node_id(names.name).is_none() {
        return Err(RejoinError::Unnamed(at));
    }
    certifies(certificate, &at, &key)?;
    files::write(&at, certificate)?;
    Ok(())
}

/// What [`ask`] is given: the old identity's credentials (its certificate, its key and the
/// authority's certificate its members chain to), the members to dial, each its id and where its
/// QUIC endpoint listens, at most the cell's bound on nodes, the key asked for, where its endpoint
/// binds, and the transport's bounds as the cell's nodes state them.
pub struct Asking {
    pub credentials: hyper_transport::tls::Credentials,
    pub members: Vec<(PeerId, SocketAddr)>,
    pub key: [u8; wire::KEY],
    pub bind: SocketAddr,
    pub messages: classes::Bounds,
    pub transport_bytes: u64,
}

/// Why [`ask`] came back with no new identity.
#[derive(Debug, thiserror::Error)]
pub enum AskError {
    /// A member answered the cell's refusal (membership.md §5.1 step 3): it stands until a person
    /// or a supervisor acts.
    #[error("rejoin refused {0:?}: {0}")]
    Refused(wire::Refusal),
    /// No member answered within the stock patience.
    #[error("rejoin unreachable: no member answered")]
    Unreachable,
    /// A member's answer does not decode.
    #[error("a member's answer: {0}")]
    Answer(#[from] wire::EnrollError),
    #[error("the transport: {0:?}")]
    Transport(Refusal),
    #[error("the runtime: {0}")]
    Runtime(String),
    #[error("the transport's bounds: {0}")]
    Bounds(String),
}

impl From<Refusal> for AskError {
    fn from(r: Refusal) -> Self {
        Self::Transport(r)
    }
}

impl From<hyper_rt::RtError> for AskError {
    fn from(e: hyper_rt::RtError) -> Self {
        Self::Runtime(format!("{e:?}"))
    }
}

/// The endpoint a written-off node asks from: a client's, under its old certificate, offering the
/// enrollment protocol alone.
type Asker = Driver<NodeClasses, Fixed, Peers>;

/// Asks the cell for a new identity (membership.md §5.1 step 3): dials each of `asking`'s members
/// in turn, once each, under the enrollment protocol alone and the old certificate, sends
/// `Regrant { key }` and reads the answer, each member given the stock patience
/// (`bounds::STOCK_PATIENCE`) from its dial to its answer. The first member's certificate, or the
/// first refusal of the cell's, is the round's; a member that is not reached, loses the
/// exchange, or answers that the root range did not answer in time passes the round to the next.
/// Each connection is closed once its one request is answered.
pub async fn ask(asking: Asking) -> Result<wire::Granted, AskError> {
    classes::state_bounds(asking.messages).map_err(|e| AskError::Bounds(e.to_string()))?;
    let peers = asking.members.len();
    let stated = Stated {
        peers,
        replicas: 0,
        clients: 0,
        pool: 0,
        // One request out, to one member at a time.
        ranges: 1,
        inflight: 0,
        // Nothing replicates to it: one lane, the least a peer may open.
        lanes: 1,
        transfers: 0,
        asked: 0,
        transport_bytes: asking.transport_bytes,
        messages: asking.messages,
        max_head: crate::node::MAX_HEAD,
    };
    let limits = bounds::limits(&stated).map_err(|e| AskError::Bounds(e.to_string()))?;
    let io = bounds::io(&stated);
    let mut tls = Tls::mutual(&asking.credentials, false)?;
    tls.client.alpn_protocols = vec![classes::ENROLL.to_vec()];
    let config = Config {
        tls,
        role: Role::Enrolling,
        limits,
        congestion: Default::default(),
    };
    let budget = Fixed::new(asking.transport_bytes, peers.max(1));
    // Each member is dialed by its name under the authority, as a granted node's seeds are.
    let mut directory = Peers::new(Vec::new(), 0);
    for (id, _) in &asking.members {
        directory.seed(*id);
    }
    let endpoint = Endpoint::new(
        config,
        budget,
        directory,
        hyper_rt_transport::shard_instant(),
    )?;
    let mut driver = Driver::bind(endpoint, asking.bind, io)?;
    let request = wire::Request::Regrant { key: asking.key }.encode();
    let patience = u64::try_from(bounds::STOCK_PATIENCE.as_nanos()).unwrap_or(u64::MAX);
    for (id, at) in &asking.members {
        let until_ns = hyper_rt::futures::now_ns().saturating_add(patience);
        let answered = ask_one(&mut driver, (*id, *at), &request, until_ns).await?;
        let clock = driver.clock()?;
        driver.endpoint().disconnect(clock, *id);
        match answered
            .map(|bytes| wire::Answer::decode(&bytes))
            .transpose()?
        {
            Some(wire::Answer::Granted(granted)) => return Ok(granted),
            Some(wire::Answer::Refused(wire::Refusal::Unanswered | wire::Refusal::Unled))
            | None => {}
            Some(wire::Answer::Refused(refusal)) => return Err(AskError::Refused(refusal)),
        }
    }
    Err(AskError::Unreachable)
}

/// Asks member `id`, listening at `at`, once, by `until_ns` on the shard's clock: the answer's
/// bytes, or none where it was not reached or the exchange ended without one.
async fn ask_one(
    driver: &mut Asker,
    (id, at): (PeerId, SocketAddr),
    request: &[u8],
    until_ns: u64,
) -> Result<Option<Vec<u8>>, AskError> {
    let clock = driver.clock()?;
    if driver.endpoint().connect(clock, id, at).is_err() {
        return Ok(None);
    }
    loop {
        match next_event(driver, until_ns).await? {
            Some(Event::Connected { peer, .. }) if peer == id => break,
            Some(Event::Unreachable { peer, .. } | Event::Closed { peer, .. }) if peer == id => {
                return Ok(None);
            }
            Some(_) => {}
            None => return Ok(None),
        }
    }
    let clock = driver.clock()?;
    let left = until_ns.saturating_sub(hyper_rt::futures::now_ns()).max(1);
    let period = Progress::new(Duration::from_nanos(left))?;
    let len = u64::try_from(request.len()).unwrap_or(u64::MAX);
    let Ok(exchange) = driver
        .endpoint()
        .open(clock, id, Kind::Enroll, &[], Some(len), period)
    else {
        return Ok(None);
    };
    let mut written = 0usize;
    let mut bytes = Vec::new();
    let mut replied = false;
    loop {
        while let Some(rest) = request.get(written..)
            && !rest.is_empty()
        {
            match driver.endpoint().write_body(exchange, rest) {
                Ok(0) => break,
                Ok(took) => written = written.saturating_add(took),
                Err(_) => return Ok(None),
            }
        }
        match next_event(driver, until_ns).await? {
            Some(Event::Reply { exchange: e, .. }) if e == exchange => {
                replied = true;
                if read_answer(driver, exchange, &mut bytes)? {
                    return Ok(Some(bytes));
                }
            }
            Some(Event::BodyReady { exchange: e }) if e == exchange && replied => {
                if read_answer(driver, exchange, &mut bytes)? {
                    return Ok(Some(bytes));
                }
            }
            Some(Event::Refused { exchange: e, .. }) if e == exchange => return Ok(None),
            Some(Event::Unreachable { peer, .. } | Event::Closed { peer, .. }) if peer == id => {
                return Ok(None);
            }
            Some(_) => {}
            None => {
                driver.endpoint().end(exchange);
                return Ok(None);
            }
        }
    }
}

/// Reads what has arrived of the answer on `exchange` into `into`: whether it is whole, its
/// checksum verified.
fn read_answer(
    driver: &mut Asker,
    exchange: ExchangeId,
    into: &mut Vec<u8>,
) -> Result<bool, AskError> {
    let endpoint = driver.endpoint();
    loop {
        let mut piece = endpoint.reserve(classes::Class::Request, PIECE)?;
        let got = endpoint.read_body(exchange, &mut piece);
        into.extend_from_slice(piece.bytes());
        endpoint.release(piece);
        if got? == 0 {
            break;
        }
    }
    if !endpoint.body_complete(exchange) {
        return Ok(false);
    }
    endpoint.end(exchange);
    Ok(true)
}

/// The asker's next event, or none once `until_ns` passes on the shard's clock.
async fn next_event(
    driver: &mut Asker,
    until_ns: u64,
) -> Result<Option<Event<NodeClasses>>, AskError> {
    if let Some(event) = driver.endpoint().poll_event() {
        return Ok(Some(event));
    }
    let left = until_ns.saturating_sub(hyper_rt::futures::now_ns());
    if left == 0 {
        return Ok(None);
    }
    let ended = {
        let event = pin!(driver.event());
        let sleep = pin!(hyper_rt::futures::sleep(left));
        race2(event, sleep).await
    };
    match ended {
        Either::First(event) => Ok(Some(event?)),
        Either::Second(slept) => {
            slept?;
            Ok(None)
        }
    }
}

/// The piece an answer is read in: the transport's own receive chunk.
const PIECE: u64 = hyper_transport::RECEIVE_CHUNK as u64;

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;

    use mantle_cert::{Serial, Validity};

    use super::*;

    /// A node's certificate naming node `id`, and its key.
    pub(crate) fn certificate(id: u64) -> (Vec<u8>, KeyPair) {
        let key = KeyPair::generate().unwrap();
        (certificate_for(id, &key), key)
    }

    /// A node's certificate naming node `id`, for `key`.
    pub(crate) fn certificate_for(id: u64, key: &KeyPair) -> Vec<u8> {
        let authority = KeyPair::generate().unwrap();
        mantle_cert::node(
            &authority,
            "cell",
            key.public_key(),
            &mantle_protocol::node_name(id),
            (
                Validity {
                    not_before: 0,
                    not_after: u64::from(u32::MAX),
                },
                Serial::from_octets([7; 16]),
            ),
        )
        .unwrap()
    }

    /// The files the wipe deletes in a directory [`laid_out`] made, from it.
    const MEMBERS: &[&str] = &[
        "log",
        "range-00000000000000000000000000000001.store",
        "range-00000000000000000000000000000002.store",
        "run",
        "stopped",
        "live",
        "disk/mantle/volume",
        "disk/mantle/volume.new",
    ];

    /// The files the rejoin keeps in it.
    const KEPT: &[&str] = &[
        "LOCK",
        "calibration",
        "identity/authority.der",
        "identity/s3.der",
        "identity/s3.key",
        "identity/peers/2.der",
        "keys/1",
        "keys/s3/key",
        "disk/mantle/LOCK",
        "disk/mantle/profile",
    ];

    /// A node's directory as `mantle init` lays it out and a run leaves it, node `id`'s, retired:
    /// its configuration naming two ranges and a volume, its identity in `identity/node.*`, every
    /// member's file and every kept one, and the marker holding its certificate's fingerprint. The
    /// configuration's file.
    pub(crate) fn laid_out(dir: &Path, id: u64) -> PathBuf {
        let config = dir.join("config");
        for sub in ["identity/peers", "keys/s3", "disk/mantle"] {
            std::fs::create_dir_all(dir.join(sub)).unwrap();
        }
        let (der, key) = certificate(id);
        files::write(&dir.join("identity/node.der"), &der).unwrap();
        files::write_secret(&dir.join("identity/node.key"), key.pkcs8()).unwrap();
        std::fs::write(
            &config,
            format!(
                "# made by a test\nid = {id}\ndir = .\nquic = 127.0.0.1:0\nplane = 127.0.0.1:0\n\
                 authority = identity/authority.der\ncertificate = identity/node.der # its own\n\
                 key = identity/node.key\nvoters = 1\nrange = 1 root\nrange = 2 name\n\
                 volume = disk 4096\ndomain = a\nroot.max_nodes = 1\nroot.max_volumes = 2\n"
            ),
        )
        .unwrap();
        for name in MEMBERS.iter().chain(KEPT) {
            std::fs::write(dir.join(name), name.as_bytes()).unwrap();
        }
        Layout::new(dir)
            .retire(&mantle_cert::fingerprint(&der))
            .unwrap();
        config
    }

    /// What the rejoin reads of the configuration at `file`, as `mantle serve` reads it.
    pub(crate) fn configured(file: &Path) -> Configured {
        let stated = crate::process::read_stated(file).unwrap();
        Configured {
            file: file.to_path_buf(),
            id: stated.id,
            dir: stated.dir.clone(),
            certificate: stated.certificate.clone(),
            ranges: stated.ranges.iter().map(|(group, _)| *group).collect(),
            volumes: stated.volumes.iter().map(|(dir, _)| dir.clone()).collect(),
        }
    }

    /// The cell's answer for the next key, node `id`'s certificate, kept as the ask will keep it:
    /// the key first, then the certificate for it.
    fn answered(file: &Path, id: u64) -> Vec<u8> {
        let key = next_key(&configured(file)).unwrap();
        // A second ask, after a crash, asks for the same key.
        assert_eq!(
            next_key(&configured(file)).unwrap().public_key(),
            key.public_key()
        );
        let der = certificate_for(id, &key);
        keep_certificate(&configured(file), &der).unwrap();
        der
    }

    /// The cell's answer is kept only where it certifies the next key, and the switch takes no
    /// certificate for another key than the one beside it, the configuration untouched.
    #[test]
    fn a_certificate_for_another_key_is_refused_as_the_answer_and_at_the_switch() {
        let dir = tempfile::tempdir().unwrap();
        let file = laid_out(dir.path(), 5);
        next_key(&configured(&file)).unwrap();
        let (der, _) = certificate(9);
        assert!(matches!(
            keep_certificate(&configured(&file), &der),
            Err(RejoinError::Mismatched(_))
        ));
        assert!(!dir.path().join("identity/next.der").exists());
        std::fs::write(dir.path().join("identity/next.der"), &der).unwrap();
        let before = std::fs::read_to_string(&file).unwrap();
        for _ in MEMBERS {
            assert!(matches!(
                act(&configured(&file)).unwrap(),
                Some(Done::Deleted(_))
            ));
        }
        assert!(matches!(
            act(&configured(&file)),
            Err(RejoinError::Mismatched(_))
        ));
        assert_eq!(std::fs::read_to_string(&file).unwrap(), before);
    }

    /// Every file under `dir` with its bytes, by its path from `dir`.
    fn files_under(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        let mut found = BTreeMap::new();
        let mut open = vec![dir.to_path_buf()];
        while let Some(at) = open.pop() {
            for entry in std::fs::read_dir(&at).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    open.push(path);
                } else {
                    let bytes = std::fs::read(&path).unwrap();
                    found.insert(path.strip_prefix(dir).unwrap().to_path_buf(), bytes);
                }
            }
        }
        found
    }

    /// The identity the configuration at `file` names, which is whole: its certificate and key
    /// each there, the key owner-only and a node's, the certificate naming the node the
    /// configuration states. Its fingerprint.
    fn one_identity(file: &Path) -> [u8; FINGERPRINT] {
        let stated = crate::process::read_stated(file).unwrap();
        let der = std::fs::read(&stated.certificate).unwrap();
        let named = mantle_cert::node_names(&der).unwrap().name;
        assert_eq!(mantle_protocol::node_id(named), Some(stated.id));
        KeyPair::from_pkcs8(&secret::read(&stated.key).unwrap()).unwrap();
        mantle_cert::fingerprint(&der)
    }

    /// The resume table: no marker, a member's start; the marker and no next certificate, the
    /// ask, whose key is kept once and the same after; a next certificate holding the old
    /// identity, the ask still; the cell's certificate beside the next key, the wipe; the switch
    /// done, the rest of it; the marker removed, a member's start again. The answer is refused
    /// before the next key is kept, and at any step but the ask.
    #[test]
    fn a_start_finds_its_place_in_the_rejoin_from_its_files_alone() {
        let dir = tempfile::tempdir().unwrap();
        let file = laid_out(dir.path(), 5);
        let marker = Layout::new(dir.path()).retired().unwrap().unwrap();
        assert_eq!(state(&configured(&file)).unwrap(), Resume::Ask(marker));
        let (der, _) = certificate(9);
        assert!(matches!(
            keep_certificate(&configured(&file), &der),
            Err(RejoinError::Keyless(_))
        ));
        // The old certificate copied to the next place is not the cell's answer.
        let old = std::fs::read(dir.path().join("identity/node.der")).unwrap();
        std::fs::write(dir.path().join("identity/next.der"), &old).unwrap();
        assert_eq!(state(&configured(&file)).unwrap(), Resume::Ask(marker));
        std::fs::remove_file(dir.path().join("identity/next.der")).unwrap();
        answered(&file, 9);
        assert_eq!(state(&configured(&file)).unwrap(), Resume::Wipe);
        assert!(matches!(
            next_key(&configured(&file)),
            Err(RejoinError::Order(Resume::Wipe, _))
        ));
        for deleted in MEMBERS {
            let done = act(&configured(&file)).unwrap();
            assert!(
                matches!(done, Some(Done::Deleted(_))),
                "{deleted}: {done:?}"
            );
        }
        assert_eq!(act(&configured(&file)).unwrap(), Some(Done::Switched(9)));
        assert_eq!(state(&configured(&file)).unwrap(), Resume::Finish);
        assert!(matches!(
            keep_certificate(&configured(&file), &der),
            Err(RejoinError::Order(Resume::Finish, _))
        ));
        for old in ["identity/node.der", "identity/node.key"] {
            let done = act(&configured(&file)).unwrap();
            assert_eq!(done, Some(Done::Deleted(dir.path().join(old))));
        }
        assert_eq!(act(&configured(&file)).unwrap(), Some(Done::Unmarked));
        assert_eq!(state(&configured(&file)).unwrap(), Resume::Start);
        assert_eq!(act(&configured(&file)).unwrap(), None);
        // Without the marker, no start asks or answers.
        assert!(matches!(
            next_key(&configured(&file)),
            Err(RejoinError::Order(Resume::Start, _))
        ));
    }

    /// The wipe deletes the member's files, exactly: its log, each range's store, its run, stop
    /// and readiness records, its volume and one being laid out; and keeps the configuration, the
    /// cell's keys, the S3 endpoint's certificate and key, the pinned peers, the calibration, the
    /// device's profile and every lock. The switch names the new node and identity, every other
    /// line kept, and the old identity and the marker go.
    #[test]
    fn the_wipe_deletes_the_members_files_and_keeps_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let file = laid_out(dir.path(), 5);
        let before = std::fs::read_to_string(&file).unwrap();
        let der = answered(&file, 9);
        let done = resume(|| Ok::<_, RejoinError>(configured(&file))).unwrap();
        assert_eq!(done.id, 9);
        assert_eq!(state(&done).unwrap(), Resume::Start);
        for name in MEMBERS {
            assert!(!dir.path().join(name).exists(), "{name} kept");
        }
        for name in KEPT {
            assert_eq!(
                std::fs::read(dir.path().join(name)).unwrap(),
                name.as_bytes(),
                "{name} not kept as it was"
            );
        }
        for name in ["identity/node.der", "identity/node.key", "rejoin"] {
            assert!(!dir.path().join(name).exists(), "{name} kept");
        }
        assert_eq!(one_identity(&file), mantle_cert::fingerprint(&der));
        let after = std::fs::read_to_string(&file).unwrap();
        let expected = before
            .replace("id = 5", "id = 9")
            .replace(
                "certificate = identity/node.der # its own",
                "certificate = identity/next.der",
            )
            .replace("key = identity/node.key", "key = identity/next.key");
        assert_eq!(after, expected);
        // A configuration stating the identity twice is refused, untouched.
        assert!(matches!(
            switched(&format!("{after}key = k\n"), &file, (9, NODE)),
            Err(RejoinError::Stated(_, "key", 2))
        ));
    }

    /// A crash after any act, its configuration or the next certificate's write torn as well,
    /// resumes to the end an uncrashed rejoin reaches, and at every crash point the configuration
    /// names one identity, whole: the old until the switch, the new after it.
    #[test]
    fn every_crash_point_resumes_to_one_identity() {
        let whole = tempfile::tempdir().unwrap();
        let file = laid_out(whole.path(), 5);
        let old = one_identity(&file);
        answered(&file, 9);
        let mut acts = 0usize;
        while act(&configured(&file)).unwrap().is_some() {
            acts += 1;
        }
        let end = files_under(whole.path());
        let new = one_identity(&file);
        assert_ne!(old, new);
        assert_eq!(acts, MEMBERS.len() + 4);
        for crash in 0..=acts {
            let dir = tempfile::tempdir().unwrap();
            let file = laid_out(dir.path(), 5);
            // The test's identities are drawn afresh: the same steps, its own keys.
            let old = one_identity(&file);
            answered(&file, 9);
            let mut switched = false;
            for _ in 0..crash {
                let done = act(&configured(&file)).unwrap().unwrap();
                switched |= matches!(done, Done::Switched(_));
                let named = one_identity(&file);
                assert_eq!(named == old, !switched, "after {done:?}");
            }
            // Torn writes a crash leaves under each temporary name: the configuration's only
            // before its rename, which takes the temporary name away.
            if !switched {
                std::fs::write(dir.path().join("config.new"), b"id = 1\n").unwrap();
            }
            std::fs::write(dir.path().join("identity/next.der.new"), b"torn").unwrap();
            let done = resume(|| Ok::<_, RejoinError>(configured(&file))).unwrap();
            assert_eq!(state(&done).unwrap(), Resume::Start);
            assert_ne!(one_identity(&file), old);
            // The same files as the uncrashed rejoin's, but for each run's own identity and the
            // torn certificate's temporary, which only the next write of it replaces.
            let mut found = files_under(dir.path());
            assert!(found.remove(Path::new("identity/next.der.new")).is_some());
            let names = |files: &BTreeMap<PathBuf, Vec<u8>>| -> Vec<PathBuf> {
                files.keys().cloned().collect()
            };
            assert_eq!(names(&found), names(&end), "crashed after {crash} acts");
        }
    }

    /// A node written off again after a rejoin writes its next identity to the place its first
    /// switch left: the configuration names `next.*`, so the second's goes to `node.*`, and the
    /// first's is deleted once the second switch names it.
    #[test]
    fn a_second_rejoin_takes_the_place_the_first_left() {
        let dir = tempfile::tempdir().unwrap();
        let file = laid_out(dir.path(), 5);
        answered(&file, 9);
        resume(|| Ok::<_, RejoinError>(configured(&file))).unwrap();
        let first = one_identity(&file);
        Layout::new(dir.path()).retire(&first).unwrap();
        assert_eq!(state(&configured(&file)).unwrap(), Resume::Ask(first));
        let second = answered(&file, 11);
        assert!(dir.path().join("identity/node.der").exists());
        let done = resume(|| Ok::<_, RejoinError>(configured(&file))).unwrap();
        assert_eq!(done.id, 11);
        assert_eq!(one_identity(&file), mantle_cert::fingerprint(&second));
        let text = std::fs::read_to_string(&file).unwrap();
        assert!(text.contains("certificate = identity/node.der\n"));
        assert!(text.contains("key = identity/node.key\n"));
        for name in ["identity/next.der", "identity/next.key", "rejoin"] {
            assert!(!dir.path().join(name).exists(), "{name} kept");
        }
    }
}
