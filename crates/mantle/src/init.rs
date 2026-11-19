//! `mantle init` (docs/design/node.md §1.5, §1.6): a cell made at once from the machines it starts
//! with, its founders, each given by its operator's statements alone: where it listens, its
//! failure domain, the directories and bytes of the volumes it keeps and, where stated, the most
//! memory it may hold. They come on the command line, a `--node` each, in a machines file
//! (`--founders`), or both, the file's first; the cell's bounds on its volume table and its
//! durability target likewise (architecture §11 leaves the bounds' values open: they are stated).
//!
//! It draws from the OS the cell's authority's key (`mantle_cert::authority`), its issuer's key,
//! whose certificate the authority signs (`mantle_cert::issuer`, docs/design/membership.md §5.1),
//! the root key's first
//! generation, 32 bytes (encryption.md §2), the founding ranges' groups, one for the root range and
//! one for each of the Bucket, Name, File and Block layers, and each founder's id, 64 bits; issues
//! each founder's certificate under the authority (`mantle_cert::node`); and writes each founder's
//! directory (§1.6): its `config`, the stated keys alone (`mantle_node::process::STATED_KEYS`), its
//! paths relative to the directory so it moves to its host whole; `identity/`, the authority's
//! certificate, the issuer's certificate and key beside it, the founder's own certificate and key,
//! and each other founder's certificate, pinned (§3.6); and `keys/`, the root key. Keys are owner-only (`crate::files`). One founder makes
//! a cell of one in OUT, which keeps the authority's key (§1.5: "on a laptop it is in the node's
//! directory"), and is measured now: this machine's calibration record and each of its devices'
//! profiles, each device under its directory's lock (`mantle_disk::profile`). Several make
//! OUT/authority, the authority's certificate and key and the root key, which the operator keeps,
//! and OUT/node-ID for each founder, which the operator moves to its host, where its first
//! `mantle serve` measures what this machine cannot.
//!
//! For S3 (node.md §1.5, §4) it draws the cell's first account, twelve digits, and its one access
//! key, an id and a secret, from the OS, and writes the key on every founder, owner-only, under
//! `keys/s3/`; it draws the S3 endpoint's own authority, ECDSA P-256 beside the cell's Ed25519, as
//! stock clients verify it (`mantle_cert::endpoint_authority`; research/84), kept with the cell's,
//! and issues each founder's endpoint a certificate for the names and the address its clients dial.
//! For the operator it writes OUT/s3: the key as AWS's shared credentials file, a config naming the
//! endpoint where init can name it, and the endpoint's authority as PEM for that config's
//! `ca_bundle`.
//!
//! Beside the authority's key it keeps `cell`, what a client of the native protocol needs: the
//! authority's name, the account its clients act for, and each founder's id and QUIC address.
//! `mantle init client NAME --cell OUT` ([`client`]) issues a client its certificate under the
//! authority (`mantle_cert::client`: `client-ID`, that account, clientAuth alone) and an owner-only
//! key, and writes the client's directory NAME: `identity/` (the authority's certificate, the
//! client's own and its key) and `config` (its id, those files, and a `node = ID ADDR` line for
//! each founder), which `mantle`'s object commands read through the native door, and `s3/`, its own
//! copy of the cell's S3 profile, which they read through the S3 door (docs/design/doors.md §2). No
//! node's configuration names a client: nodes admit one by its certificate (node.md §3.6).

use std::fmt;
use std::io::Write;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use mantle_cert::{
    EndpointKey, EndpointName, KeyPair, MAX_ENDPOINT_VALIDITY, NO_EXPIRY, Serial, Validity,
    check_dns_name,
};
use mantle_meta::apply::Layer;
use mantle_meta::root::MAX_DOMAIN;
use mantle_node::derive::VolumeBytes;
use mantle_protocol::node_name;

use mantle_node::files;
use mantle_node::process::ISSUER;

/// The file beside the authority's key that says what a client of the native protocol needs: the
/// authority's name, the account its clients act for, and each founder's id and QUIC address.
const CELL: &str = "cell";

/// The cell file's text for the authority named `authority`, `account`, each founder and id, and
/// what a grant states of the cell (docs/design/membership.md §7): its founding ranges' groups, its
/// bounds, its region, its durability target and its access key's id.
fn cell_file<'a>(
    (authority, endpoint_authority): (&str, &str),
    (account, access_key): (&str, &str),
    founders: impl Iterator<Item = (&'a Founder, &'a u64)>,
    (cell, groups): (&Cell, &[u128]),
) -> String {
    let mut text = format!(
        "authority = {authority}\ns3_authority = {endpoint_authority}\naccount = {account}\n"
    );
    // The most bytes any founder's volumes state, each volume at its stated bytes: what a node
    // granted with no volume of its own estimates the cell's rows by (`cell.volume`, `grant`).
    let mut volume_bytes: Option<u64> = None;
    for (founder, id) in founders {
        let quic = founder
            .dialed(founder.quic)
            .unwrap_or_else(|| founder.quic.to_string());
        text.push_str(&format!("node = {id} {quic}\n"));
        if let Some(plane) = founder.dialed(founder.plane) {
            text.push_str(&format!("plane = {id} {plane}\n"));
        }
        let stated = founder
            .volumes
            .iter()
            .try_fold(0u64, |sum, (_, bytes)| match bytes {
                VolumeBytes::Stated(size) => sum.checked_add(*size),
                VolumeBytes::Spare { .. } => None,
            })
            .filter(|bytes| *bytes > 0);
        if let Some(bytes) = stated {
            volume_bytes = Some(volume_bytes.map_or(bytes, |most| most.max(bytes)));
        }
    }
    if let Some(bytes) = volume_bytes {
        text.push_str(&format!("volume_bytes = {bytes}\n"));
    }
    for (group, layer) in groups.iter().zip(LAYERS) {
        text.push_str(&format!("range = {group:x} {}\n", layer_name(layer)));
    }
    if let (Some(nodes), Some(volumes)) = (cell.max_nodes, cell.max_volumes) {
        text.push_str(&format!("max_nodes = {nodes}\nmax_volumes = {volumes}\n"));
    }
    text.push_str(&format!(
        "region = {}\naccess_key = {access_key}\n",
        cell.region()
    ));
    if let Some(target) = cell.target {
        text.push_str(&format!("target = {target}\n"));
    }
    text
}

/// The file beside the authority's key that holds the last grant it issued, in decimal: each grant
/// is numbered past it (docs/design/membership.md §2).
const GRANTS: &str = "grants";

/// The founding ranges' layers, a range of each (node.md §1.5).
const LAYERS: [Layer; 5] = [
    Layer::Root,
    Layer::Bucket,
    Layer::Name,
    Layer::File,
    Layer::Block,
];

#[derive(Debug)]
pub enum Error {
    Output(std::io::Error),
    /// A founder's statements that do not read: which, and why.
    Founder(String, String),
    /// A machines file's line that does not read: the file, the line and why.
    File(PathBuf, usize, String),
    /// The cell as stated holds no node, or misses a statement it needs.
    Cell(String),
    /// OUT holds something already: a cell is made in a directory of its own.
    Exists(PathBuf),
    /// The OS gave no random bytes, or drew an id twice.
    Random(String),
    Cert(String),
    Disk(hyper_block::DiskError),
    Measure(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Output(e) => write!(f, "writing output: {e}"),
            Self::Founder(spec, why) => write!(f, "the founder {spec:?}: {why}"),
            Self::File(path, line, why) => write!(f, "{}, line {line}: {why}", path.display()),
            Self::Cell(why) => write!(f, "the cell: {why}"),
            Self::Exists(path) => write!(
                f,
                "{} holds files already: init makes a cell in a directory of its own",
                path.display()
            ),
            Self::Random(why) => write!(f, "the OS's random numbers: {why}"),
            Self::Cert(why) => write!(f, "a certificate: {why}"),
            Self::Disk(e) => write!(f, "{e}"),
            Self::Measure(why) => write!(f, "measuring: {why}"),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Output(e)
    }
}

impl From<hyper_block::DiskError> for Error {
    fn from(e: hyper_block::DiskError) -> Self {
        Self::Disk(e)
    }
}

/// A founder: what its operator states of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Founder {
    pub quic: SocketAddr,
    pub plane: SocketAddr,
    /// Where its S3 endpoint listens, over TCP, where stated ([`Founder::s3`]).
    pub s3: Option<SocketAddr>,
    /// The DNS names its S3 clients dial it by.
    pub names: Vec<String>,
    pub domain: Option<String>,
    /// Each volume's device directory and bytes.
    pub volumes: Vec<(PathBuf, VolumeBytes)>,
    /// The most memory the node may hold, where stated.
    pub memory: Option<u64>,
    /// The host its peers and native clients dial it at, on its QUIC and plane ports, where it
    /// differs from the address it binds: a Service's name in front of a pod (research/78).
    pub reach: Option<String>,
}

/// The cell as stated: its founders, in order, and the cell's own statements.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Cell {
    pub founders: Vec<Founder>,
    /// The cell's bounds on its volume table's nodes and volumes (`root.max_nodes`,
    /// `root.max_volumes`), needed where a founder keeps volumes.
    pub max_nodes: Option<u32>,
    pub max_volumes: Option<u32>,
    /// A block's annual chance of loss at most, where stated (`durability.target`).
    pub target: Option<f64>,
    /// The cell's repairs an hour, where stated (`durability.repair_rate`; docs/design/repair.md
    /// §2).
    pub repair_rate: Option<f64>,
    /// The region its S3 endpoint serves, where stated ([`Cell::region`]).
    pub region: Option<String>,
}

impl Founder {
    /// Where its S3 endpoint listens: as stated, else at its QUIC address, TCP beside QUIC's UDP on
    /// the one port, as an https URI's port is dialed over either (RFC 9114 §3.1).
    pub fn s3(&self) -> SocketAddr {
        self.s3.unwrap_or(self.quic)
    }

    /// Where its peers and clients dial `bound`, its QUIC or plane address, as `HOST:PORT`: its
    /// reach on that port where stated, else the address itself where it can be dialed (a port and
    /// a specified address); none where the founder learns its address only as it binds, its peers
    /// told then on standard input.
    pub fn dialed(&self, bound: SocketAddr) -> Option<String> {
        match &self.reach {
            Some(host) if host.contains(':') => Some(format!("[{host}]:{}", bound.port())),
            Some(host) => Some(format!("{host}:{}", bound.port())),
            None if bound.port() != 0 && !bound.ip().is_unspecified() => Some(bound.to_string()),
            None => None,
        }
    }
}

impl Cell {
    /// The region its S3 endpoint serves: as stated, else S3's own default, us-east-1, the region
    /// a bucket made with no constraint is in (S3 API Reference, CreateBucket; research/84).
    pub fn region(&self) -> &str {
        self.region.as_deref().unwrap_or(DEFAULT_REGION)
    }
}

/// S3's default region (S3 API Reference, CreateBucket: "If you don't specify a Region, the bucket
/// is created in the US East (N. Virginia) Region (us-east-1) by default").
pub(crate) const DEFAULT_REGION: &str = "us-east-1";

/// A byte count, with a K, M, G or T suffix for a power of 1024.
pub fn parse_bytes(text: &str) -> Result<u64, String> {
    let text = text.trim();
    let (digits, shift) = match text.char_indices().last() {
        Some((at, 'k' | 'K')) => (text.get(..at).unwrap_or_default(), 10),
        Some((at, 'm' | 'M')) => (text.get(..at).unwrap_or_default(), 20),
        Some((at, 'g' | 'G')) => (text.get(..at).unwrap_or_default(), 30),
        Some((at, 't' | 'T')) => (text.get(..at).unwrap_or_default(), 40),
        _ => (text, 0),
    };
    let value: u64 = digits
        .parse()
        .map_err(|_| format!("{text} is not a size in bytes"))?;
    value
        .checked_shl(shift)
        .filter(|bytes| bytes.checked_shr(shift) == Some(value) && *bytes > 0)
        .ok_or_else(|| format!("{text} is not a usable size"))
}

/// A volume stated as its directory and its bytes, `DIR:SIZE`, or as what its file system spares,
/// `DIR:spare` or `DIR:spare:KEEP` with `KEEP` the bytes the machine's other work keeps there
/// (docs/design/membership.md §3.1): the directory before `separator`'s last place in `text` that
/// these leave, so a path with that character in it, a Windows drive's colon among them, still
/// reads.
fn volume(text: &str, separator: char) -> Result<(PathBuf, VolumeBytes), String> {
    let (rest, last) = text
        .trim()
        .rsplit_once(separator)
        .ok_or_else(|| format!("{text:?} is not a volume's directory and bytes"))?;
    let (dir, bytes) = if last.trim() == "spare" {
        (rest, VolumeBytes::Spare { keep: 0 })
    } else {
        let size = parse_bytes(last)?;
        match rest.rsplit_once(separator) {
            Some((dir, word)) if word.trim() == "spare" => (dir, VolumeBytes::Spare { keep: size }),
            _ => (rest, VolumeBytes::Stated(size)),
        }
    };
    let dir = dir.trim();
    if dir.is_empty() {
        return Err(format!("{text:?} names no directory"));
    }
    Ok((PathBuf::from(dir), bytes))
}

/// A `volume` line's bytes as `mantle serve`'s configuration reads them (`process::read_stated`).
fn volume_words(bytes: &VolumeBytes) -> String {
    match bytes {
        VolumeBytes::Stated(size) => size.to_string(),
        VolumeBytes::Spare { keep } => format!("spare {keep}"),
    }
}

/// One founder's statements, each `key` with its `value`, in the order given.
fn founder<'a>(
    pairs: impl IntoIterator<Item = (&'a str, &'a str)>,
    form: char,
) -> Result<Founder, String> {
    let (mut quic, mut plane, mut domain, mut memory) = (None, None, None, None);
    let (mut s3, mut reach) = (None, None);
    let (mut volumes, mut names) = (Vec::new(), Vec::new());
    let once = |slot: &mut Option<String>, key: &str, value: &str| -> Result<(), String> {
        match slot.replace(value.to_owned()) {
            None => Ok(()),
            Some(_) => Err(format!("{key} stated twice")),
        }
    };
    for (key, value) in pairs {
        match key {
            "quic" => once(&mut quic, key, value)?,
            "plane" => once(&mut plane, key, value)?,
            "domain" => once(&mut domain, key, value)?,
            "memory" => once(&mut memory, key, value)?,
            "volume" => volumes.push(volume(value, form)?),
            "s3" => once(&mut s3, key, value)?,
            "reach" => {
                if value.parse::<IpAddr>().is_err() && check_dns_name(value).is_err() {
                    return Err(format!("{value:?} is neither an IP address nor a DNS name"));
                }
                once(&mut reach, key, value)?;
            }
            "name" => {
                check_dns_name(value)
                    .map_err(|_| format!("{value:?} is not a DNS name a certificate may carry"))?;
                names.push(value.to_owned());
            }
            other => {
                return Err(format!(
                    "{other} is not a founder's statement: quic, plane, reach, s3, name, domain, \
                     volume or memory"
                ));
            }
        }
    }
    let address = |slot: Option<String>, key: &str| -> Result<SocketAddr, String> {
        slot.ok_or_else(|| format!("no {key} address"))?
            .parse()
            .map_err(|_| format!("{key} is not an address"))
    };
    if let Some(domain) = &domain
        && (domain.is_empty() || domain.len() > MAX_DOMAIN)
    {
        return Err(format!(
            "a domain of {} bytes, where 1 to {MAX_DOMAIN} are",
            domain.len()
        ));
    }
    if !volumes.is_empty() && domain.is_none() {
        return Err("volumes and no failure domain".into());
    }
    let founder = Founder {
        quic: address(quic, "quic")?,
        plane: address(plane, "plane")?,
        s3: s3.map(|s3| address(Some(s3), "s3")).transpose()?,
        names,
        domain,
        volumes,
        memory: memory.map(|m| parse_bytes(&m)).transpose()?,
        reach,
    };
    // A reach is dialed on the founder's own ports, which a port of 0 leaves unknown until it binds.
    if founder.reach.is_some() && (founder.quic.port() == 0 || founder.plane.port() == 0) {
        return Err("a reach and a port of 0: its peers would dial no port".into());
    }
    // An endpoint at every address has no one address its certificate can name.
    if founder.s3().ip().is_unspecified() && founder.names.is_empty() {
        return Err(
            "an S3 endpoint at every address and no name its clients dial: name=HOST, a DNS name"
                .into(),
        );
    }
    Ok(founder)
}

/// A founder in `--node`'s form: comma-separated `key=value`s, `quic=ADDR`, `plane=ADDR`,
/// `s3=ADDR` (its S3 endpoint's, TCP; its QUIC address unless stated), `name=HOST` (repeatable: a
/// DNS name its S3 clients dial), `domain=NAME`, `volume=DIR:SIZE` (repeatable, the size after the
/// path's last colon; `DIR:spare` or `DIR:spare:KEEP` for what its file system spares), `memory=SIZE` and `reach=HOST` (the host its peers and native clients dial
/// on its QUIC and plane ports, where it differs from the address it binds). A directory with a
/// comma in it is stated in a machines file instead.
pub fn parse_node(spec: &str) -> Result<Founder, Error> {
    let mut pairs = Vec::new();
    for part in spec.split(',') {
        let (key, value) = part
            .split_once('=')
            .ok_or_else(|| Error::Founder(spec.to_owned(), format!("{part:?} is not key=value")))?;
        pairs.push((key.trim(), value.trim()));
    }
    founder(pairs, ':').map_err(|why| Error::Founder(spec.to_owned(), why))
}

/// A machines file: `key = value` lines (`#` begins a comment), the cell's own first (`max_nodes`,
/// `max_volumes`, `durability.target`, `region`), then a block for each founder, opened by a
/// `[node]` line, of the statements `--node` takes, a volume as `volume = DIR SIZE`.
pub fn parse_file(path: &Path, text: &str) -> Result<Cell, Error> {
    let mut cell = Cell::default();
    let mut block: Option<(usize, Vec<(String, String)>)> = None;
    let mut blocks = Vec::new();
    for (at, line) in text.lines().enumerate() {
        let number = at.saturating_add(1);
        let bad = |why: String| Error::File(path.to_path_buf(), number, why);
        let line = line.split('#').next().unwrap_or_default().trim();
        if line.is_empty() {
            continue;
        }
        if line == "[node]" {
            if let Some(done) = block.replace((number, Vec::new())) {
                blocks.push(done);
            }
            continue;
        }
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| bad("not key = value".into()))?;
        let (key, value) = (key.trim(), value.trim());
        if let Some((_, pairs)) = &mut block {
            pairs.push((key.to_owned(), value.to_owned()));
            continue;
        }
        let stated = |slot: bool| -> Result<(), Error> {
            if slot {
                Err(bad(format!("{key} stated twice")))
            } else {
                Ok(())
            }
        };
        match key {
            "max_nodes" => {
                stated(cell.max_nodes.is_some())?;
                cell.max_nodes = Some(value.parse().map_err(|_| bad("a count of nodes".into()))?);
            }
            "max_volumes" => {
                stated(cell.max_volumes.is_some())?;
                cell.max_volumes = Some(
                    value
                        .parse()
                        .map_err(|_| bad("a count of volumes".into()))?,
                );
            }
            "durability.target" => {
                stated(cell.target.is_some())?;
                cell.target = Some(value.parse().map_err(|_| bad("a probability".into()))?);
            }
            "durability.repair_rate" => {
                stated(cell.repair_rate.is_some())?;
                cell.repair_rate = Some(value.parse().map_err(|_| bad("repairs an hour".into()))?);
            }
            "region" => {
                stated(cell.region.is_some())?;
                cell.region = Some(check_region(value).map_err(bad)?);
            }
            other => {
                return Err(bad(format!(
                    "{other} is not the cell's statement: max_nodes, max_volumes, durability.target, \
                     durability.repair_rate or region; a founder's opens with [node]"
                )));
            }
        }
    }
    blocks.extend(block);
    for (number, pairs) in blocks {
        let founder = founder(pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())), ' ')
            .map_err(|why| Error::File(path.to_path_buf(), number, why))?;
        cell.founders.push(founder);
    }
    Ok(cell)
}

/// A region its S3 clients sign for: S3's region codes are lowercase letters, digits and hyphens
/// (S3 API Reference, CreateBucket's LocationConstraint values), and SigV4's credential scope
/// separates its parts with `/` (research/05 §1).
pub fn check_region(region: &str) -> Result<String, String> {
    if region.is_empty()
        || !region
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(format!(
            "{region:?} is not a region: lowercase letters, digits and hyphens"
        ));
    }
    Ok(region.to_owned())
}

fn random<const N: usize>() -> Result<[u8; N], Error> {
    let mut bytes = [0u8; N];
    getrandom::fill(&mut bytes).map_err(|e| Error::Random(e.to_string()))?;
    Ok(bytes)
}

/// `count` values drawn from the OS by `draw`, each nonzero and distinct: one the OS gives twice,
/// or zero, which names no member, is refused rather than drawn again.
fn distinct<T: PartialEq + Default>(
    count: usize,
    mut draw: impl FnMut() -> Result<T, Error>,
) -> Result<Vec<T>, Error> {
    let mut drawn: Vec<T> = Vec::with_capacity(count);
    for _ in 0..count {
        let value = draw()?;
        if value == T::default() || drawn.contains(&value) {
            return Err(Error::Random("an id drawn twice, or zero".into()));
        }
        drawn.push(value);
    }
    Ok(drawn)
}

fn layer_name(layer: Layer) -> &'static str {
    match layer {
        Layer::Root => "root",
        Layer::Bucket => "bucket",
        Layer::Name => "name",
        Layer::File => "file",
        Layer::Block => "block",
    }
}

/// The cell's first access key (research/84): the account it acts for, twelve decimal digits
/// (`mantle_s3::account::valid_id`); its id and its secret, drawn as every access key of the cell
/// is (`mantle_s3::account::draw_id`, `draw_secret`): twenty characters of RFC 4648's base32
/// alphabet, and 30 octets from the OS as 40 characters of base64, each as long as AWS's.
struct AccessKey {
    account: String,
    id: String,
    secret: mantle_s3::account::Secret,
}

/// The accounts' ids: twelve decimal digits.
const ACCOUNTS: u128 = 1_000_000_000_000;

impl AccessKey {
    fn draw() -> Result<Self, Error> {
        // The draws past the largest multiple of 10^12 below 2^128 are refused, about one in 3e27,
        // so the ids taken are uniform.
        let draw = u128::from_le_bytes(random::<16>()?);
        let whole = u128::MAX
            .checked_div(ACCOUNTS)
            .and_then(|n| n.checked_mul(ACCOUNTS));
        let account = match (whole, draw.checked_rem(ACCOUNTS)) {
            (Some(whole), Some(id)) if draw < whole => format!("{id:012}"),
            _ => return Err(Error::Random("an account id past the uniform range".into())),
        };
        let drawn = |e: mantle_s3::account::RandomError| Error::Random(e.to_string());
        let id = mantle_s3::account::draw_id().map_err(drawn)?;
        let secret = mantle_s3::account::draw_secret().map_err(drawn)?;
        Ok(Self {
            account,
            id,
            secret,
        })
    }

    /// The key's file on a founder, `keys/s3/ID`: its account and its secret.
    fn file(&self) -> String {
        format!(
            "account = {}\nsecret = {}\n",
            self.account,
            self.secret.as_str()
        )
    }
}

/// The names a founder's S3 clients dial its endpoint by: each stated; its address where it is
/// one; and `localhost` where that address is a loopback one, a name for it there (RFC 6761 §6.3).
fn endpoint_names(founder: &Founder) -> Vec<EndpointName<'_>> {
    let mut names: Vec<EndpointName<'_>> = founder
        .names
        .iter()
        .map(|name| EndpointName::Dns(name))
        .collect();
    let ip = founder.s3().ip();
    if !ip.is_unspecified() {
        names.push(EndpointName::Address(ip));
        if ip.is_loopback()
            && !founder
                .names
                .iter()
                .any(|name| name.eq_ignore_ascii_case("localhost"))
        {
            names.push(EndpointName::Dns("localhost"));
        }
    }
    names
}

/// The URL a founder's S3 clients reach its endpoint at, where init can name it: none while its
/// port is the one it will bind (0), or it listens at every address with no name stated. A
/// loopback endpoint serves plain HTTP, any other TLS (node.md §4.1).
fn endpoint_url(founder: &Founder) -> Option<String> {
    let at = founder.s3();
    if at.port() == 0 {
        return None;
    }
    let host = match (founder.names.first(), at.ip()) {
        (Some(name), _) => name.clone(),
        (None, ip) if ip.is_unspecified() => return None,
        (None, IpAddr::V4(ip)) => ip.to_string(),
        (None, IpAddr::V6(ip)) => format!("[{ip}]"),
    };
    let scheme = if at.ip().is_loopback() {
        "http"
    } else {
        "https"
    };
    Some(format!("{scheme}://{host}:{}", at.port()))
}

/// The profile stock S3 clients take the cell by (AWS SDKs and Tools Reference Guide, the shared
/// config and credentials files), named `mantle`.
pub(crate) const PROFILE: &str = "mantle";

/// The operator's S3 profile in `dir` (OUT/s3; research/84): the access key as AWS's shared
/// credentials file, owner-only; the endpoint's authority as PEM; and where `url` names the
/// endpoint, the config of the profile: its region, its endpoint (`endpoint_url`), for an https
/// endpoint the PEM as its CA bundle (`ca_bundle`, an absolute path), and path-style addressing,
/// the bucket in the path, which an endpoint named by an address or `localhost` needs
/// (`addressing_style`, AWS CLI's S3 configuration). The config's path where one was written.
fn profile(
    dir: &Path,
    access: &AccessKey,
    (region, url): (&str, Option<&str>),
    authority_pem: &str,
) -> Result<Option<PathBuf>, Error> {
    let pem = dir.join("authority.pem");
    files::write(&pem, authority_pem.as_bytes())?;
    files::write_secret(
        &dir.join("credentials"),
        format!(
            "[{PROFILE}]\naws_access_key_id = {}\naws_secret_access_key = {}\n",
            access.id,
            access.secret.as_str()
        )
        .as_bytes(),
    )?;
    let Some(url) = url else {
        return Ok(None);
    };
    let mut text = format!(
        "# The cell's S3 profile, made by `mantle init`: AWS_CONFIG_FILE names this file,\n\
         # AWS_SHARED_CREDENTIALS_FILE the credentials beside it, AWS_PROFILE={PROFILE}.\n\
         [profile {PROFILE}]\nregion = {region}\nendpoint_url = {url}\n"
    );
    if url.starts_with("https:") {
        let absolute = std::path::absolute(&pem)?;
        let path = absolute
            .to_str()
            .ok_or_else(|| Error::Cell(format!("{} is not UTF-8", absolute.display())))?;
        text.push_str(&format!("ca_bundle = {path}\n"));
    }
    text.push_str("s3 =\n  addressing_style = path\n");
    let config = dir.join("config");
    files::write(&config, text.as_bytes())?;
    Ok(Some(config))
}

/// The config of a founder: its statements and the cell's, paths relative to its directory.
fn config(
    id: u64,
    founder: &Founder,
    cell: &Cell,
    (ids, groups): (&[u64], &[u128]),
    access_key: &str,
) -> String {
    let mut text = format!(
        "# The operator's statements for node {id}, made by `mantle init`: every other value \
         `mantle serve` measures or derives (docs/design/node.md §1.4). Paths are from this \
         file's directory.\nid = {id}\ndir = .\nquic = {}\nplane = {}\n\
         authority = identity/authority.der\ncertificate = identity/node.der\nkey = identity/node.key\n",
        founder.quic, founder.plane
    );
    // The S3 endpoint: where it listens, the region it serves, its certificate and key, and the
    // cell's access key (node.md §4).
    text.push_str(&format!(
        "s3 = {}\ns3.region = {}\ns3.certificate = identity/s3.der\ns3.key = identity/s3.key\n\
         access_key = {access_key} keys/s3/{access_key}\n",
        founder.s3(),
        cell.region()
    ));
    // The names its clients reach it by, which its requests' hosts name a bucket under.
    for name in &founder.names {
        text.push_str(&format!("s3.name = {name}\n"));
    }
    let voters: Vec<String> = ids.iter().map(u64::to_string).collect();
    text.push_str(&format!("voters = {}\n", voters.join(",")));
    for (group, layer) in groups.iter().zip(LAYERS) {
        text.push_str(&format!("range = {group:x} {}\n", layer_name(layer)));
    }
    text.push_str("root_key = 1 keys/1\n");
    for (peer, other) in ids
        .iter()
        .zip(&cell.founders)
        .filter(|(peer, _)| **peer != id)
    {
        // A peer is dialed at its reach where stated. One stated at port 0, or at no address of its
        // own, learns where it listens only as it binds: its peers are told then, on standard
        // input, as of a peer that moved.
        let at = match (other.dialed(other.quic), other.dialed(other.plane)) {
            (Some(quic), Some(plane)) => format!(" {quic} {plane}"),
            _ => String::new(),
        };
        text.push_str(&format!(
            "peer = {peer} node identity/peers/{peer}.der{at}\n"
        ));
    }
    for (dir, bytes) in &founder.volumes {
        text.push_str(&format!(
            "volume = {} {}\n",
            dir.display(),
            volume_words(bytes)
        ));
    }
    if let Some(domain) = &founder.domain {
        text.push_str(&format!("domain = {domain}\n"));
    }
    if let Some(reach) = &founder.reach {
        text.push_str(&format!("reach = {reach}\n"));
    }
    if !founder.volumes.is_empty()
        && let (Some(nodes), Some(volumes)) = (cell.max_nodes, cell.max_volumes)
    {
        text.push_str(&format!(
            "root.max_nodes = {nodes}\nroot.max_volumes = {volumes}\n"
        ));
    }
    if let Some(memory) = founder.memory {
        text.push_str(&format!("memory = {memory}\n"));
    }
    if let Some(target) = cell.target {
        text.push_str(&format!("durability.target = {target}\n"));
    }
    if let Some(rate) = cell.repair_rate {
        text.push_str(&format!("durability.repair_rate = {rate}\n"));
    }
    text
}

/// Makes `cell` in `out`, saying on `said` what it made and measured.
pub fn init(said: &mut impl Write, out: &Path, cell: &Cell) -> Result<(), Error> {
    if cell.founders.is_empty() {
        return Err(Error::Cell(
            "no founder: state each with --node or in a machines file".into(),
        ));
    }
    if cell.founders.iter().any(|f| !f.volumes.is_empty())
        && (cell.max_nodes.is_none() || cell.max_volumes.is_none())
    {
        return Err(Error::Cell(
            "founders keep volumes, and the cell's bounds on its volume table are not stated: \
             --max-nodes and --max-volumes, or max_nodes and max_volumes in the machines file"
                .into(),
        ));
    }
    if cell
        .target
        .is_some_and(|t| !(t.is_finite() && t > 0.0 && t < 1.0))
    {
        return Err(Error::Cell("a durability target between 0 and 1".into()));
    }
    if cell
        .repair_rate
        .is_some_and(|r| !(r.is_finite() && r > 0.0))
    {
        return Err(Error::Cell(
            "a repair rate of more than none an hour".into(),
        ));
    }
    match std::fs::read_dir(out) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                return Err(Error::Exists(out.to_path_buf()));
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(Error::Output(e)),
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::Cert(e.to_string()))?
        .as_secs();
    let validity = Validity {
        not_before: now,
        not_after: NO_EXPIRY,
    };
    let cert = |e: mantle_cert::CertError| Error::Cert(format!("{e:?}"));
    let authority_key = KeyPair::generate().map_err(cert)?;
    let cell_id = u64::from_le_bytes(random::<8>()?);
    let authority_name = format!("mantle cell {cell_id:016x}");
    let authority = mantle_cert::authority(
        &authority_key,
        &authority_name,
        validity,
        Serial::draw().map_err(cert)?,
    )
    .map_err(cert)?;
    let (issuer_key, issuer) = make_issuer(&authority_key, &authority_name, now)?;
    let root_key = random::<32>()?;
    let ids = distinct(cell.founders.len(), || {
        Ok(u64::from_le_bytes(random::<8>()?))
    })?;
    let groups = distinct(LAYERS.len(), || Ok(u128::from_le_bytes(random::<16>()?)))?;
    // Each founder under a grant of its own, numbered from 1 in the founders' order: what the cell's
    // root range spends as the founder joins (docs/design/membership.md §2).
    let mut issued = Vec::with_capacity(ids.len());
    for (&id, grant) in ids.iter().zip(1u64..) {
        let own = KeyPair::generate().map_err(cert)?;
        let certificate = mantle_cert::granted_node(
            &authority_key,
            &authority_name,
            own.public_key(),
            (&node_name(id), grant),
            (validity, Serial::draw().map_err(cert)?),
        )
        .map_err(cert)?;
        issued.push((certificate, own));
    }
    let access = AccessKey::draw()?;
    let endpoint_key = EndpointKey::generate().map_err(cert)?;
    let endpoint_authority_name = format!("mantle s3 {cell_id:016x}");
    let endpoint_authority = mantle_cert::endpoint_authority(
        &endpoint_key,
        &endpoint_authority_name,
        validity,
        Serial::draw().map_err(cert)?,
    )
    .map_err(cert)?;
    // An endpoint's certificate is valid for 825 days at most, as Apple's clients ask
    // (`MAX_ENDPOINT_VALIDITY`); issuing it again is still to build (node.md §1.5).
    let endpoint_validity = Validity {
        not_before: now,
        not_after: now
            .checked_add(MAX_ENDPOINT_VALIDITY)
            .ok_or_else(|| Error::Cert("an endpoint's validity past 9999".into()))?,
    };
    let mut endpoints = Vec::with_capacity(ids.len());
    for founder in &cell.founders {
        let own = EndpointKey::generate().map_err(cert)?;
        let certificate = mantle_cert::endpoint(
            &endpoint_key,
            &endpoint_authority_name,
            own.public_key(),
            &endpoint_names(founder),
            (endpoint_validity, Serial::draw().map_err(cert)?),
        )
        .map_err(cert)?;
        endpoints.push((certificate, own));
    }
    let one = cell.founders.len() == 1;
    let authority_dir = if one {
        out.join("identity")
    } else {
        out.join("authority")
    };
    files::create_dir(&authority_dir)?;
    files::write(&authority_dir.join("authority.der"), &authority)?;
    files::write_secret(&authority_dir.join("authority.key"), authority_key.pkcs8())?;
    keep_issuer(&authority_dir, &issuer, &issuer_key)?;
    files::write(&authority_dir.join("s3-authority.der"), &endpoint_authority)?;
    files::write_secret(
        &authority_dir.join("s3-authority.key"),
        endpoint_key.pkcs8(),
    )?;
    if !one {
        files::create_dir(&authority_dir.join("keys"))?;
        files::write_secret(&authority_dir.join("keys").join("1"), &root_key)?;
    }
    files::write(
        &authority_dir.join(CELL),
        cell_file(
            (&authority_name, &endpoint_authority_name),
            (&access.account, &access.id),
            cell.founders.iter().zip(&ids),
            (cell, &groups),
        )
        .as_bytes(),
    )?;
    files::write(
        &authority_dir.join(GRANTS),
        ids.len().to_string().as_bytes(),
    )?;
    writeln!(said, "cell {authority_name}")?;
    writeln!(
        said,
        "authority {}: its keys sign every member's certificate and every S3 endpoint's; keep \
         them",
        authority_dir.display()
    )?;
    let mut first = None;
    for (at, (founder, &id)) in cell.founders.iter().zip(&ids).enumerate() {
        let dir = if one {
            out.to_path_buf()
        } else {
            out.join(format!("node-{id}"))
        };
        let identity = dir.join("identity");
        files::create_dir(&identity.join("peers"))?;
        files::create_dir(&dir.join("keys"))?;
        let (certificate, own) = issued
            .get(at)
            .ok_or_else(|| Error::Cell("a founder's certificate".into()))?;
        if !one {
            files::write(&identity.join("authority.der"), &authority)?;
            keep_issuer(&identity, &issuer, &issuer_key)?;
        }
        files::write(&identity.join("node.der"), certificate)?;
        files::write_secret(&identity.join("node.key"), own.pkcs8())?;
        let (endpoint, endpoint_own) = endpoints
            .get(at)
            .ok_or_else(|| Error::Cell("a founder's endpoint certificate".into()))?;
        files::write(&identity.join("s3.der"), endpoint)?;
        files::write_secret(&identity.join("s3.key"), endpoint_own.pkcs8())?;
        files::create_dir(&dir.join("keys").join("s3"))?;
        files::write_secret(
            &dir.join("keys").join("s3").join(&access.id),
            access.file().as_bytes(),
        )?;
        for (peer, (peer_certificate, _)) in ids.iter().zip(&issued) {
            if *peer != id {
                files::write(
                    &identity.join("peers").join(format!("{peer}.der")),
                    peer_certificate,
                )?;
            }
        }
        files::write_secret(&dir.join("keys").join("1"), &root_key)?;
        files::write(
            &dir.join("config"),
            config(id, founder, cell, (&ids, &groups), &access.id).as_bytes(),
        )?;
        writeln!(
            said,
            "node {id} {}: quic {} plane {} s3 {}",
            dir.display(),
            founder.quic,
            founder.plane,
            founder.s3()
        )?;
        first.get_or_insert((founder, dir));
    }
    let s3 = out.join("s3");
    files::create_dir(&s3)?;
    let url = cell.founders.iter().find_map(endpoint_url);
    let profile = profile(
        &s3,
        &access,
        (cell.region(), url.as_deref()),
        &mantle_cert::pem(&endpoint_authority),
    )?;
    writeln!(
        said,
        "s3 account {} access key {}, region {}",
        access.account,
        access.id,
        cell.region()
    )?;
    match profile {
        Some(config) => writeln!(
            said,
            "s3 profile {PROFILE}: AWS_CONFIG_FILE={} AWS_SHARED_CREDENTIALS_FILE={} \
             AWS_PROFILE={PROFILE}",
            config.display(),
            s3.join("credentials").display()
        )?,
        None => writeln!(
            said,
            "s3 profile {PROFILE}: credentials {} and the endpoint's authority {}; no founder's \
             endpoint has a port and a name init can state, so no config names one",
            s3.join("credentials").display(),
            s3.join("authority.pem").display()
        )?,
    }
    match first {
        Some((founder, dir)) if one => measure(said, founder, &dir),
        _ => {
            writeln!(
                said,
                "each founder measures its host and its devices at its first `mantle serve`"
            )?;
            Ok(())
        }
    }
}

/// A client of the native protocol for the cell `mantle init` made in `cell`, its directory made at
/// `out` (node.md §1.5, §3.6): its id drawn from the OS, nonzero and no founder's; its certificate
/// issued under the cell's authority, `client-ID` acting for the cell's account, a connection's
/// client end alone (`mantle_cert::client`); its key, owner-only; the authority's certificate,
/// which its nodes' chain to; and its `config`, naming each founder where it listens.
pub fn client(said: &mut impl Write, out: &Path, cell: &Path) -> Result<(), Error> {
    let Authority {
        name: authority_name,
        account,
        nodes,
        certificate: authority,
        key: authority_key,
        ..
    } = Authority::read(cell)?;
    match std::fs::read_dir(out) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                return Err(Error::Exists(out.to_path_buf()));
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(Error::Output(e)),
    }
    let cert = |e: mantle_cert::CertError| Error::Cert(format!("{e:?}"));
    let id = loop {
        let id = u64::from_le_bytes(random::<8>()?);
        if id != 0 && !nodes.iter().any(|(node, _)| *node == id) {
            break id;
        }
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::Cert(e.to_string()))?
        .as_secs();
    let own = KeyPair::generate().map_err(cert)?;
    let certificate = mantle_cert::client(
        &authority_key,
        &authority_name,
        own.public_key(),
        (&mantle_protocol::client_name(id), &account),
        (
            Validity {
                not_before: now,
                not_after: NO_EXPIRY,
            },
            Serial::draw().map_err(cert)?,
        ),
    )
    .map_err(cert)?;
    // Issued under the authority whose certificate the client keeps: the check a node's start
    // makes of its own (RFC 5280 §6.1.3 (a)).
    mantle_cert::issued_by(&certificate, &authority, now).map_err(cert)?;
    let identity = out.join("identity");
    files::create_dir(&identity)?;
    files::write(&identity.join("authority.der"), &authority)?;
    files::write(&identity.join("client.der"), &certificate)?;
    files::write_secret(&identity.join("client.key"), own.pkcs8())?;
    let mut config = format!(
        "id = {id}\nauthority = identity/authority.der\ncertificate = identity/client.der\n\
         key = identity/client.key\n"
    );
    for (node, quic) in &nodes {
        config.push_str(&format!("node = {node} {quic}\n"));
    }
    files::write(&out.join("config"), config.as_bytes())?;
    // The cell's S3 profile, the client's own copy: the key, region and endpoint its S3 door signs
    // for and dials, and the endpoint's authority (`--door s3`, src/door.rs), a profile stock S3
    // clients take as well.
    let given =
        crate::profile::Profile::read(&cell.join("s3")).map_err(|e| Error::Cell(e.to_string()))?;
    let s3 = out.join("s3");
    files::create_dir(&s3)?;
    profile(
        &s3,
        &AccessKey {
            account: account.clone(),
            id: given.id,
            secret: mantle_s3::account::Secret::new(given.secret),
        },
        (&given.region, given.endpoint.as_deref()),
        &mantle_cert::pem(&given.authority),
    )?;
    writeln!(
        said,
        "client {id} {}: account {account}, {} nodes",
        out.display(),
        nodes.len()
    )?;
    Ok(())
}

/// The authority of the cell `mantle init` made in `cell`, as its cell file and key say it: what an
/// identity issued after init is issued under.
pub(crate) struct Authority {
    pub(crate) name: String,
    /// The account the cell's native clients act for.
    pub(crate) account: String,
    /// Each founder's and granted node's id and the QUIC address its clients dial, `HOST:PORT`.
    pub(crate) nodes: Vec<(u64, String)>,
    /// Each node's plane address, where init or its grant could state it.
    pub(crate) planes: Vec<(u64, String)>,
    /// The cell's founding ranges, each its group and layer as a `range` line names them.
    pub(crate) ranges: Vec<(u128, String)>,
    /// The cell's bounds on its nodes and volumes, its region, its durability target and its
    /// access key's id, where its cell file states them (docs/design/membership.md §7).
    pub(crate) bounds: Option<(u32, u32)>,
    pub(crate) region: Option<String>,
    pub(crate) target: Option<f64>,
    pub(crate) access_key: Option<String>,
    /// Each gateway `mantle init gateway` pinned, by its id.
    pub(crate) gateways: Vec<u64>,
    /// The S3 endpoint's authority's name, which its endpoints' certificates name as their issuer.
    pub(crate) endpoint_authority: Option<String>,
    /// The most bytes any founder's volumes state, where the cell file states it: what a node
    /// granted with no volume of its own estimates the cell's rows by (`cell.volume`).
    pub(crate) volume_bytes: Option<u64>,
    /// The directory the authority's key is in.
    pub(crate) dir: PathBuf,
    pub(crate) certificate: Vec<u8>,
    pub(crate) key: KeyPair,
}

impl Authority {
    pub(crate) fn read(cell: &Path) -> Result<Self, Error> {
        let authority_dir = [cell.join("authority"), cell.join("identity")]
            .into_iter()
            .find(|dir| dir.join(CELL).is_file())
            .ok_or_else(|| {
                Error::Cell(format!(
                    "{}: no cell file beside an authority's key, as `mantle init` writes one in \
                     the directory it makes a cell in",
                    cell.display()
                ))
            })?;
        let text = std::fs::read_to_string(authority_dir.join(CELL))?;
        let mut name = None;
        let mut account = None;
        let mut nodes: Vec<(u64, String)> = Vec::new();
        let mut planes: Vec<(u64, String)> = Vec::new();
        let mut ranges: Vec<(u128, String)> = Vec::new();
        let (mut max_nodes, mut max_volumes) = (None, None);
        let (mut region, mut target, mut access_key) = (None, None, None);
        let mut gateways = Vec::new();
        let mut endpoint_authority = None;
        let mut volume_bytes = None;
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let bad = || Error::Cell(format!("the cell file's line does not read: {line}"));
            let (key, value) = line.split_once('=').ok_or_else(bad)?;
            let value = value.trim();
            let at = |value: &str| -> Result<(u64, String), Error> {
                let (id, place) = value.split_once(' ').ok_or_else(bad)?;
                let id = id.parse().map_err(|_| bad())?;
                let place = mantle_node::process::Endpoint::parse(place.trim())
                    .ok_or_else(bad)?
                    .to_string();
                Ok((id, place))
            };
            match key.trim() {
                "authority" => name = Some(value.to_owned()),
                "account" => account = Some(value.to_owned()),
                "node" => nodes.push(at(value)?),
                "plane" => planes.push(at(value)?),
                "range" => {
                    let (group, layer) = value.split_once(' ').ok_or_else(bad)?;
                    let group = u128::from_str_radix(group, 16).map_err(|_| bad())?;
                    ranges.push((group, layer.trim().to_owned()));
                }
                "max_nodes" => max_nodes = Some(value.parse().map_err(|_| bad())?),
                "max_volumes" => max_volumes = Some(value.parse().map_err(|_| bad())?),
                "region" => region = Some(value.to_owned()),
                "target" => target = Some(value.parse().map_err(|_| bad())?),
                "access_key" => access_key = Some(value.to_owned()),
                "gateway" => gateways.push(value.parse().map_err(|_| bad())?),
                "s3_authority" => endpoint_authority = Some(value.to_owned()),
                "volume_bytes" => volume_bytes = Some(value.parse().map_err(|_| bad())?),
                _ => return Err(bad()),
            }
        }
        let (Some(name), Some(account)) = (name, account) else {
            return Err(Error::Cell(
                "the cell file names no authority or no account".into(),
            ));
        };
        if nodes.is_empty() {
            return Err(Error::Cell("the cell file names no node".into()));
        }
        let cert = |e: mantle_cert::CertError| Error::Cert(format!("{e:?}"));
        let certificate = std::fs::read(authority_dir.join("authority.der"))?;
        let key = mantle_node::secret::read(&authority_dir.join("authority.key"))
            .map_err(|e| Error::Cert(e.to_string()))
            .and_then(|pkcs8| KeyPair::from_pkcs8(&pkcs8).map_err(cert))?;
        Ok(Self {
            name,
            account,
            nodes,
            planes,
            ranges,
            bounds: max_nodes.zip(max_volumes),
            region,
            target,
            access_key,
            gateways,
            endpoint_authority,
            volume_bytes,
            dir: authority_dir,
            certificate,
            key,
        })
    }
}

impl Authority {
    /// The cell's issuer kept beside the authority's key, its certificate and key, where the cell
    /// has one (docs/design/membership.md §5.1).
    fn issuer(&self) -> Result<Option<(Vec<u8>, KeyPair)>, Error> {
        let (certificate, key) = (self.dir.join(ISSUER.0), self.dir.join(ISSUER.1));
        if !certificate.try_exists()? {
            return Ok(None);
        }
        let der = std::fs::read(&certificate)?;
        let pair = mantle_node::secret::read(&key)
            .map_err(|e| Error::Cert(e.to_string()))
            .and_then(|pkcs8| {
                KeyPair::from_pkcs8(&pkcs8).map_err(|e| Error::Cert(format!("{e:?}")))
            })?;
        Ok(Some((der, pair)))
    }
}

/// The cell's issuer under the authority holding `authority_key` and named `authority_name`
/// (docs/design/membership.md §5.1): its key, drawn from the OS, and its certificate, named after
/// the authority, a CA of node certificates alone (`mantle_cert::issuer`), checked as a node's
/// start checks it.
fn make_issuer(
    authority_key: &KeyPair,
    authority_name: &str,
    now: u64,
) -> Result<(KeyPair, Vec<u8>), Error> {
    let cert = |e: mantle_cert::CertError| Error::Cert(format!("{e:?}"));
    let key = KeyPair::generate().map_err(cert)?;
    let certificate = mantle_cert::issuer(
        authority_key,
        authority_name,
        key.public_key(),
        &format!("{authority_name} issuer"),
        (
            Validity {
                not_before: now,
                not_after: NO_EXPIRY,
            },
            Serial::draw().map_err(cert)?,
        ),
    )
    .map_err(cert)?;
    Ok((key, certificate))
}

/// The issuer's certificate and key kept in `dir`, beside an authority's certificate, where a
/// node's start finds them (`mantle_node::process::ISSUER`): the key owner-only, each written
/// whole and durable (CLAUDE.md §6).
fn keep_issuer(dir: &Path, certificate: &[u8], key: &KeyPair) -> Result<(), Error> {
    files::write(&dir.join(ISSUER.0), certificate)?;
    files::write_secret(&dir.join(ISSUER.1), key.pkcs8())?;
    Ok(())
}

/// The issuer of a cell `mantle init` made in `cell` before cells had one (docs/design/membership.md
/// §5.1), made by its authority's holder, offline, once: kept beside the authority's key, and in
/// each node's directory `mantle init` made in `cell`, beside the authority's certificate there;
/// a node whose directory is elsewhere takes the two files from beside the authority's key. Refused
/// where the cell has an issuer, so the authority signs one.
pub fn issuer(said: &mut impl Write, cell: &Path) -> Result<(), Error> {
    let authority = Authority::read(cell)?;
    let at = authority.dir.join(ISSUER.0);
    if at.try_exists()? {
        return Err(Error::Cell(format!(
            "{}: the cell has its issuer already, which its authority signs once",
            at.display()
        )));
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::Cert(e.to_string()))?
        .as_secs();
    let (key, certificate) = make_issuer(&authority.key, &authority.name, now)?;
    mantle_cert::issuer_issued(&certificate, &authority.certificate, now)
        .map_err(|e| Error::Cert(format!("{e:?}")))?;
    keep_issuer(&authority.dir, &certificate, &key)?;
    writeln!(
        said,
        "issuer {}: it signs the certificates of the nodes the cell re-grants; every member keeps          it",
        at.display()
    )?;
    for (id, _) in &authority.nodes {
        let identity = cell.join(format!("node-{id}")).join("identity");
        if identity.join("authority.der").is_file() {
            keep_issuer(&identity, &certificate, &key)?;
            writeln!(said, "issuer kept by node {id} {}", identity.display())?;
        }
    }
    Ok(())
}

/// A gateway's identity for the cell `mantle init` made in `cell`, which its operator pins in each
/// founder's configuration (`peer = ID gateway CERTIFICATE`, node.md §3.6): its id drawn from the
/// OS, nonzero and no founder's, its certificate issued under the cell's authority, and its key.
pub(crate) fn gateway(cell: &Path) -> Result<(u64, Vec<u8>, KeyPair), Error> {
    let authority = Authority::read(cell)?;
    let cert = |e: mantle_cert::CertError| Error::Cert(format!("{e:?}"));
    let id = loop {
        let id = u64::from_le_bytes(random::<8>()?);
        if id != 0 && !authority.nodes.iter().any(|(node, _)| *node == id) {
            break id;
        }
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::Cert(e.to_string()))?
        .as_secs();
    let own = KeyPair::generate().map_err(cert)?;
    let certificate = mantle_cert::node(
        &authority.key,
        &authority.name,
        own.public_key(),
        &format!("gateway-{id}"),
        (
            Validity {
                not_before: now,
                not_after: NO_EXPIRY,
            },
            Serial::draw().map_err(cert)?,
        ),
    )
    .map_err(cert)?;
    mantle_cert::issued_by(&certificate, &authority.certificate, now).map_err(cert)?;
    Ok((id, certificate, own))
}

/// The directory of founder `id` of the cell `mantle init` made in `cell`, which holds `founders`
/// founders: the cell's own directory for a cell of one, else `node-ID` in it.
pub(crate) fn founder_dir(cell: &Path, id: u64, founders: usize) -> PathBuf {
    if founders == 1 {
        cell.to_path_buf()
    } else {
        cell.join(format!("node-{id}"))
    }
}

/// A gateway's identity made in `out`, an empty or missing directory, for the cell `mantle init`
/// made in `cell`, and pinned in each founder's configuration there (`peer = ID gateway
/// CERTIFICATE`, node.md §3.6), its certificate beside the founder's peers', so a founder carried
/// whole to its host (`mantle pack`) carries the pin: what a reader of the cell's metadata, as the
/// transfer harness is, connects as. Made before any founder starts, since a founder reads its
/// peers once, at its start.
pub fn pin_gateway(said: &mut impl Write, out: &Path, cell: &Path) -> Result<u64, Error> {
    match std::fs::read_dir(out) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                return Err(Error::Exists(out.to_path_buf()));
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(Error::Output(e)),
    }
    let authority = Authority::read(cell)?;
    let founders = authority.nodes;
    let (id, certificate, own) = gateway(cell)?;
    let identity = out.join("identity");
    files::create_dir(&identity)?;
    files::write(&identity.join("gateway.der"), &certificate)?;
    files::write_secret(&identity.join("gateway.key"), own.pkcs8())?;
    files::write(&out.join("config"), format!("id = {id}\n").as_bytes())?;
    // Kept beside the authority and named in its cell file, so every node granted later pins it as
    // the founders do (docs/design/membership.md §7).
    files::create_dir(&authority.dir.join("gateways"))?;
    files::write(
        &authority
            .dir
            .join("gateways")
            .join(format!("gateway-{id}.der")),
        &certificate,
    )?;
    let cell_text = std::fs::read_to_string(authority.dir.join(CELL))?;
    files::write(
        &authority.dir.join(CELL),
        format!("{cell_text}gateway = {id}\n").as_bytes(),
    )?;
    let pinned = format!("identity/peers/gateway-{id}.der");
    for (founder, _) in &founders {
        let dir = founder_dir(cell, *founder, founders.len());
        files::write(&dir.join(&pinned), &certificate)?;
        let config = dir.join("config");
        let mut text = std::fs::read_to_string(&config)?;
        text.push_str(&format!("peer = {id} gateway {pinned}\n"));
        files::write(&config, text.as_bytes())?;
    }
    writeln!(
        said,
        "gateway {id} {}: pinned by {} founders",
        out.display(),
        founders.len()
    )?;
    Ok(id)
}

/// A new node's grant (docs/design/membership.md §2, §7): for the cell `mantle init` made in `cell`,
/// a node the operator states as a founder is stated (`founder`), its directory made in `out`, an
/// empty or missing directory, to move to its host whole. Its grant is numbered past the
/// authority's last, which is kept first, so no two grants share a number whatever ends the grant;
/// its id drawn from the OS, nonzero and no node's the cell file names; its certificate issued
/// under the authority naming its grant (`mantle_cert::granted_node`), and its S3 endpoint's under
/// the endpoint's authority; the cell's root key and access key; and its configuration of stated
/// keys: the cell's founding ranges, every node the cell file names as its voters, which it is no
/// member of until the cell adds it, and as its seeds, each gateway the authority pinned, and its
/// own addresses, domain, volumes and the cell's bounds. The cell file names the new node too, for
/// grants and clients after it. Nothing is sent to the cell: the node joins at its first `mantle
/// serve`. Its id and grant.
pub fn grant(
    said: &mut impl Write,
    out: &Path,
    cell: &Path,
    founder: &Founder,
) -> Result<(u64, u64), Error> {
    match std::fs::read_dir(out) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                return Err(Error::Exists(out.to_path_buf()));
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(Error::Output(e)),
    }
    let authority = Authority::read(cell)?;
    let (max_nodes, max_volumes) = authority.bounds.ok_or_else(|| {
        Error::Cell("the cell file states no bounds on the cell's nodes: a cell whose members come and go states them at init".into())
    })?;
    if founder.domain.is_none() {
        return Err(Error::Founder(
            "--node".into(),
            "no failure domain: a member names one".into(),
        ));
    }
    let access_id = authority
        .access_key
        .clone()
        .ok_or_else(|| Error::Cell("the cell file names no access key".into()))?;
    let cert = |e: mantle_cert::CertError| Error::Cert(format!("{e:?}"));
    // The grant's number, kept before anything is issued under it.
    let grants_file = authority.dir.join(GRANTS);
    let last: u64 = std::fs::read_to_string(&grants_file)?
        .trim()
        .parse()
        .map_err(|_| Error::Cell(format!("{}: not a grant's number", grants_file.display())))?;
    let number = last
        .checked_add(1)
        .ok_or_else(|| Error::Cell("the authority's grants are spent".into()))?;
    files::write(&grants_file, number.to_string().as_bytes())?;
    let id = loop {
        let id = u64::from_le_bytes(random::<8>()?);
        if id != 0 && !authority.nodes.iter().any(|(node, _)| *node == id) {
            break id;
        }
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::Cert(e.to_string()))?
        .as_secs();
    let own = KeyPair::generate().map_err(cert)?;
    let certificate = mantle_cert::granted_node(
        &authority.key,
        &authority.name,
        own.public_key(),
        (&node_name(id), number),
        (
            Validity {
                not_before: now,
                not_after: NO_EXPIRY,
            },
            Serial::draw().map_err(cert)?,
        ),
    )
    .map_err(cert)?;
    mantle_cert::issued_by(&certificate, &authority.certificate, now).map_err(cert)?;
    // The S3 endpoint's certificate, under the endpoint's own authority, as init issues a
    // founder's.
    let endpoint_key = mantle_node::secret::read(&authority.dir.join("s3-authority.key"))
        .map_err(|e| Error::Cert(e.to_string()))
        .and_then(|pkcs8| EndpointKey::from_pkcs8(&pkcs8).map_err(cert))?;
    let endpoint_name = authority
        .endpoint_authority
        .clone()
        .ok_or_else(|| Error::Cell("the cell file names no S3 endpoint authority".into()))?;
    let endpoint_own = EndpointKey::generate().map_err(cert)?;
    let endpoint = mantle_cert::endpoint(
        &endpoint_key,
        &endpoint_name,
        endpoint_own.public_key(),
        &endpoint_names(founder),
        (
            Validity {
                not_before: now,
                not_after: now
                    .checked_add(MAX_ENDPOINT_VALIDITY)
                    .ok_or_else(|| Error::Cert("an endpoint's validity past 9999".into()))?,
            },
            Serial::draw().map_err(cert)?,
        ),
    )
    .map_err(cert)?;
    // The cell's keys: the root key's first generation and the access key, as every founder holds
    // them.
    let root_key = [
        authority.dir.join("keys").join("1"),
        cell.join("keys").join("1"),
    ]
    .into_iter()
    .find(|path| path.is_file())
    .ok_or_else(|| Error::Cell("no root key beside the authority".into()))?;
    let root_key = mantle_node::secret::read(&root_key).map_err(|e| Error::Cert(e.to_string()))?;
    let credentials = std::fs::read_to_string(cell.join("s3").join("credentials"))?;
    let secret = credentials
        .lines()
        .find_map(|line| line.strip_prefix("aws_secret_access_key = "))
        .ok_or_else(|| Error::Cell("the cell's S3 credentials hold no secret".into()))?;
    let identity = out.join("identity");
    files::create_dir(&identity.join("peers"))?;
    files::create_dir(&out.join("keys").join("s3"))?;
    files::write(&identity.join("authority.der"), &authority.certificate)?;
    if let Some((issuer, issuer_key)) = authority.issuer()? {
        keep_issuer(&identity, &issuer, &issuer_key)?;
    }
    files::write(&identity.join("node.der"), &certificate)?;
    files::write_secret(&identity.join("node.key"), own.pkcs8())?;
    files::write(&identity.join("s3.der"), &endpoint)?;
    files::write_secret(&identity.join("s3.key"), endpoint_own.pkcs8())?;
    files::write_secret(&out.join("keys").join("1"), &root_key)?;
    files::write_secret(
        &out.join("keys").join("s3").join(&access_id),
        format!("account = {}\nsecret = {secret}\n", authority.account).as_bytes(),
    )?;
    let mut text = format!(
        "# The operator's statements for node {id}, made by `mantle grant` under grant {number}: \
         every other value `mantle serve` measures or derives (docs/design/node.md §1.4). It joins \
         the cell at its first start (docs/design/membership.md §3). Paths are from this file's \
         directory.\nid = {id}\ndir = .\nquic = {}\nplane = {}\n\
         authority = identity/authority.der\ncertificate = identity/node.der\nkey = identity/node.key\n",
        founder.quic, founder.plane
    );
    text.push_str(&format!(
        "s3 = {}\ns3.region = {}\ns3.certificate = identity/s3.der\ns3.key = identity/s3.key\n\
         access_key = {access_id} keys/s3/{access_id}\n",
        founder.s3(),
        authority.region.as_deref().unwrap_or(DEFAULT_REGION)
    ));
    for name in &founder.names {
        text.push_str(&format!("s3.name = {name}\n"));
    }
    // Its voters are the nodes the cell file names, a configuration it is no member of: it takes
    // the cell's own from whichever node leads, once the cell adds it.
    let voters: Vec<String> = authority.nodes.iter().map(|(n, _)| n.to_string()).collect();
    text.push_str(&format!("voters = {}\n", voters.join(",")));
    for (group, layer) in &authority.ranges {
        text.push_str(&format!("range = {group:x} {layer}\n"));
    }
    text.push_str("root_key = 1 keys/1\n");
    for (node, quic) in &authority.nodes {
        let plane = authority
            .planes
            .iter()
            .find(|(p, _)| p == node)
            .map(|(_, plane)| plane.as_str());
        let dialed =
            |at: &str| mantle_node::process::Endpoint::parse(at).is_some_and(|e| e.port != 0);
        match plane {
            Some(plane) if dialed(quic) && dialed(plane) => {
                text.push_str(&format!("seed = {node} {quic} {plane}\n"));
            }
            _ => text.push_str(&format!("seed = {node}\n")),
        }
    }
    for gateway in &authority.gateways {
        let pinned = format!("identity/peers/gateway-{gateway}.der");
        let der = std::fs::read(
            authority
                .dir
                .join("gateways")
                .join(format!("gateway-{gateway}.der")),
        )?;
        files::write(&out.join(&pinned), &der)?;
        text.push_str(&format!("peer = {gateway} gateway {pinned}\n"));
    }
    for (dir, bytes) in &founder.volumes {
        text.push_str(&format!(
            "volume = {} {}\n",
            dir.display(),
            volume_words(bytes)
        ));
    }
    if let Some(domain) = &founder.domain {
        text.push_str(&format!("domain = {domain}\n"));
    }
    if let Some(reach) = &founder.reach {
        text.push_str(&format!("reach = {reach}\n"));
    }
    // A node with no volume of its own holds the cell's rows all the same: its engines estimate
    // them as each node's volumes holding what the founders' do (docs/design/node.md §1.4).
    if founder.volumes.is_empty()
        && let Some(bytes) = authority.volume_bytes
    {
        text.push_str(&format!("cell.volume = {bytes}\n"));
    }
    text.push_str(&format!(
        "root.max_nodes = {max_nodes}\nroot.max_volumes = {max_volumes}\n"
    ));
    if let Some(memory) = founder.memory {
        text.push_str(&format!("memory = {memory}\n"));
    }
    if let Some(target) = authority.target {
        text.push_str(&format!("durability.target = {target}\n"));
    }
    files::write(&out.join("config"), text.as_bytes())?;
    // The cell file names it, for the grants and the clients after it.
    let mut cell_text = std::fs::read_to_string(authority.dir.join(CELL))?;
    let quic = founder
        .dialed(founder.quic)
        .unwrap_or_else(|| founder.quic.to_string());
    cell_text.push_str(&format!("node = {id} {quic}\n"));
    if let Some(plane) = founder.dialed(founder.plane) {
        cell_text.push_str(&format!("plane = {id} {plane}\n"));
    }
    files::write(&authority.dir.join(CELL), cell_text.as_bytes())?;
    writeln!(
        said,
        "granted {id} {number} {}: quic {} plane {} s3 {}",
        out.display(),
        founder.quic,
        founder.plane,
        founder.s3()
    )?;
    Ok((id, number))
}

/// A cell of one's host and devices measured now: the machine's calibration record in its
/// directory (`mantle_node::process::calibration`), and each volume's device profile, under its
/// directory's lock (`mantle_node::storage::Locks`), kept in its `mantle/profile`.
fn measure(said: &mut impl Write, founder: &Founder, dir: &Path) -> Result<(), Error> {
    let budget = mantle_node::derive::probe_budget().map_err(|e| Error::Measure(e.to_string()))?;
    mantle_node::process::calibration(dir, budget).map_err(|e| Error::Measure(e.to_string()))?;
    writeln!(
        said,
        "measured this machine: {}",
        dir.join("calibration").display()
    )?;
    let locks = mantle_node::storage::Locks::take(founder.volumes.iter().map(|(d, _)| d.as_path()))
        .map_err(|e| Error::Measure(e.to_string()))?;
    for (volume, _) in &founder.volumes {
        let identity = mantle_disk::probe::identify(volume);
        let plan = mantle_disk::calibrate::Plan::standard(
            identity.transfer_alignment(),
            identity.queue_depth,
        );
        let mantle = mantle_node::storage::mantle_dir(volume);
        let (profile, _) =
            mantle_disk::profile::load_or_measure(volume, &mantle, &identity, &plan)?;
        writeln!(
            said,
            "measured {}: {} reads at once, {} bytes of reads in flight, {} bytes read past, written once at layout: {}",
            volume.display(),
            profile.depth,
            profile.read_bytes,
            profile.read_gap,
            if profile.prewrite { "yes" } else { "no" }
        )?;
    }
    drop(locks);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use mantle_node::process::Endpoint;

    fn address(text: &str) -> SocketAddr {
        text.parse().unwrap()
    }

    #[test]
    fn sizes_are_bytes_or_a_power_of_1024() {
        assert_eq!(parse_bytes("4096"), Ok(4096));
        assert_eq!(parse_bytes("256M"), Ok(256 << 20));
        assert_eq!(parse_bytes("2g"), Ok(2 << 30));
        assert_eq!(parse_bytes("4T"), Ok(4 << 40));
        assert_eq!(parse_bytes("8k"), Ok(8 << 10));
        for bad in ["0", "", "x", "4X", "-1", "16777216T"] {
            assert!(parse_bytes(bad).is_err(), "{bad}");
        }
    }

    /// A `--node` names a founder's addresses, its domain, each volume, a Windows drive's among
    /// them, and its memory; every malformed one is refused naming it.
    #[test]
    fn a_node_flag_states_a_founder() {
        let founder = parse_node(
            "quic=10.0.0.1:7000, plane=10.0.0.1:7001,domain=rack-a,volume=/data:4T,volume=C:\\d2:1G,\
             volume=/spare:spare,volume=C:\\d3:spare:2G,memory=16G",
        )
        .unwrap();
        assert_eq!(
            founder,
            Founder {
                quic: address("10.0.0.1:7000"),
                plane: address("10.0.0.1:7001"),
                s3: None,
                names: Vec::new(),
                domain: Some("rack-a".into()),
                volumes: vec![
                    (PathBuf::from("/data"), VolumeBytes::Stated(4 << 40)),
                    (PathBuf::from("C:\\d2"), VolumeBytes::Stated(1 << 30)),
                    (PathBuf::from("/spare"), VolumeBytes::Spare { keep: 0 }),
                    (
                        PathBuf::from("C:\\d3"),
                        VolumeBytes::Spare { keep: 2 << 30 }
                    ),
                ],
                memory: Some(16 << 30),
                reach: None,
            }
        );
        let bare = parse_node("quic=127.0.0.1:0,plane=127.0.0.1:0").unwrap();
        assert!(bare.volumes.is_empty() && bare.domain.is_none() && bare.memory.is_none());
        // The S3 endpoint at the QUIC address unless stated, and the names its clients dial.
        assert_eq!(bare.s3(), bare.quic);
        let named = parse_node(
            "quic=0.0.0.0:7000,plane=0.0.0.0:7001,s3=0.0.0.0:443,name=s3.example,name=s3-b.example",
        )
        .unwrap();
        assert_eq!(named.s3(), address("0.0.0.0:443"));
        assert_eq!(named.names, ["s3.example", "s3-b.example"]);
        for refused in [
            // Every address, and no name its certificate can carry.
            "quic=0.0.0.0:7000,plane=0.0.0.0:7001",
            "quic=127.0.0.1:0,plane=127.0.0.1:0,name=-s3.example",
            "quic=127.0.0.1:0,plane=127.0.0.1:0,s3=nowhere",
            "quic=127.0.0.1:0,plane=127.0.0.1:0,s3=127.0.0.1:1,s3=127.0.0.1:2",
        ] {
            assert!(
                matches!(parse_node(refused), Err(Error::Founder(..))),
                "{refused}"
            );
        }
        for bad in [
            "quic",
            "plane=127.0.0.1:1",
            "quic=127.0.0.1:1",
            "quic=here,plane=127.0.0.1:1",
            "quic=127.0.0.1:1,plane=127.0.0.1:2,quic=127.0.0.1:3",
            "quic=127.0.0.1:1,plane=127.0.0.1:2,volume=/d:1G",
            "quic=127.0.0.1:1,plane=127.0.0.1:2,domain=a,volume=/d",
            "quic=127.0.0.1:1,plane=127.0.0.1:2,domain=a,volume=:1G",
            "quic=127.0.0.1:1,plane=127.0.0.1:2,domain=",
            "quic=127.0.0.1:1,plane=127.0.0.1:2,colour=red",
            "quic=127.0.0.1:1,plane=127.0.0.1:2,memory=lots",
        ] {
            assert!(matches!(parse_node(bad), Err(Error::Founder(..))), "{bad}");
        }
        let long = format!(
            "quic=127.0.0.1:1,plane=127.0.0.1:2,domain={}",
            "d".repeat(MAX_DOMAIN + 1)
        );
        assert!(parse_node(&long).is_err());
    }

    /// A founder bound at every address is dialed at its reach, on its own ports: a Service's name
    /// in front of a pod (research/78). Without one, an address that can be dialed is its own, and
    /// one that cannot is none, its peers told on standard input as it binds.
    #[test]
    fn a_founder_is_dialed_at_its_reach_on_its_own_ports() {
        let pod = parse_node(
            "quic=0.0.0.0:7000,plane=0.0.0.0:7001,reach=mantle-0.cell.svc.cluster.local,\
             s3=0.0.0.0:7480,name=s3.cell.svc.cluster.local",
        )
        .unwrap();
        assert_eq!(
            pod.reach.as_deref(),
            Some("mantle-0.cell.svc.cluster.local")
        );
        assert_eq!(
            pod.dialed(pod.quic).as_deref(),
            Some("mantle-0.cell.svc.cluster.local:7000")
        );
        assert_eq!(
            pod.dialed(pod.plane).as_deref(),
            Some("mantle-0.cell.svc.cluster.local:7001")
        );
        let v6 =
            parse_node("quic=[::]:7000,plane=[::]:7001,reach=fd00::7,name=s3.example").unwrap();
        assert_eq!(v6.dialed(v6.quic).as_deref(), Some("[fd00::7]:7000"));
        let own = parse_node("quic=10.0.0.1:7000,plane=10.0.0.1:7001").unwrap();
        assert_eq!(own.dialed(own.quic).as_deref(), Some("10.0.0.1:7000"));
        let binding = parse_node("quic=127.0.0.1:0,plane=127.0.0.1:0").unwrap();
        assert_eq!(binding.dialed(binding.quic), None);
        for refused in [
            "quic=0.0.0.0:7000,plane=0.0.0.0:7001,reach=-bad,name=s3.example",
            "quic=0.0.0.0:7000,plane=0.0.0.0:7001,reach=a.example,reach=b.example,name=s3.example",
            "quic=127.0.0.1:0,plane=127.0.0.1:7001,reach=a.example",
        ] {
            assert!(
                matches!(parse_node(refused), Err(Error::Founder(..))),
                "{refused}"
            );
        }
        // Each founder's peers name it at its reach, and its clients do through the cell file.
        let root = tempfile::tempdir().unwrap();
        let out = root.path().join("cell");
        let founders = (0..3)
            .map(|n| Founder {
                quic: address("0.0.0.0:7000"),
                plane: address("0.0.0.0:7001"),
                s3: Some(address("0.0.0.0:7480")),
                names: vec![format!("s3-{n}.cell.svc.cluster.local")],
                domain: None,
                volumes: Vec::new(),
                memory: None,
                reach: Some(format!("mantle-{n}.cell.svc.cluster.local")),
            })
            .collect();
        let cell = Cell {
            founders,
            ..Cell::default()
        };
        init(&mut Vec::new(), &out, &cell).unwrap();
        let authority = Authority::read(&out).unwrap();
        let reached: Vec<&str> = authority.nodes.iter().map(|(_, at)| at.as_str()).collect();
        assert_eq!(
            reached,
            (0..3)
                .map(|n| format!("mantle-{n}.cell.svc.cluster.local:7000"))
                .collect::<Vec<_>>()
        );
        for (id, _) in &authority.nodes {
            let config =
                std::fs::read_to_string(out.join(format!("node-{id}")).join("config")).unwrap();
            let peers: Vec<&str> = config
                .lines()
                .filter(|l| l.starts_with("peer = "))
                .collect();
            assert_eq!(peers.len(), 2, "{config}");
            for line in peers {
                assert!(
                    line.contains(".cell.svc.cluster.local:7000 mantle-")
                        && line.ends_with(".cell.svc.cluster.local:7001"),
                    "{line}"
                );
            }
        }
    }

    /// A machines file states the cell's own keys, then a block a founder; a volume's directory may
    /// hold a space; each malformed line is refused at it, a founder's at its block's first line.
    #[test]
    fn a_machines_file_states_the_cell_then_each_founder() {
        let text = "# the cell\nmax_nodes = 3\nmax_volumes = 6\ndurability.target = 1e-9\n\n\
                    [node]\nquic = 127.0.0.1:1\nplane = 127.0.0.1:2\ndomain = a # rack a\n\
                    volume = /my data/disk 1G\n[node]\nquic = 127.0.0.1:3\nplane = 127.0.0.1:4\n";
        let cell = parse_file(Path::new("m.conf"), text).unwrap();
        assert_eq!(
            (cell.max_nodes, cell.max_volumes, cell.target),
            (Some(3), Some(6), Some(1e-9))
        );
        assert_eq!(cell.founders.len(), 2);
        assert_eq!(cell.founders[0].domain.as_deref(), Some("a"));
        assert_eq!(
            cell.founders[0].volumes,
            vec![(PathBuf::from("/my data/disk"), VolumeBytes::Stated(1 << 30))]
        );
        assert_eq!(cell.founders[1].quic, address("127.0.0.1:3"));
        for (text, line) in [
            ("quic = 127.0.0.1:1\n", 1),
            ("max_nodes = 3\nmax_nodes = 4\n", 2),
            ("max_nodes = three\n", 1),
            ("max_nodes\n", 1),
            (
                "max_nodes = 3\n[node]\nquic = here\nplane = 127.0.0.1:2\n",
                2,
            ),
            (
                "[node]\nquic = 127.0.0.1:1\nplane = 127.0.0.1:2\n[node]\nmax_nodes = 3\n",
                4,
            ),
        ] {
            assert!(
                matches!(parse_file(Path::new("m.conf"), text), Err(Error::File(_, n, _)) if n == line),
                "{text:?}"
            );
        }
    }

    /// The cell file states the most bytes any founder's volumes state, and a node granted with no
    /// volume of its own states them as its cell's (`cell.volume`), which its engines estimate the
    /// cell's rows by; one granted with a volume states none, and a cell whose founders state no
    /// volume at its bytes states nothing to grant.
    #[test]
    fn a_node_granted_with_no_volume_states_its_founders_volumes() {
        let root = tempfile::tempdir().unwrap();
        let founder = |n: u16, volumes: Vec<(PathBuf, VolumeBytes)>| Founder {
            quic: address(&format!("127.0.0.1:{}", 7300 + n)),
            plane: address(&format!("127.0.0.1:{}", 8300 + n)),
            s3: None,
            names: Vec::new(),
            domain: Some("rack".into()),
            volumes,
            memory: None,
            reach: None,
        };
        let stated = |at: &str, bytes: u64| (root.path().join(at), VolumeBytes::Stated(bytes));
        let cell = Cell {
            founders: vec![
                founder(0, vec![stated("v0", 256 << 20)]),
                founder(1, vec![stated("v1a", 256 << 20), stated("v1b", 256 << 20)]),
            ],
            max_nodes: Some(5),
            max_volumes: Some(10),
            ..Cell::default()
        };
        let out = root.path().join("cell");
        init(&mut Vec::new(), &out, &cell).unwrap();
        let config = |dir: &Path| std::fs::read_to_string(dir.join("config")).unwrap();
        let bare = root.path().join("bare");
        grant(&mut Vec::new(), &bare, &out, &founder(2, Vec::new())).unwrap();
        assert!(
            config(&bare)
                .lines()
                .any(|l| l == format!("cell.volume = {}", 512u64 << 20))
        );
        let with = root.path().join("with");
        grant(
            &mut Vec::new(),
            &with,
            &out,
            &founder(3, vec![stated("v3", 1 << 30)]),
        )
        .unwrap();
        assert!(!config(&with).contains("cell.volume"));
        let spared = Cell {
            founders: vec![founder(
                4,
                vec![(root.path().join("v4"), VolumeBytes::Spare { keep: 0 })],
            )],
            max_nodes: Some(5),
            max_volumes: Some(10),
            ..Cell::default()
        };
        let other = root.path().join("other");
        init(&mut Vec::new(), &other, &spared).unwrap();
        let none = root.path().join("none");
        grant(&mut Vec::new(), &none, &other, &founder(5, Vec::new())).unwrap();
        assert!(!config(&none).contains("cell.volume"));
    }

    /// A granted node keeps the cell's issuer as a founder does, its certificate and its key
    /// beside its authority's certificate, the key owner-only.
    #[test]
    fn a_granted_node_keeps_the_cells_issuer() {
        let root = tempfile::tempdir().unwrap();
        let out = root.path().join("cell");
        let founder = |n: u16| Founder {
            quic: address(&format!("127.0.0.1:{}", 7200 + n)),
            plane: address(&format!("127.0.0.1:{}", 8200 + n)),
            s3: None,
            names: Vec::new(),
            domain: Some("rack".into()),
            volumes: Vec::new(),
            memory: None,
            reach: None,
        };
        let cell = Cell {
            founders: vec![founder(0), founder(1)],
            max_nodes: Some(5),
            max_volumes: Some(10),
            ..Cell::default()
        };
        init(&mut Vec::new(), &out, &cell).unwrap();
        let granted = root.path().join("granted");
        grant(&mut Vec::new(), &granted, &out, &founder(2)).unwrap();
        for file in [ISSUER.0, ISSUER.1] {
            assert_eq!(
                std::fs::read(granted.join("identity").join(file)).unwrap(),
                std::fs::read(out.join("authority").join(file)).unwrap(),
                "{file}"
            );
        }
        assert!(files::owner_only(&granted.join("identity").join(ISSUER.1)).unwrap());
    }

    /// A cell has its issuer from its founding, beside the authority's key and in each founder's
    /// directory beside its authority's certificate, where a node's start finds it; and one made
    /// before cells had an issuer is given it by `mantle init issuer`, once: each copy its
    /// certificate whole, issued by the authority, and its key owner-only, the one the certificate
    /// certifies, written durable under a temporary name that none is left at.
    #[test]
    fn an_issuer_is_made_once_owner_only_beside_the_authority_and_in_each_founder() {
        let root = tempfile::tempdir().unwrap();
        let out = root.path().join("cell");
        let founders = (0..2)
            .map(|n| Founder {
                quic: address(&format!("127.0.0.1:{}", 7100 + n)),
                plane: address(&format!("127.0.0.1:{}", 8100 + n)),
                s3: None,
                names: Vec::new(),
                domain: None,
                volumes: Vec::new(),
                memory: None,
                reach: None,
            })
            .collect();
        let cell = Cell {
            founders,
            ..Cell::default()
        };
        init(&mut Vec::new(), &out, &cell).unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let authority_dir = out.join("authority");
        let authority = std::fs::read(authority_dir.join("authority.der")).unwrap();
        let mut holders = vec![authority_dir.clone()];
        for entry in std::fs::read_dir(&out).unwrap() {
            let path = entry.unwrap().path();
            if path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("node-")
            {
                holders.push(path.join("identity"));
            }
        }
        assert_eq!(holders.len(), 3);
        let held = |dir: &Path| -> Vec<u8> {
            let der = std::fs::read(dir.join(ISSUER.0)).unwrap();
            mantle_cert::issuer_issued(&der, &authority, now).unwrap();
            let key = dir.join(ISSUER.1);
            assert!(files::owner_only(&key).unwrap(), "{}", key.display());
            let pair = KeyPair::from_pkcs8(&mantle_node::secret::read(&key).unwrap()).unwrap();
            assert_eq!(mantle_cert::public_key(&der).unwrap(), pair.public_key());
            for entry in std::fs::read_dir(dir).unwrap() {
                let name = entry.unwrap().file_name();
                assert!(!name.to_str().unwrap().ends_with(".new"), "{name:?}");
            }
            der
        };
        let founded = held(&authority_dir);
        for dir in &holders {
            assert_eq!(held(dir), founded);
            std::fs::remove_file(dir.join(ISSUER.0)).unwrap();
            std::fs::remove_file(dir.join(ISSUER.1)).unwrap();
        }
        let mut said = Vec::new();
        issuer(&mut said, &out).unwrap();
        let said = String::from_utf8(said).unwrap();
        assert_eq!(
            said.lines()
                .filter(|l| l.starts_with("issuer kept by node "))
                .count(),
            2
        );
        let made = held(&authority_dir);
        assert_ne!(made, founded);
        for dir in &holders {
            assert_eq!(held(dir), made);
        }
        assert!(matches!(issuer(&mut Vec::new(), &out), Err(Error::Cell(_))));
        assert_eq!(held(&authority_dir), made);
    }

    /// Founders of a larger cell: each directory pins every other founder and names every founder a
    /// voter, its configuration read by `mantle serve` from wherever the directory moves, its paths
    /// from it; the authority's directory keeps the authority's key and the root key; and a
    /// directory holding files already is refused.
    #[test]
    fn each_founders_directory_moves_whole_and_reads_as_stated() {
        let root = tempfile::tempdir().unwrap();
        let out = root.path().join("cell");
        let founders = (0..3)
            .map(|n| Founder {
                quic: address(&format!("127.0.0.1:{}", 7000 + n)),
                plane: address(&format!("127.0.0.1:{}", 8000 + n)),
                s3: None,
                names: Vec::new(),
                domain: None,
                volumes: Vec::new(),
                memory: if n == 1 { Some(8 << 30) } else { None },
                reach: None,
            })
            .collect();
        let cell = Cell {
            founders,
            target: Some(1e-9),
            ..Cell::default()
        };
        let mut said = Vec::new();
        init(&mut said, &out, &cell).unwrap();
        let said = String::from_utf8(said).unwrap();
        let nodes: Vec<(u64, PathBuf)> = said
            .lines()
            .filter_map(|line| line.strip_prefix("node "))
            .map(|rest| {
                let (id, rest) = rest.split_once(' ').unwrap();
                let (dir, _) = rest.split_once(": ").unwrap();
                (id.parse().unwrap(), PathBuf::from(dir))
            })
            .collect();
        assert_eq!(nodes.len(), 3);
        for file in ["authority.der", "authority.key"] {
            assert!(out.join("authority").join(file).is_file(), "{file}");
        }
        assert!(files::owner_only(&out.join("authority").join("authority.key")).unwrap());
        assert_eq!(
            std::fs::read(out.join("authority").join("keys").join("1"))
                .unwrap()
                .len(),
            32
        );
        let ids: Vec<u64> = nodes.iter().map(|(id, _)| *id).collect();
        for (at, (id, dir)) in nodes.iter().enumerate() {
            let moved = root.path().join(format!("host-{at}"));
            std::fs::rename(dir, &moved).unwrap();
            let stated = mantle_node::process::read_stated(&moved.join("config")).unwrap();
            assert_eq!(stated.id, *id);
            assert_eq!(stated.dir, moved.join("."));
            assert_eq!(stated.quic, cell.founders[at].quic);
            assert_eq!(stated.voters, ids);
            assert_eq!(stated.ranges.len(), LAYERS.len());
            assert_eq!(stated.ceiling, cell.founders[at].memory);
            assert_eq!(stated.target, Some(1e-9));
            for path in [&stated.authority, &stated.certificate, &stated.key] {
                assert!(
                    path.starts_with(&moved) && path.is_file(),
                    "{}",
                    path.display()
                );
            }
            assert!(files::owner_only(&stated.key).unwrap());
            let peers: Vec<u64> = stated.peers.iter().map(|peer| peer.id).collect();
            assert_eq!(
                peers,
                ids.iter().copied().filter(|p| p != id).collect::<Vec<_>>()
            );
            for peer in &stated.peers {
                assert!(peer.certificate.starts_with(&moved) && peer.certificate.is_file());
                let (_, founder) = ids
                    .iter()
                    .zip(&cell.founders)
                    .find(|(id, _)| **id == peer.id)
                    .unwrap();
                let (quic, plane) = peer.at.clone().unwrap();
                assert_eq!(quic, Endpoint::parse(&founder.quic.to_string()).unwrap());
                assert_eq!(plane, Endpoint::parse(&founder.plane.to_string()).unwrap());
            }
            let (generation, key) = &stated.keys[0];
            assert_eq!(*generation, 1);
            assert!(files::owner_only(key).unwrap());
            // The S3 endpoint at its QUIC address, its certificate and key, and the cell's key.
            let s3 = stated.s3.clone().unwrap();
            assert_eq!(
                (s3.listen, s3.region.as_str()),
                (cell.founders[at].quic, "us-east-1")
            );
            assert_eq!(s3.certificate, moved.join("identity/s3.der"));
            assert_eq!(s3.key, moved.join("identity/s3.key"));
            let [(id, file)] = s3.access.as_slice() else {
                panic!("{:?}", s3.access)
            };
            assert_eq!(file, &moved.join("keys/s3").join(id));
            assert!(files::owner_only(file).unwrap() && files::owner_only(&s3.key).unwrap());
            mantle_node::process::s3_keys(&s3).unwrap();
        }
        assert!(matches!(
            init(&mut Vec::new(), &out, &cell),
            Err(Error::Exists(_))
        ));
        // The cell's bounds are stated where a founder keeps volumes.
        let stored = Cell {
            founders: vec![Founder {
                domain: Some("a".into()),
                volumes: vec![(root.path().join("d"), VolumeBytes::Stated(1 << 30))],
                ..cell.founders[0].clone()
            }],
            ..Cell::default()
        };
        assert!(matches!(
            init(&mut Vec::new(), &root.path().join("other"), &stored),
            Err(Error::Cell(_))
        ));
    }

    /// A setting of an AWS file, its key, its value and its nested settings; and a section, its
    /// name and its settings.
    type Setting = (String, String, Vec<(String, String)>);
    type Section = (String, Vec<Setting>);

    /// A shared config or credentials file as AWS's tools read one (AWS SDKs and Tools Reference
    /// Guide, the files' format): each section's settings in order, `key = value`, a setting's
    /// indented lines its nested ones.
    fn aws_file(text: &str) -> Vec<Section> {
        let mut sections: Vec<Section> = Vec::new();
        for line in text.lines() {
            if line.trim().is_empty() || line.trim_start().starts_with('#') {
                continue;
            }
            if let Some(name) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
                sections.push((name.to_owned(), Vec::new()));
                continue;
            }
            let (key, value) = line.split_once('=').unwrap();
            let (key, value) = (key.trim().to_owned(), value.trim().to_owned());
            let settings = &mut sections.last_mut().unwrap().1;
            if line.starts_with(char::is_whitespace) {
                settings.last_mut().unwrap().2.push((key, value));
            } else {
                settings.push((key, value, Vec::new()));
            }
        }
        sections
    }

    /// A cell of `founders`, made in a directory of the test's own: its OUT and what init said.
    fn made(founders: &[&str]) -> (tempfile::TempDir, PathBuf, String) {
        let root = tempfile::tempdir().unwrap();
        let out = root.path().join("cell");
        let cell = Cell {
            founders: founders.iter().map(|f| parse_node(f).unwrap()).collect(),
            ..Cell::default()
        };
        let mut said = Vec::new();
        init(&mut said, &out, &cell).unwrap();
        (root, out, String::from_utf8(said).unwrap())
    }

    /// Each founder's directory in `out`, OUT/node-ID.
    fn founders_dirs(out: &Path) -> Vec<PathBuf> {
        let mut dirs: Vec<PathBuf> = std::fs::read_dir(out)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|d| {
                d.file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with("node-"))
            })
            .collect();
        dirs.sort();
        dirs
    }

    /// Each founder's S3 keys as its `mantle serve` reads them.
    fn founders_keys(out: &Path) -> Vec<mantle_node::process::S3Keys> {
        founders_dirs(out)
            .iter()
            .map(|dir| {
                let stated = mantle_node::process::read_stated(&dir.join("config")).unwrap();
                mantle_node::process::s3_keys(&stated.s3.unwrap()).unwrap()
            })
            .collect()
    }

    /// Every founder holds the cell's one access key, readable by its owner alone; the operator's
    /// profile holds the same key, names the endpoint plain on a loopback address, and a request
    /// signed with it verifies under every founder's key, as one signed with another secret does
    /// not (SignatureDoesNotMatch).
    #[test]
    fn the_profiles_key_signs_what_every_founder_verifies() {
        let (_root, out, said) = made(&[
            "quic=127.0.0.1:9101,plane=127.0.0.1:9201",
            "quic=127.0.0.1:9102,plane=127.0.0.1:9202",
        ]);
        let keys = founders_keys(&out);
        assert_eq!(keys.len(), 2);
        let access = &keys[0].access;
        assert!(
            keys.iter()
                .all(|k| &k.access == access && k.region == "us-east-1")
        );
        let [key] = access.as_slice() else {
            panic!("{access:?}")
        };
        assert_eq!(key.id.len(), mantle_s3::account::KEY_ID_CHARS);
        assert!(
            key.id
                .bytes()
                .all(|b| mantle_s3::account::BASE32.contains(&b))
        );
        assert!(mantle_s3::account::valid_id(&key.account));
        assert_eq!(key.secret.len(), 40);
        let s3 = out.join("s3");
        assert!(files::owner_only(&s3.join("credentials")).unwrap());
        let credentials = aws_file(&std::fs::read_to_string(s3.join("credentials")).unwrap());
        let config = aws_file(&std::fs::read_to_string(s3.join("config")).unwrap());
        assert_eq!(
            credentials,
            [(
                PROFILE.to_owned(),
                vec![
                    ("aws_access_key_id".into(), key.id.clone(), vec![]),
                    ("aws_secret_access_key".into(), key.secret.clone(), vec![]),
                ]
            )]
        );
        assert_eq!(
            config,
            [(
                format!("profile {PROFILE}"),
                vec![
                    ("region".into(), "us-east-1".into(), vec![]),
                    (
                        "endpoint_url".into(),
                        "http://127.0.0.1:9101".into(),
                        vec![]
                    ),
                    (
                        "s3".into(),
                        String::new(),
                        vec![("addressing_style".into(), "path".into())]
                    ),
                ]
            )]
        );
        assert!(said.contains(&format!("s3 profile {PROFILE}: AWS_CONFIG_FILE=")));
        // A GetObject signed with the profile's key, as a client of the endpoint signs it.
        use mantle_s3::sigv4::{AuthError, EMPTY_SHA256, Request, canonical_request, hex, sign};
        let when = "20261025T120000Z";
        let now = mantle_s3::time::parse_amz_date(when).unwrap();
        let signed = |secret: &str| {
            let headers = [
                ("host", "127.0.0.1:9101"),
                ("x-amz-content-sha256", EMPTY_SHA256),
                ("x-amz-date", when),
            ];
            let request: Request<'_> = Request {
                method: "GET",
                path: "/bucket/key",
                query: "",
                headers: &headers,
            };
            let names = ["host", "x-amz-content-sha256", "x-amz-date"];
            let canonical = canonical_request(&request, &names, EMPTY_SHA256, None).unwrap();
            let signature = sign(secret, when, "20261025", "us-east-1", &canonical).unwrap();
            let authorization = format!(
                "AWS4-HMAC-SHA256 Credential={}/20261025/us-east-1/s3/aws4_request,\
                 SignedHeaders={},Signature={}",
                key.id,
                names.join(";"),
                hex(&signature)
            );
            let mut all = headers.to_vec();
            all.push(("authorization", authorization.as_str()));
            let request: Request<'_> = Request {
                headers: &all,
                ..request
            };
            // The founder's verifier: each of its keys' secrets by id.
            keys.iter()
                .map(|founder| {
                    mantle_s3::sigv4::verify(&request, &founder.region, now, |id| {
                        founder
                            .access
                            .iter()
                            .find(|k| k.id == id)
                            .map(|k| k.secret.clone())
                    })
                    .map(|v| v.access_key)
                })
                .collect::<Vec<_>>()
        };
        assert!(
            signed(&key.secret)
                .iter()
                .all(|v| v.as_ref() == Ok(&key.id))
        );
        let other = BASE64.encode([7u8; mantle_s3::account::SECRET_BYTES]);
        assert!(
            signed(&other)
                .iter()
                .all(|v| v == &Err(AuthError::Mismatch))
        );
    }

    /// A founder's key or access key file others may read is refused by its start, as the node's
    /// own key is.
    #[cfg(unix)]
    #[test]
    fn an_access_key_others_may_read_is_refused() {
        use std::os::unix::fs::PermissionsExt as _;
        let (_root, out, _) = made(&[
            "quic=127.0.0.1:9101,plane=127.0.0.1:9201",
            "quic=127.0.0.1:9102,plane=127.0.0.1:9202",
        ]);
        let dir = founders_dirs(&out).remove(0);
        let s3 = mantle_node::process::read_stated(&dir.join("config"))
            .unwrap()
            .s3
            .unwrap();
        for path in [&s3.access[0].1, &s3.key] {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(matches!(
                mantle_node::process::s3_keys(&s3),
                Err(mantle_node::process::ProcessError::Secret(..))
            ));
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        mantle_node::process::s3_keys(&s3).unwrap();
    }

    /// Each founder's endpoint certificate verifies under the authority the profile's CA bundle
    /// holds for every name and address its clients dial, and no other; the profile names the
    /// first founder's endpoint by its first name, over TLS on an address that is not loopback.
    #[test]
    fn an_endpoint_certificate_verifies_for_the_names_its_clients_dial() {
        use hyper_tls::RootCertStore;
        use hyper_tls::client::WebPkiServerVerifier;
        use hyper_tls::client::danger::ServerCertVerifier;
        use hyper_tls::pki_types::{CertificateDer, ServerName, UnixTime};
        let (_root, out, _) = made(&[
            "quic=10.1.2.3:9101,plane=10.1.2.3:9201,s3=10.1.2.3:9443,name=s3.example",
            "quic=127.0.0.1:9102,plane=127.0.0.1:9202",
        ]);
        let s3 = out.join("s3");
        let pem = std::fs::read_to_string(s3.join("authority.pem")).unwrap();
        let body: String = pem.lines().filter(|l| !l.starts_with("-----")).collect();
        let authority = BASE64.decode(body).unwrap();
        assert_eq!(
            authority,
            std::fs::read(out.join("authority").join("s3-authority.der")).unwrap()
        );
        let mut roots = RootCertStore::empty();
        roots.add(CertificateDer::from(authority)).unwrap();
        let provider = hyper_tls::crypto::aws_lc_rs::default_provider();
        let verifier = WebPkiServerVerifier::builder_with_provider(roots, &provider)
            .build()
            .unwrap();
        let keys = founders_keys(&out);
        let mut checked = 0;
        for founder in &keys {
            let names: &[&str] = if founder.listen.ip().is_loopback() {
                &["127.0.0.1", "localhost"]
            } else {
                &["s3.example", "10.1.2.3"]
            };
            let verify = |name: &str| {
                verifier.verify_server_cert(
                    &CertificateDer::from(founder.certificate.clone()),
                    &[],
                    &ServerName::try_from(name.to_owned()).unwrap(),
                    &[],
                    UnixTime::now(),
                )
            };
            for name in names {
                verify(name).unwrap_or_else(|e| panic!("{name}: {e:?}"));
                checked += 1;
            }
            // The node reads the names its config states, which requests' hosts name buckets by.
            let stated: &[&str] = if founder.listen.ip().is_loopback() {
                &[]
            } else {
                &["s3.example"]
            };
            assert_eq!(founder.names, stated);
            assert!(verify("other.example").is_err() && verify("10.9.9.9").is_err());
            mantle_cert::EndpointKey::from_pkcs8(&founder.key).unwrap();
        }
        assert_eq!(checked, 4);
        let config = aws_file(&std::fs::read_to_string(s3.join("config")).unwrap());
        let settings = &config[0].1;
        assert_eq!(settings[1].1, "https://s3.example:9443");
        assert_eq!(settings[2].0, "ca_bundle");
        let bundle = PathBuf::from(&settings[2].1);
        assert!(bundle.is_absolute() && std::fs::read_to_string(bundle).unwrap() == pem);
    }

    /// A cell whose endpoints' ports are chosen as they bind writes no config naming an endpoint,
    /// and says so; its credentials and the authority's PEM are written all the same.
    #[test]
    fn a_profile_names_no_endpoint_whose_port_is_not_yet_chosen() {
        let (_root, out, said) = made(&[
            "quic=127.0.0.1:0,plane=127.0.0.1:0",
            "quic=127.0.0.1:0,plane=127.0.0.1:0",
        ]);
        let s3 = out.join("s3");
        assert!(!s3.join("config").exists());
        assert!(s3.join("credentials").is_file() && s3.join("authority.pem").is_file());
        assert!(said.contains("no config names one"), "{said}");
    }
}
