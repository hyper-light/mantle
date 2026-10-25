//! `mantle init` (docs/design/node.md §1.5, §1.6): a cell made at once from the machines it starts
//! with, its founders, each given by its operator's statements alone: where it listens, its
//! failure domain, the directories and bytes of the volumes it keeps and, where stated, the most
//! memory it may hold. They come on the command line, a `--node` each, in a machines file
//! (`--founders`), or both, the file's first; the cell's bounds on its volume table and its
//! durability target likewise (architecture §11 leaves the bounds' values open: they are stated).
//!
//! It draws from the OS the cell's authority's key (`mantle_cert::authority`), the root key's first
//! generation, 32 bytes (encryption.md §2), the founding ranges' groups, one for the root range and
//! one for each of the Bucket, Name, File and Block layers, and each founder's id, 64 bits; issues
//! each founder's certificate under the authority (`mantle_cert::node`); and writes each founder's
//! directory (§1.6): its `config`, the stated keys alone (`mantle_node::process::STATED_KEYS`), its
//! paths relative to the directory so it moves to its host whole; `identity/`, the authority's
//! certificate, the founder's own certificate and key, and each other founder's certificate,
//! pinned (§3.6); and `keys/`, the root key. Keys are owner-only (`crate::files`). One founder makes
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
//! stock clients verify it (`mantle_cert::endpoint_authority`; research/71), kept with the cell's,
//! and issues each founder's endpoint a certificate for the names and the address its clients dial.
//! For the operator it writes OUT/s3: the key as AWS's shared credentials file, a config naming the
//! endpoint where init can name it, and the endpoint's authority as PEM for that config's
//! `ca_bundle`.

use std::fmt;
use std::io::Write;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use mantle_cert::{
    EndpointKey, EndpointName, KeyPair, MAX_ENDPOINT_VALIDITY, Serial, Validity, check_dns_name,
};
use mantle_meta::apply::Layer;
use mantle_meta::root::MAX_DOMAIN;
use mantle_node::directory::node_name;

use crate::files;

/// RFC 5280 §4.1.2.5: "To indicate that a certificate has no well-defined expiration date, the
/// notAfter SHOULD be assigned the GeneralizedTime value of 99991231235959Z", in Unix seconds.
/// Certificates are not yet rotated (node.md §1.5): until they are, none expires.
const NO_EXPIRY: u64 = 253_402_300_799;

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
    pub volumes: Vec<(PathBuf, u64)>,
    /// The most memory the node may hold, where stated.
    pub memory: Option<u64>,
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
    /// The region its S3 endpoint serves, where stated ([`Cell::region`]).
    pub region: Option<String>,
}

impl Founder {
    /// Where its S3 endpoint listens: as stated, else at its QUIC address, TCP beside QUIC's UDP on
    /// the one port, as an https URI's port is dialed over either (RFC 9114 §3.1).
    pub fn s3(&self) -> SocketAddr {
        self.s3.unwrap_or(self.quic)
    }
}

impl Cell {
    /// The region its S3 endpoint serves: as stated, else S3's own default, us-east-1, the region
    /// a bucket made with no constraint is in (S3 API Reference, CreateBucket; research/71).
    pub fn region(&self) -> &str {
        self.region.as_deref().unwrap_or(DEFAULT_REGION)
    }
}

/// S3's default region (S3 API Reference, CreateBucket: "If you don't specify a Region, the bucket
/// is created in the US East (N. Virginia) Region (us-east-1) by default").
const DEFAULT_REGION: &str = "us-east-1";

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

/// A volume stated as its directory and its bytes, the directory before `separator`'s last place
/// in `text`, so a path with that character in it, a Windows drive's colon among them, still reads.
fn volume(text: &str, separator: char) -> Result<(PathBuf, u64), String> {
    let (dir, size) = text
        .trim()
        .rsplit_once(separator)
        .ok_or_else(|| format!("{text:?} is not a volume's directory and bytes"))?;
    let dir = dir.trim();
    if dir.is_empty() {
        return Err(format!("{text:?} names no directory"));
    }
    Ok((PathBuf::from(dir), parse_bytes(size)?))
}

/// One founder's statements, each `key` with its `value`, in the order given.
fn founder<'a>(
    pairs: impl IntoIterator<Item = (&'a str, &'a str)>,
    form: char,
) -> Result<Founder, String> {
    let (mut quic, mut plane, mut domain, mut memory) = (None, None, None, None);
    let mut s3 = None;
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
            "name" => {
                check_dns_name(value)
                    .map_err(|_| format!("{value:?} is not a DNS name a certificate may carry"))?;
                names.push(value.to_owned());
            }
            other => {
                return Err(format!(
                    "{other} is not a founder's statement: quic, plane, s3, name, domain, volume \
                     or memory"
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
    };
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
/// path's last colon) and `memory=SIZE`. A directory with a comma in it is stated in a machines
/// file instead.
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
            "region" => {
                stated(cell.region.is_some())?;
                cell.region = Some(check_region(value).map_err(bad)?);
            }
            other => {
                return Err(bad(format!(
                    "{other} is not the cell's statement: max_nodes, max_volumes, durability.target \
                     or region; a founder's opens with [node]"
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

/// The cell's first access key (research/71): the account it acts for, twelve decimal digits
/// (`mantle_s3::account::valid_id`); its id, twenty characters of RFC 4648's base32 alphabet, an id
/// IAM takes (16 to 128 word characters) as long as AWS's own, with no `/`, which SigV4's credential
/// scope separates its parts by; and its secret, 30 octets from the OS as 40 characters of base64,
/// as long as AWS's.
struct AccessKey {
    account: String,
    id: String,
    secret: String,
}

/// The accounts' ids: twelve decimal digits.
const ACCOUNTS: u128 = 1_000_000_000_000;
/// An access key's id in characters, and its secret's octets from the OS.
const KEY_ID_CHARS: usize = 20;
const SECRET_BYTES: usize = 30;
/// RFC 4648 §6's base32 alphabet: 32 characters, so a random octet's low five bits pick one
/// uniformly.
const BASE32: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

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
        let id = random::<KEY_ID_CHARS>()?
            .iter()
            .filter_map(|b| BASE32.get(usize::from(b & 0x1f)).map(|c| char::from(*c)))
            .collect();
        let secret = BASE64.encode(random::<SECRET_BYTES>()?);
        Ok(Self {
            account,
            id,
            secret,
        })
    }

    /// The key's file on a founder, `keys/s3/ID`: its account and its secret.
    fn file(&self) -> String {
        format!("account = {}\nsecret = {}\n", self.account, self.secret)
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
const PROFILE: &str = "mantle";

/// The operator's S3 profile in `dir` (OUT/s3; research/71): the access key as AWS's shared
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
            access.id, access.secret
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
        // A founder stated at port 0, or at no address of its own, learns where it listens only as
        // it binds: its peers are told then, on standard input, as of a peer that moved.
        let dialable = |at: &SocketAddr| at.port() != 0 && !at.ip().is_unspecified();
        let at = if dialable(&other.quic) && dialable(&other.plane) {
            format!(" {} {}", other.quic, other.plane)
        } else {
            String::new()
        };
        text.push_str(&format!(
            "peer = {peer} node identity/peers/{peer}.der{at}\n"
        ));
    }
    for (dir, bytes) in &founder.volumes {
        text.push_str(&format!("volume = {} {bytes}\n", dir.display()));
    }
    if let Some(domain) = &founder.domain {
        text.push_str(&format!("domain = {domain}\n"));
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
    let root_key = random::<32>()?;
    let ids = distinct(cell.founders.len(), || {
        Ok(u64::from_le_bytes(random::<8>()?))
    })?;
    let groups = distinct(LAYERS.len(), || Ok(u128::from_le_bytes(random::<16>()?)))?;
    let mut issued = Vec::with_capacity(ids.len());
    for &id in &ids {
        let own = KeyPair::generate().map_err(cert)?;
        let certificate = mantle_cert::node(
            &authority_key,
            &authority_name,
            own.public_key(),
            &node_name(id),
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
    files::write(&authority_dir.join("s3-authority.der"), &endpoint_authority)?;
    files::write_secret(
        &authority_dir.join("s3-authority.key"),
        endpoint_key.pkcs8(),
    )?;
    if !one {
        files::create_dir(&authority_dir.join("keys"))?;
        files::write_secret(&authority_dir.join("keys").join("1"), &root_key)?;
    }
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
            "quic=10.0.0.1:7000, plane=10.0.0.1:7001,domain=rack-a,volume=/data:4T,volume=C:\\d2:1G,memory=16G",
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
                    (PathBuf::from("/data"), 4 << 40),
                    (PathBuf::from("C:\\d2"), 1 << 30)
                ],
                memory: Some(16 << 30),
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
            vec![(PathBuf::from("/my data/disk"), 1 << 30)]
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
                volumes: vec![(root.path().join("d"), 1 << 30)],
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
        assert_eq!(key.id.len(), KEY_ID_CHARS);
        assert!(key.id.bytes().all(|b| BASE32.contains(&b)));
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
            let request = Request {
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
            let request = Request {
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
        let other = BASE64.encode([7u8; SECRET_BYTES]);
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
