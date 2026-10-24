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

use std::fmt;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use mantle_cert::{KeyPair, Serial, Validity};
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
}

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
    let mut volumes = Vec::new();
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
            other => {
                return Err(format!(
                    "{other} is not a founder's statement: quic, plane, domain, volume or memory"
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
    Ok(Founder {
        quic: address(quic, "quic")?,
        plane: address(plane, "plane")?,
        domain,
        volumes,
        memory: memory.map(|m| parse_bytes(&m)).transpose()?,
    })
}

/// A founder in `--node`'s form: comma-separated `key=value`s, `quic=ADDR`, `plane=ADDR`,
/// `domain=NAME`, `volume=DIR:SIZE` (repeatable, the size after the path's last colon) and
/// `memory=SIZE`. A directory with a comma in it is stated in a machines file instead.
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
/// `max_volumes`, `durability.target`), then a block for each founder, opened by a `[node]` line,
/// of the statements `--node` takes, a volume as `volume = DIR SIZE`.
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
            other => {
                return Err(bad(format!(
                    "{other} is not the cell's statement: max_nodes, max_volumes or durability.target; a founder's opens with [node]"
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

/// The config of a founder: its statements and the cell's, paths relative to its directory.
fn config(id: u64, founder: &Founder, cell: &Cell, ids: &[u64], groups: &[u128]) -> String {
    let mut text = format!(
        "# The operator's statements for node {id}, made by `mantle init`: every other value \
         `mantle serve` measures or derives (docs/design/node.md §1.4). Paths are from this \
         file's directory.\nid = {id}\ndir = .\nquic = {}\nplane = {}\n\
         authority = identity/authority.der\ncertificate = identity/node.der\nkey = identity/node.key\n",
        founder.quic, founder.plane
    );
    let voters: Vec<String> = ids.iter().map(u64::to_string).collect();
    text.push_str(&format!("voters = {}\n", voters.join(",")));
    for (group, layer) in groups.iter().zip(LAYERS) {
        text.push_str(&format!("range = {group:x} {}\n", layer_name(layer)));
    }
    text.push_str("root_key = 1 keys/1\n");
    for peer in ids.iter().filter(|peer| **peer != id) {
        text.push_str(&format!("peer = {peer} node identity/peers/{peer}.der\n"));
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
    let one = cell.founders.len() == 1;
    let authority_dir = if one {
        out.join("identity")
    } else {
        out.join("authority")
    };
    files::create_dir(&authority_dir)?;
    files::write(&authority_dir.join("authority.der"), &authority)?;
    files::write_secret(&authority_dir.join("authority.key"), authority_key.pkcs8())?;
    if !one {
        files::create_dir(&authority_dir.join("keys"))?;
        files::write_secret(&authority_dir.join("keys").join("1"), &root_key)?;
    }
    writeln!(said, "cell {authority_name}")?;
    writeln!(
        said,
        "authority {}: its key signs every member's certificate; keep it",
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
            config(id, founder, cell, &ids, &groups).as_bytes(),
        )?;
        writeln!(
            said,
            "node {id} {}: quic {} plane {}",
            dir.display(),
            founder.quic,
            founder.plane
        )?;
        first.get_or_insert((founder, dir));
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
            let peers: Vec<u64> = stated.peers.iter().map(|(peer, _, _)| *peer).collect();
            assert_eq!(
                peers,
                ids.iter().copied().filter(|p| p != id).collect::<Vec<_>>()
            );
            for (_, _, certificate) in &stated.peers {
                assert!(certificate.starts_with(&moved) && certificate.is_file());
            }
            let (generation, key) = &stated.keys[0];
            assert_eq!(*generation, 1);
            assert!(files::owner_only(key).unwrap());
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
}
