//! Where a case's blocks sit, read from the cell's metadata as its gateway reads it (plan step 9;
//! research/76): each object's version from its Name range, its file's extents from the File
//! range, a completed upload's part files followed to their blocks, each block's chunks' volumes
//! from the Block range, and each volume's node from the root range's volume table. A cell of
//! three domains keeps a block's three copies on three nodes (f34): a block two of whose chunks
//! share a node is counted.
//!
//! The harness reads as a gateway its founders pin (`peer = ID gateway CERTIFICATE`, node.md
//! §3.6), its certificate issued under the cell's authority once `mantle init` made the cell and
//! before any founder starts (`crate::init::gateway`).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use mantle_client::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer};
use mantle_meta::record::Target;
use mantle_node::client::{Client, ClientSettings};
use mantle_node::directory::Peer;
use mantle_node::read::{Query, Read};
use mantle_protocol::bounds::Stated;
use mantle_protocol::classes::{Bounds, Role};

use super::TransferError;

/// The client's pieces a founder the gateway's reader's transport holds, as the cell tests' range
/// readers hold (`crates/mantle/tests/support/cell.rs`'s `client_settings`), which read through
/// CI's busy runner where the least transport did not.
const READER_PIECES: u64 = 3;

/// The groups of the ranges a block's place is read from, as each founder's configuration names
/// them (`range = GROUP LAYER`).
#[derive(Clone, Copy)]
pub struct Groups {
    pub root: u128,
    pub name: u128,
    pub file: u128,
    pub block: u128,
}

impl Groups {
    /// The groups a founder's configuration at `config` names.
    pub fn read(config: &Path) -> Result<Self, TransferError> {
        let text = std::fs::read_to_string(config)?;
        let (mut root, mut name, mut file, mut block) = (None, None, None, None);
        for line in text.lines() {
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            if key.trim() != "range" {
                continue;
            }
            let mut words = value.split_whitespace();
            let (Some(group), Some(layer)) = (words.next(), words.next()) else {
                continue;
            };
            let group = u128::from_str_radix(group, 16).map_err(|_| unread(line))?;
            match layer {
                "root" => root = Some(group),
                "name" => name = Some(group),
                "file" => file = Some(group),
                "block" => block = Some(group),
                _ => {}
            }
        }
        match (root, name, file, block) {
            (Some(root), Some(name), Some(file), Some(block)) => Ok(Self {
                root,
                name,
                file,
                block,
            }),
            _ => Err(unread("a founder's ranges")),
        }
    }
}

fn unread(what: &str) -> TransferError {
    TransferError::Io(std::io::Error::other(format!("does not read: {what}")))
}

/// What the placement of a case's objects came to.
#[derive(Default)]
pub struct Placement {
    pub objects: u64,
    pub blocks: u64,
    /// Blocks two of whose chunks share a node, or whose volume the table does not name.
    pub shared: u64,
}

impl Placement {
    pub fn exact(&self) -> bool {
        self.shared == 0
    }
}

/// Whether the chunks of a block at `volumes` sit on as many nodes as there are chunks, each
/// volume's node as `node_of` says: none shares a node and every volume is in the table.
pub fn distinct(volumes: &[u128], node_of: &BTreeMap<u128, u64>) -> bool {
    let nodes: BTreeSet<u64> = volumes
        .iter()
        .filter_map(|v| node_of.get(v).copied())
        .collect();
    nodes.len() == volumes.len()
}

/// The gateway the harness reads the cell's metadata as: its identity, pinned in every founder's
/// configuration, the cell authority's certificate and each founder's, and the groups.
pub struct Gateway {
    id: u64,
    certificate: Vec<u8>,
    key: Vec<u8>,
    authority: Vec<u8>,
    founders: Vec<(u64, Vec<u8>)>,
    pub groups: Groups,
}

impl Gateway {
    /// The gateway of the cell init made in `cell`, its founders in `founders` (each's id and
    /// directory), pinned now in each founder's configuration, which no founder has started on.
    pub fn pin(cell: &Path, founders: &[(u64, PathBuf)]) -> Result<Self, TransferError> {
        let (id, certificate, key) = crate::init::gateway(cell).map_err(TransferError::Init)?;
        let pinned = cell.join(format!("gateway-{id}.der"));
        crate::files::write(&pinned, &certificate)
            .map_err(|e| TransferError::Io(std::io::Error::other(e.to_string())))?;
        let mut known = Vec::with_capacity(founders.len());
        let mut groups = None;
        for (founder, dir) in founders {
            let config = dir.join("config");
            let mut text = std::fs::read_to_string(&config)?;
            text.push_str(&format!("peer = {id} gateway {}\n", pinned.display()));
            std::fs::write(&config, text)?;
            known.push((
                *founder,
                std::fs::read(dir.join("identity").join("node.der"))?,
            ));
            if groups.is_none() {
                groups = Some(Groups::read(&config)?);
            }
        }
        let authority = std::fs::read(
            founders
                .first()
                .map(|(_, dir)| dir.join("identity").join("authority.der"))
                .ok_or_else(|| unread("no founder"))?,
        )?;
        Ok(Self {
            id,
            certificate,
            key: key.pkcs8().to_vec(),
            authority,
            founders: known,
            groups: groups.ok_or_else(|| unread("no founder's ranges"))?,
        })
    }

    /// What the gateway's reader states of itself for its transport's bounds, as the range
    /// client states it (`mantle_node::client::Client::open`): the founders, one read out at a
    /// time, a gateway's one chunk, `transport_bytes` held at once and the cell's `messages`.
    fn stated(&self, messages: Bounds, transport_bytes: u64) -> Stated {
        Stated {
            peers: self.founders.len(),
            ranges: 1,
            inflight: 0,
            transfers: 1,
            transport_bytes,
            messages,
            max_head: mantle_node::node::MAX_HEAD,
        }
    }

    /// The sockets the gateway's reader holds: its endpoint's, and one for each connection its
    /// admission takes where the platform gives a connection a socket of its own, as
    /// `mantle_client::sockets` counts a client's.
    pub fn sockets(&self, messages: Bounds) -> Result<usize, TransferError> {
        let least = mantle_protocol::bounds::least_transport(&self.stated(messages, 0))
            .map_err(|e| unread(&e.to_string()))?;
        mantle_protocol::bounds::limits(&self.stated(messages, least))
            .map_err(|e| unread(&e.to_string()))?
            .admission
            .connections
            .checked_add(1)
            .ok_or_else(|| unread("the gateway's sockets"))
    }

    /// A range client's settings for the gateway, stating `messages`, the cell's bounds. Its
    /// transport holds three of the client's pieces a founder, as the cell tests' range readers
    /// hold, and no less than its shape's least: a reply is read a piece at a time from the bytes
    /// its windows share (`mantle_node::client`'s `read_reply`), and at the least, 242,449 bytes
    /// for a cell of three where a piece is 65,527, a reply in one window left no room for the
    /// piece it is read into on CI's busy runner (`Budget`, run 40589088028).
    pub fn settings(&self, messages: Bounds) -> Result<ClientSettings, TransferError> {
        let least = mantle_protocol::bounds::least_transport(&self.stated(messages, 0))
            .map_err(|e| unread(&e.to_string()))?;
        let pieces = u64::try_from(self.founders.len())
            .ok()
            .and_then(|founders| founders.checked_mul(READER_PIECES))
            .and_then(|pieces| pieces.checked_mul(u64::try_from(mantle_client::PIECE).ok()?))
            .ok_or_else(|| unread("the reader's transport"))?;
        let transport_bytes = least.max(pieces);
        Ok(ClientSettings {
            id: self.id,
            bind: SocketAddr::from(([127, 0, 0, 1], 0)),
            credentials: Credentials {
                chain: vec![CertificateDer::from(self.certificate.clone())],
                key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.key.clone())),
                roots: vec![CertificateDer::from(self.authority.clone())],
            },
            nodes: self
                .founders
                .iter()
                .map(|(id, certificate)| Peer {
                    id: *id,
                    role: Role::Node,
                    certificate: certificate.clone(),
                })
                .collect(),
            role: Role::Gateway,
            messages,
            transport_bytes,
            seed: None,
        })
    }
}

/// The gateway's reader of the cell's metadata: a range client, the volume table read once, each
/// volume's node.
pub struct Reader {
    client: Client,
    groups: Groups,
    /// The most extents a read asks (`mantle_node::read::extents_page`).
    page: u32,
    node_of: BTreeMap<u128, u64>,
}

fn failed(e: mantle_node::client::ClientError) -> TransferError {
    unread(&e.to_string())
}

impl Reader {
    /// A reader of `settings`, told where the founders listen (`bound`), the volume table read a
    /// page at a time: at most a page each of the cell's `volumes` and one past the last.
    pub async fn open(
        settings: ClientSettings,
        (bound, volumes): (Vec<(u64, SocketAddr)>, usize),
        (groups, page): (Groups, u32),
    ) -> Result<Self, TransferError> {
        let mut client = Client::open(settings).await.map_err(failed)?;
        for (node, quic) in &bound {
            client.learn(*node, *quic).map_err(failed)?;
        }
        let mut node_of = BTreeMap::new();
        let mut after = None;
        for _ in 0..=volumes {
            let read = client
                .read(groups.root, &Query::Volumes { after }, u64::MAX)
                .await
                .map_err(failed)?;
            let Read::Volumes { volumes, more } = read else {
                return Err(unread("the volume table"));
            };
            after = volumes.last().map(|p| p.volume);
            node_of.extend(volumes.into_iter().map(|p| (p.volume, p.node)));
            if !more {
                break;
            }
        }
        Ok(Self {
            client,
            groups,
            page: page.max(1),
            node_of,
        })
    }

    /// The control of the placement check: `key`'s first block, one of its chunks moved onto
    /// another's node in the volume table as read, judged as every block is. Whether the check
    /// counts it, as it must; none for an object of no block of two chunks or more. A completed
    /// upload's first block is its first part's, each part file followed once.
    pub async fn moved(&mut self, bucket: &str, key: &str) -> Result<Option<bool>, TransferError> {
        let read = self
            .client
            .read(
                self.groups.name,
                &Query::Current {
                    bucket: bucket.to_owned(),
                    key: key.to_owned(),
                },
                u64::MAX,
            )
            .await
            .map_err(failed)?;
        let Read::Current(Some((_, version))) = read else {
            return Err(unread(&format!("{key}'s current version")));
        };
        let mut file = version.file;
        for _ in 0..=mantle_s3::body::MAX_PARTS {
            let Some(at) = file else {
                return Ok(None);
            };
            let read = self
                .client
                .read(
                    self.groups.file,
                    &Query::Extents {
                        file: at,
                        offset: 0,
                        max: 1,
                    },
                    u64::MAX,
                )
                .await
                .map_err(failed)?;
            let Read::Extents(extents) = read else {
                return Err(unread(&format!("{key}'s extents")));
            };
            match extents.first().map(|(_, extent)| extent.target) {
                None => return Ok(None),
                Some(Target::File(part)) => file = Some(part),
                Some(Target::Block(block)) => {
                    let read = self
                        .client
                        .read(self.groups.block, &Query::Block { block }, u64::MAX)
                        .await
                        .map_err(failed)?;
                    let Read::Block(Some((_, places))) = read else {
                        return Err(unread(&format!("{key}'s block {block:032x}")));
                    };
                    let volumes: Vec<u128> = places.iter().map(|p| p.volume).collect();
                    let (Some(first), Some(second)) = (volumes.first(), volumes.get(1)) else {
                        return Ok(None);
                    };
                    let mut moved = self.node_of.clone();
                    if let Some(&node) = self.node_of.get(first) {
                        moved.insert(*second, node);
                    }
                    return Ok(Some(!distinct(&volumes, &moved)));
                }
            }
        }
        Err(unread(&format!("{key}'s files past S3's parts")))
    }

    /// Where the blocks of `key` in `bucket`, `length` bytes, sit: its blocks, and those two of
    /// whose chunks share a node. Its file's extents are read a page at a time, and a completed
    /// upload's part files after it, each once.
    pub async fn object(
        &mut self,
        bucket: &str,
        (key, length): (&str, u64),
    ) -> Result<(u64, u64), TransferError> {
        let read = self
            .client
            .read(
                self.groups.name,
                &Query::Current {
                    bucket: bucket.to_owned(),
                    key: key.to_owned(),
                },
                u64::MAX,
            )
            .await
            .map_err(failed)?;
        let Read::Current(Some((_, version))) = read else {
            return Err(unread(&format!("{key}'s current version")));
        };
        let Some(file) = version.file else {
            return Ok((0, 0));
        };
        let (mut blocks, mut shared) = (0u64, 0u64);
        // A file's extents are its blocks or, a completed upload's, its parts' files: at most
        // S3's parts and the object's own file, each read once.
        let mut files = VecDeque::from([file]);
        let mut walked = 0u64;
        // Pages enough for an extent a byte of the object, and the read past its end.
        let pages = length
            .checked_div(u64::from(self.page))
            .unwrap_or(0)
            .saturating_add(2);
        while let Some(file) = files.pop_front() {
            walked = walked.saturating_add(1);
            if walked > u64::from(mantle_s3::body::MAX_PARTS).saturating_add(1) {
                return Err(unread(&format!("{key}'s files past S3's parts")));
            }
            let mut offset = 0u64;
            for _ in 0..pages {
                let read = self
                    .client
                    .read(
                        self.groups.file,
                        &Query::Extents {
                            file,
                            offset,
                            max: self.page,
                        },
                        u64::MAX,
                    )
                    .await
                    .map_err(failed)?;
                let Read::Extents(extents) = read else {
                    return Err(unread(&format!("{key}'s extents")));
                };
                let last = extents.len() < usize::try_from(self.page).unwrap_or(usize::MAX);
                for (start, extent) in extents {
                    offset = start.saturating_add(extent.length);
                    match extent.target {
                        Target::File(part) => files.push_back(part),
                        Target::Block(block) => {
                            let read = self
                                .client
                                .read(self.groups.block, &Query::Block { block }, u64::MAX)
                                .await
                                .map_err(failed)?;
                            let Read::Block(Some((_, places))) = read else {
                                return Err(unread(&format!("{key}'s block {block:032x}")));
                            };
                            let volumes: Vec<u128> = places.iter().map(|p| p.volume).collect();
                            blocks = blocks.saturating_add(1);
                            if !distinct(&volumes, &self.node_of) {
                                shared = shared.saturating_add(1);
                            }
                        }
                    }
                }
                if last {
                    break;
                }
            }
        }
        Ok((blocks, shared))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The control: a block whose chunk moved onto its block's other node is not on distinct
    /// nodes, nor is one whose volume the table does not name; three volumes of three nodes are.
    #[test]
    fn a_chunk_moved_onto_its_blocks_other_node_is_caught() {
        let node_of = BTreeMap::from([(1u128, 10u64), (2, 20), (3, 30), (4, 20)]);
        assert!(distinct(&[1, 2, 3], &node_of));
        assert!(!distinct(&[1, 2, 4], &node_of));
        assert!(!distinct(&[1, 2, 9], &node_of));
    }
}
