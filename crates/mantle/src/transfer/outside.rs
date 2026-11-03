//! A cell running elsewhere, for `mantle bench transfer --cell` (plan step 11; docs/design/
//! kubernetes.md): made by `mantle init` with the harness's gateway pinned in each founder before it
//! first started (`mantle init gateway`), its founders run by another, a Kubernetes StatefulSet, and
//! each reached at the name its cell file states, resolved once here as each founder resolves its
//! peers (research/78). The harness starts, kills and counts the chunks of none of its nodes: its
//! operator disrupts it (`--disrupt`).

use std::net::{SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};

use mantle_node::process::Endpoint;

use super::TransferError;
use super::cell::{FOUNDERS, Reach};
use super::place::Gateway;

fn unread(what: String) -> TransferError {
    TransferError::Io(std::io::Error::other(what))
}

/// A running cell of three that this runner did not make.
pub struct Outside {
    out: PathBuf,
    gateway: Gateway,
    quic: Vec<(u64, SocketAddr)>,
    s3: Vec<(String, String)>,
    bind: SocketAddr,
    /// The bytes of the first founder's first volume, as its configuration states them.
    volume_bytes: u64,
}

impl Outside {
    /// The cell `mantle init` made in `out`, its gateway pinned: each founder's QUIC endpoint at
    /// the name the cell file states, resolved now, and its S3 listener at that host on the port
    /// its configuration binds, signing for its region.
    pub fn open(out: &Path) -> Result<Self, TransferError> {
        let nodes = crate::init::Authority::read(out)
            .map_err(TransferError::Init)?
            .nodes;
        if nodes.len() != FOUNDERS {
            return Err(unread(format!(
                "{}: {} founders; the harness judges a cell of {FOUNDERS}",
                out.display(),
                nodes.len()
            )));
        }
        let mut quic = Vec::with_capacity(nodes.len());
        let mut s3 = Vec::with_capacity(nodes.len());
        let mut volume_bytes = None;
        for (id, reach) in &nodes {
            let at = Endpoint::parse(reach)
                .ok_or_else(|| unread(format!("node {id}'s endpoint {reach}")))?;
            let address = (at.host.as_str(), at.port)
                .to_socket_addrs()?
                .next()
                .ok_or_else(|| unread(format!("{reach} resolves to no address")))?;
            quic.push((*id, address));
            let config = crate::init::founder_dir(out, *id, nodes.len()).join("config");
            let stated = mantle_node::process::read_stated(&config)
                .map_err(|e| unread(format!("{}: {e}", config.display())))?;
            let listener = stated
                .s3
                .ok_or_else(|| unread(format!("{}: no S3 endpoint", config.display())))?;
            let endpoint = Endpoint {
                host: at.host,
                port: listener.listen.port(),
            };
            s3.push((endpoint.to_string(), listener.region));
            if volume_bytes.is_none() {
                volume_bytes = stated.volumes.first().map(|(_, bytes)| *bytes);
            }
        }
        let bind = match quic.first() {
            Some((_, address)) if address.is_ipv6() => SocketAddr::from(([0u16; 8], 0)),
            _ => SocketAddr::from(([0u8; 4], 0)),
        };
        Ok(Self {
            out: out.to_path_buf(),
            gateway: Gateway::open(out, bind)?,
            quic,
            s3,
            bind,
            volume_bytes: volume_bytes
                .ok_or_else(|| unread(format!("{}: no founder keeps a volume", out.display())))?,
        })
    }

    /// The bytes of a founder's volume, as the first founder's configuration states them.
    pub fn volume_bytes(&self) -> u64 {
        self.volume_bytes
    }
}

impl Reach for Outside {
    fn out(&self) -> &Path {
        &self.out
    }

    fn gateway(&self) -> &Gateway {
        &self.gateway
    }

    fn quic(&self) -> Vec<(u64, SocketAddr)> {
        self.quic.clone()
    }

    fn s3(&self) -> Vec<(String, String)> {
        self.s3.clone()
    }

    fn bind(&self) -> SocketAddr {
        self.bind
    }
}
