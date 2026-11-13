//! A node's admission lets one identity hold `bounds::CONNECTIONS_PER_PEER` connections and replaces
//! one only once it is idle, quiet for the node's serve period (hyper-transport's admission): a
//! client whose identity's other connections carried traffic moments before is refused `Busy`, the
//! node taking none of its request. The client asks again once a held connection can have turned
//! idle, and a body its source can give again is read again for it.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::large_enum_variant
)]

use std::sync::mpsc::channel;

use mantle_client::{Client, Credentials, Settings, Source, TRAILER};
use mantle_protocol::bounds::CONNECTIONS_PER_PEER;
use mantle_protocol::object::{Headers, Op, Status};
use mantle_s3::body::MIN_PART;

#[allow(dead_code)]
mod support;
use support::{CLIENT, Made, NODE, Script, credentials, pattern, runtime, scripted};

/// The pattern's bytes, from their first again once asked: a file, say.
struct Again {
    at: u64,
    length: u64,
}

impl Source for Again {
    async fn read(&mut self, into: &mut [u8]) -> std::io::Result<usize> {
        let n = into.len().min((self.length - self.at) as usize);
        for byte in &mut into[..n] {
            *byte = pattern(self.at);
            self.at += 1;
        }
        Ok(n)
    }

    async fn again(&mut self) -> std::io::Result<bool> {
        self.at = 0;
        Ok(true)
    }
}

/// Another copy of `credentials`: a client's, the same identity.
fn copy(credentials: &Credentials) -> Credentials {
    Credentials {
        chain: credentials.chain.clone(),
        key: credentials.key.clone_key(),
        roots: credentials.roots.clone(),
    }
}

fn put(length: u64) -> Op {
    Op::Put {
        bucket: "b".into(),
        key: "k".into(),
        id: mantle_client::identity().unwrap(),
        length,
        headers: Headers::default(),
        checksum: None,
        md5: None,
        trailing: Some(TRAILER),
    }
}

/// Clients of one identity hold every connection the node lets it hold, each having carried a put
/// just now, as the processes of a user's script leave theirs: the next client of that identity is
/// refused `Busy` until one is idle, and its put of S3's least part, from a source it can read
/// again, is answered once it is.
#[test]
fn a_put_refused_busy_while_its_identity_holds_every_connection_is_answered_once_one_is_idle() {
    let (node, client) = credentials();
    let transport_bytes = mantle_client::least_transport(1).unwrap();
    let (tell, bound) = channel();
    // The holders' connections close as one is replaced or at the node's idle timeout, while the
    // stub goes on serving the asking client.
    let script = Script {
        outlive: u32::try_from(CONNECTIONS_PER_PEER).unwrap(),
        ..Script::default()
    };
    // The node funds every connection it holds at once: the identity's places, and the connection
    // that replaces one while the one it replaced drains.
    let node_bytes = mantle_client::least_transport(CONNECTIONS_PER_PEER + 1).unwrap();
    let served = std::thread::spawn(move || scripted(node, node_bytes, (0, None), script, tell));
    let address = bound.recv().unwrap();
    let sockets = mantle_client::sockets(1).unwrap() * (CONNECTIONS_PER_PEER + 1);
    let status = runtime(sockets)
        .block_on(async move {
            let open = || {
                Client::open(Settings {
                    id: CLIENT,
                    bind: "127.0.0.1:0".parse().unwrap(),
                    credentials: copy(&client),
                    nodes: vec![(NODE, address)],
                    names: Vec::new(),
                    transport_bytes,
                })
            };
            let mut holders = Vec::new();
            for _ in 0..CONNECTIONS_PER_PEER {
                let mut holder = open().await.unwrap();
                let length = 1;
                let reply = holder
                    .put(&put(length), &mut Made { at: 0, length }, u64::MAX)
                    .await
                    .unwrap();
                assert_eq!(reply.status, Status::Done);
                holders.push(holder);
            }
            let mut asking = open().await.unwrap();
            let length = MIN_PART;
            let reply = asking
                .put(&put(length), &mut Again { at: 0, length }, u64::MAX)
                .await;
            drop(asking);
            drop(holders);
            reply.map(|r| r.status)
        })
        .unwrap();
    assert_eq!(status.unwrap(), Status::Done);
    // The stub serves on until the process ends, telling each close it outlives on `bound`: which of
    // the holders' connections close, replaced or at the node's idle timeout, and whether a close a
    // newer connection supersedes is told at all, is the node's to decide, so neither the stub nor
    // its tellings are waited for: a thread of their own reads them until the stub ends.
    drop(served);
    std::thread::spawn(move || bound.iter().for_each(drop));
}
