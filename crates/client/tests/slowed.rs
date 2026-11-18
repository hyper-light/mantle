//! A part put its node answers `SlowDown` is asked again as botocore's standard mode asks a
//! throttling error again, within the client's attempts and retry quota: its body sent again from
//! its source's first byte, its operation's identity with it, where the source gives it again, as
//! botocore rewinds a body before each retry; and answered `SlowDown` where the source is read once.

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

use mantle_client::{ATTEMPTS, Client, Settings, Source, TRAILER};
use mantle_protocol::object::{Op, Reply, Status};
use mantle_s3::body::MIN_PART;

#[allow(dead_code)]
mod support;
use support::{
    Again, CLIENT, Made, NODE, Script, Seen, crc, credentials, runtime, scripted, serve,
};

/// A part of S3's least length, the pattern's first bytes from `source`, put through the stub as
/// `script` says: its reply, the identity it carried, and what the stub took.
fn put_part<S: Source + 'static>(script: Script, mut source: S) -> (Reply, u128, Seen) {
    let (node, client) = credentials();
    let transport_bytes = mantle_client::least_transport(1).unwrap();
    let (tell, bound) = channel();
    let served = serve(move |done| scripted(node, transport_bytes, (0, None), script, tell, done));
    let address = bound.recv().unwrap();
    let sockets = mantle_client::sockets(1).unwrap();
    let id = mantle_client::identity().unwrap();
    let reply = runtime(sockets)
        .block_on(async move {
            let mut client = Client::open(Settings {
                id: CLIENT,
                bind: "127.0.0.1:0".parse().unwrap(),
                credentials: client,
                nodes: vec![(NODE, address)],
                names: Vec::new(),
                transport_bytes,
            })
            .await
            .unwrap();
            let part = Op::UploadPart {
                bucket: "b".into(),
                key: "k".into(),
                upload: "upload".into(),
                number: 1,
                id,
                length: MIN_PART,
                checksum: None,
                md5: None,
                trailing: Some(TRAILER),
            };
            client.put(&part, &mut source, u64::MAX).await.unwrap()
        })
        .unwrap();
    (reply, id, served.join().unwrap())
}

/// The part's first put is answered `SlowDown` once the node read its body whole, as a node whose
/// block found too few volumes answers it: the client sends it again with its identity, from the
/// source's first byte, and the node keeps the part whole, the checksum it kept that of the
/// pattern's bytes.
#[test]
fn a_part_slowed_once_is_sent_again_from_its_first_byte_with_its_identity() {
    let script = Script {
        slow_parts: 1,
        ..Script::default()
    };
    let (reply, id, seen) = put_part(
        script,
        Again(Made {
            at: 0,
            length: MIN_PART,
        }),
    );
    assert_eq!(reply.status, Status::Done);
    assert_eq!(reply.checksum.unwrap().value, crc(MIN_PART));
    assert_eq!(
        seen,
        Seen {
            parts: vec![id, id],
            ..Seen::default()
        }
    );
}

/// A part read once, as standard input is, cannot be sent again: its `SlowDown` is the answer, the
/// part put once.
#[test]
fn a_part_read_once_and_slowed_answers_slow_down() {
    let script = Script {
        slow_parts: 1,
        ..Script::default()
    };
    let (reply, id, seen) = put_part(
        script,
        Made {
            at: 0,
            length: MIN_PART,
        },
    );
    assert_eq!(reply.status, Status::SlowDown);
    assert_eq!(
        seen,
        Seen {
            parts: vec![id],
            ..Seen::default()
        }
    );
}

/// Every attempt is answered `SlowDown`: the part is put the standard mode's three times
/// (`ATTEMPTS`), each with its identity, and answers `SlowDown`.
#[test]
fn a_part_slowed_at_every_attempt_answers_slow_down() {
    let script = Script {
        slow_parts: ATTEMPTS,
        ..Script::default()
    };
    let (reply, id, seen) = put_part(
        script,
        Again(Made {
            at: 0,
            length: MIN_PART,
        }),
    );
    assert_eq!(reply.status, Status::SlowDown);
    assert_eq!(
        seen,
        Seen {
            parts: vec![id; ATTEMPTS as usize],
            ..Seen::default()
        }
    );
}
