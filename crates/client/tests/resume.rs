//! A native upload whose run is lost resumes from the node's offset, asking for it again where the
//! node holds that read past the client's period, as a node whose range lost its leader with the
//! node that died under the run holds it until another leads.

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

use mantle_client::{Client, Native, Rewind, Settings, TRAILER, Watch};
use mantle_protocol::object::{Headers, Progress, Status};
use mantle_s3::body::MIN_PART;

#[allow(dead_code)]
mod support;
use support::{CLIENT, Made, NODE, Script, Seen, credentials, runtime, scripted};

impl Rewind for Made {
    async fn rewind(&mut self, offset: u64) -> std::io::Result<()> {
        self.at = offset;
        Ok(())
    }
}

/// The offsets the client said the upload stood at.
#[derive(Default)]
struct Watched(Vec<u64>);

impl Watch for Watched {
    async fn opened(&mut self, _: &str) -> std::io::Result<()> {
        Ok(())
    }

    async fn progressed(&mut self, progress: &Progress) -> std::io::Result<()> {
        self.0.push(progress.offset);
        Ok(())
    }
}

/// Run 2 is read whole and never answered, and the first Offset the client asks after it is held
/// until the client gives up on it: the second is answered at run 1's end, run 2 is sent again
/// from there, and the object commits whole, run 1 sent once.
#[test]
fn a_lost_runs_offset_held_past_the_clients_period_is_asked_again_and_the_upload_resumes() {
    let (node, client) = credentials();
    let transport_bytes = mantle_client::least_transport(1).unwrap();
    let script = Script {
        lose_append: Some(2),
        hold_offset: Some(1),
    };
    let (tell, bound) = channel();
    let served =
        std::thread::spawn(move || scripted(node, transport_bytes, (0, None), script, tell));
    let address = bound.recv().unwrap();
    let length = 2 * MIN_PART + (64 << 10);
    let sockets = mantle_client::sockets(1).unwrap();
    let (reply, watched) = runtime(sockets)
        .block_on(async move {
            let mut client = Client::open(Settings {
                id: CLIENT,
                bind: "127.0.0.1:0".parse().unwrap(),
                credentials: client,
                nodes: vec![(NODE, address)],
                transport_bytes,
            })
            .await
            .unwrap();
            let native = Native {
                bucket: "b".into(),
                key: "k".into(),
                id: mantle_client::identity().unwrap(),
                upload: None,
                checksum: TRAILER,
                headers: Headers::default(),
            };
            let mut source = Made { at: 0, length };
            let mut watched = Watched::default();
            let reply = client
                .upload(&native, (&mut source, length), &mut watched, u64::MAX)
                .await
                .unwrap();
            (reply, watched)
        })
        .unwrap();
    assert_eq!(reply.status, Status::Done);
    assert_eq!(
        served.join().unwrap(),
        Seen {
            opens: 1,
            appends: 4,
            offsets: 2,
            finishes: 1,
        }
    );
    assert_eq!(watched.0, [0, MIN_PART, 2 * MIN_PART, length]);
}
