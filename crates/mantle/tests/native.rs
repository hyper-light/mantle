//! The native protocol's client against a cell of three that `mantle init` made, each founder a
//! `mantle serve` process on loopback, pinning its nodes and nothing else: a client `mantle init
//! client` made asks S3's bucket, object and multipart operations through the library
//! (`mantle_client`) and through `mantle`'s object commands, its puts streamed with their
//! CRC-64/NVME trailers and its gets held to the checksums kept; a client whose certificate another
//! authority issued, or whose certificate is the cell's but not a client's, is refused at its
//! handshake (node.md §3.6).
//!
//! A binary of its own: a process states its message bounds once, and the client here states a
//! client's (`mantle_client`), opening no node's client.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

#[allow(dead_code, unused_imports)]
#[path = "support/cell.rs"]
mod cell;
#[allow(dead_code)]
#[path = "../../node/tests/support/history.rs"]
mod history;
#[allow(dead_code)]
#[path = "../../range/tests/support/linear.rs"]
mod linear;
#[allow(dead_code)]
#[path = "../../node/tests/support/relay.rs"]
mod relay;
use cell::*;
use std::process::{Command, Stdio};

use mantle_client::{Client, ClientError, Settings, Sink, Source, TRAILER};
use mantle_protocol::object::{Headers, List as ListOp, Listing, Op, Reply, Status};

/// Bytes `n` of the test's objects: each a function of its object and its place, so a byte moved,
/// dropped or another object's shows.
fn bytes(n: u64, len: usize) -> Vec<u8> {
    (0..len as u64)
        .map(|at| (at.wrapping_mul(131).wrapping_add(n.wrapping_mul(7)) % 251) as u8)
        .collect()
}

/// A client's settings from the directory `mantle init client` made, each node at the address it
/// bound.
fn settings(dir: &Path, bound: &[(u64, SocketAddr)]) -> Settings {
    let text = std::fs::read_to_string(dir.join("config")).unwrap();
    let value = |key: &str| {
        text.lines()
            .find_map(|l| l.strip_prefix(&format!("{key} = ")))
            .unwrap()
            .to_owned()
    };
    let named: Vec<u64> = text
        .lines()
        .filter_map(|l| l.strip_prefix("node = "))
        .map(|l| l.split(' ').next().unwrap().parse().unwrap())
        .collect();
    assert_eq!(named.len(), bound.len(), "{text}");
    Settings {
        id: value("id").parse().unwrap(),
        bind: "127.0.0.1:0".parse().unwrap(),
        credentials: mantle_client::Credentials {
            chain: vec![mantle_client::CertificateDer::from(
                std::fs::read(dir.join(value("certificate"))).unwrap(),
            )],
            key: mantle_client::PrivateKeyDer::Pkcs8(mantle_client::PrivatePkcs8KeyDer::from(
                std::fs::read(dir.join(value("key"))).unwrap(),
            )),
            roots: vec![mantle_client::CertificateDer::from(
                std::fs::read(dir.join(value("authority"))).unwrap(),
            )],
        },
        nodes: bound.to_vec(),
        transport_bytes: mantle_client::least_transport(bound.len()).unwrap(),
    }
}

/// The client's directory's config, each `node` line naming where its node bound.
fn point_at(dir: &Path, bound: &[(u64, SocketAddr)]) {
    let path = dir.join("config");
    let text = std::fs::read_to_string(&path).unwrap();
    let mut kept: String = text
        .lines()
        .filter(|l| !l.starts_with("node = "))
        .map(|l| format!("{l}\n"))
        .collect();
    for (id, quic) in bound {
        kept.push_str(&format!("node = {id} {quic}\n"));
    }
    std::fs::write(&path, kept).unwrap();
}

/// What `mantle --client DIR ARGS` said, its input `input` where given; refused unless it exited
/// well.
fn mantle(dir: &Path, args: &[&str], input: Option<&[u8]>) -> Vec<u8> {
    let (status, out, err) = mantle_status(dir, args, input);
    assert!(
        status,
        "mantle {args:?}: {}{}",
        String::from_utf8_lossy(&out),
        String::from_utf8_lossy(&err)
    );
    out
}

fn mantle_status(dir: &Path, args: &[&str], input: Option<&[u8]>) -> (bool, Vec<u8>, Vec<u8>) {
    let mut child = Command::new(NODE)
        .arg("--client")
        .arg(dir)
        .args(args)
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    if let Some(input) = input {
        let mut stdin = child.stdin.take().unwrap();
        stdin.write_all(input).unwrap();
    }
    let output = child.wait_with_output().unwrap();
    (output.status.success(), output.stdout, output.stderr)
}

/// The account the cell's clients act for, as `mantle init` kept it beside the authority.
fn account(cell: &Cell) -> String {
    let text = std::fs::read_to_string(cell.out().join("authority").join("cell")).unwrap();
    text.lines()
        .find_map(|l| l.strip_prefix("account = "))
        .unwrap()
        .to_owned()
}

/// Every founder's configuration pins its peers as nodes, and nothing else.
fn pins_nodes_alone(cell: &Cell) {
    for config in cell.made.values() {
        let text = std::fs::read_to_string(config).unwrap();
        let peers: Vec<&str> = text.lines().filter(|l| l.starts_with("peer")).collect();
        assert_eq!(peers.len(), 2, "{text}");
        assert!(peers.iter().all(|l| l.contains(" node ")), "{text}");
    }
}

/// A client made by `mantle init client` in the cell's root.
fn made_client(cell: &Cell, name: &str) -> PathBuf {
    let dir = cell.out().parent().unwrap().join(name);
    let made = Command::new(NODE)
        .args(["init", "client"])
        .arg(&dir)
        .arg("--cell")
        .arg(cell.out())
        .output()
        .unwrap();
    assert!(
        made.status.success(),
        "mantle init client: {}{}",
        String::from_utf8_lossy(&made.stdout),
        String::from_utf8_lossy(&made.stderr)
    );
    let said = String::from_utf8(made.stdout).unwrap();
    assert!(said.starts_with("client "), "{said}");
    point_at(&dir, &cell.bound_quic());
    dir
}

/// The test's object bytes, read once.
struct Made<'a>(&'a [u8]);

impl Source for Made<'_> {
    async fn read(&mut self, into: &mut [u8]) -> std::io::Result<usize> {
        let mut rest = self.0;
        let n = rest.read(into).await?;
        self.0 = rest;
        Ok(n)
    }
}

/// A get's bytes, kept.
struct Kept(Vec<u8>);

impl Sink for Kept {
    async fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.0.extend_from_slice(bytes);
        Ok(())
    }
}

fn done(what: &str, reply: Result<Reply, ClientError>) -> Reply {
    let reply = reply.unwrap_or_else(|e| panic!("{what}: {e}"));
    assert_eq!(reply.status, Status::Done, "{what}: {reply:?}");
    reply
}

fn put(bucket: &str, key: &str, length: u64) -> Op {
    Op::Put {
        bucket: bucket.into(),
        key: key.into(),
        id: mantle_client::identity().unwrap(),
        length,
        headers: Headers::default(),
        checksum: None,
        md5: None,
        trailing: Some(TRAILER),
    }
}

fn get(bucket: &str, key: &str, range: Option<&str>) -> Op {
    Op::Get {
        bucket: bucket.into(),
        key: key.into(),
        headers: Headers {
            range: range.map(Into::into),
            ..Headers::default()
        },
    }
}

/// S3's operations through the library: buckets made, headed, listed and versioned; an object put
/// in one stream and got whole and by range; a multipart upload of two parts completed and got
/// whole, its checksum one full-object value; another aborted; objects listed under the account and
/// removed. What the library said, for the CLI's half to find.
async fn through_the_library(mut client: Client, account: String) {
    let call = async |client: &mut Client, op: Op| client.call(&op, u64::MAX).await;
    done(
        "CreateBucket",
        call(
            &mut client,
            Op::Create {
                bucket: "native".into(),
                id: mantle_client::identity().unwrap(),
            },
        )
        .await,
    );
    done(
        "HeadBucket",
        call(
            &mut client,
            Op::HeadBucket {
                bucket: "native".into(),
            },
        )
        .await,
    );
    let listed = done(
        "ListBuckets",
        call(
            &mut client,
            Op::ListBuckets {
                prefix: String::new(),
                after: None,
                max: 100,
            },
        )
        .await,
    );
    let Some(Listing::Buckets(page)) = listed.listing else {
        panic!("ListBuckets: {listed:?}");
    };
    assert!(page.buckets.iter().any(|(b, _)| b == "native"));
    done(
        "PutBucketVersioning",
        call(
            &mut client,
            Op::PutVersioning {
                bucket: "native".into(),
                id: mantle_client::identity().unwrap(),
                versioning: mantle_s3::body::Versioning::Enabled,
            },
        )
        .await,
    );
    let versioning = done(
        "GetBucketVersioning",
        call(
            &mut client,
            Op::GetVersioning {
                bucket: "native".into(),
            },
        )
        .await,
    );
    assert_eq!(
        versioning.versioning,
        Some(mantle_s3::body::Versioning::Enabled)
    );

    // An object streamed whole, then got whole and by a range.
    let object = bytes(1, 3 << 20);
    let made = done(
        "PutObject",
        client
            .put(
                &put("native", "one/object", object.len() as u64),
                &mut Made(&object),
                u64::MAX,
            )
            .await,
    );
    let kept = made.checksum.clone().unwrap();
    assert_eq!((kept.algorithm, kept.parts), (TRAILER.code(), 0));
    let mut whole = Kept(Vec::new());
    let got = done(
        "GetObject",
        client
            .get(&get("native", "one/object", None), &mut whole, u64::MAX)
            .await,
    );
    assert_eq!(got.etag, made.etag);
    assert!(whole.0 == object, "the object's bytes came back otherwise");
    let mut ranged = Kept(Vec::new());
    let got = done(
        "GetObject of a range",
        client
            .get(
                &get("native", "one/object", Some("bytes=10-1033")),
                &mut ranged,
                u64::MAX,
            )
            .await,
    );
    assert_eq!(got.range, Some((10, 1033)));
    assert_eq!(ranged.0, object[10..=1033]);
    let head = done(
        "HeadObject",
        call(
            &mut client,
            Op::Head {
                bucket: "native".into(),
                key: "one/object".into(),
                headers: Headers::default(),
            },
        )
        .await,
    );
    assert_eq!(
        (head.size, head.checksum),
        (object.len() as u64, Some(kept))
    );

    // A multipart upload of a 5 MiB part and a last of 1 MiB, its parts' checksums combined as
    // one full-object CRC-64/NVME.
    let created = done(
        "CreateMultipartUpload",
        call(
            &mut client,
            Op::CreateUpload {
                bucket: "native".into(),
                key: "two/parts".into(),
                id: mantle_client::identity().unwrap(),
                checksum: Some((TRAILER, true)),
            },
        )
        .await,
    );
    let upload = created.upload.clone();
    let parts = [bytes(2, 5 << 20), bytes(3, 1 << 20)];
    let mut etags = Vec::new();
    for (number, part) in (1u16..).zip(&parts) {
        let op = Op::UploadPart {
            bucket: "native".into(),
            key: "two/parts".into(),
            upload: upload.clone(),
            number,
            id: mantle_client::identity().unwrap(),
            length: part.len() as u64,
            checksum: None,
            md5: None,
            trailing: Some(TRAILER),
        };
        let reply = done(
            "UploadPart",
            client.put(&op, &mut Made(part), u64::MAX).await,
        );
        etags.push((number, reply.etag));
    }
    let listed = done(
        "ListParts",
        call(
            &mut client,
            Op::ListParts {
                bucket: "native".into(),
                key: "two/parts".into(),
                upload: upload.clone(),
                after: 0,
                max: 1000,
            },
        )
        .await,
    );
    let Some(Listing::Parts(page)) = listed.listing else {
        panic!("ListParts: {listed:?}");
    };
    assert_eq!(page.parts.len(), 2);
    let completed = done(
        "CompleteMultipartUpload",
        call(
            &mut client,
            Op::Complete {
                bucket: "native".into(),
                key: "two/parts".into(),
                upload: upload.clone(),
                id: mantle_client::identity().unwrap(),
                headers: Headers::default(),
                parts: etags,
            },
        )
        .await,
    );
    assert!(completed.etag.ends_with("-2"), "{}", completed.etag);
    let whole: Vec<u8> = parts.concat();
    let mut kept = Kept(Vec::new());
    let got = done(
        "GetObject of the multipart object",
        client
            .get(&get("native", "two/parts", None), &mut kept, u64::MAX)
            .await,
    );
    assert_eq!(got.checksum.map(|c| c.parts), Some(0));
    assert!(
        kept.0 == whole,
        "the multipart object's bytes came back otherwise"
    );

    // Another upload aborted: none listed after.
    let aborted = done(
        "CreateMultipartUpload",
        call(
            &mut client,
            Op::CreateUpload {
                bucket: "native".into(),
                key: "aborted".into(),
                id: mantle_client::identity().unwrap(),
                checksum: Some((TRAILER, true)),
            },
        )
        .await,
    );
    done(
        "AbortMultipartUpload",
        call(
            &mut client,
            Op::Abort {
                bucket: "native".into(),
                key: "aborted".into(),
                upload: aborted.upload,
                id: mantle_client::identity().unwrap(),
            },
        )
        .await,
    );
    let uploads = done(
        "ListMultipartUploads",
        call(
            &mut client,
            Op::ListUploads {
                bucket: "native".into(),
                prefix: String::new(),
                delimiter: String::new(),
                key_marker: None,
                id_marker: None,
                max: 1000,
            },
        )
        .await,
    );
    let Some(Listing::Uploads(page)) = uploads.listing else {
        panic!("ListMultipartUploads: {uploads:?}");
    };
    assert!(page.uploads.is_empty(), "{page:?}");

    // The keys, listed with their owner: the cell's account, the client's principal.
    let listed = done(
        "ListObjectsV2",
        call(
            &mut client,
            Op::List(ListOp {
                bucket: "native".into(),
                prefix: String::new(),
                delimiter: String::new(),
                start: None,
                max_keys: 1000,
                fetch_owner: true,
            }),
        )
        .await,
    );
    let Some(Listing::Keys(page)) = listed.listing else {
        panic!("ListObjectsV2: {listed:?}");
    };
    let keys: Vec<&str> = page.contents.iter().map(|l| l.key.as_str()).collect();
    assert_eq!(keys, ["one/object", "two/parts"]);
    assert!(
        page.contents
            .iter()
            .all(|l| l.owner.as_deref() == Some(account.as_str())),
        "{page:?}"
    );
    // A removed object is gone.
    done(
        "DeleteObject",
        call(
            &mut client,
            Op::Delete {
                bucket: "native".into(),
                key: "one/object".into(),
                id: mantle_client::identity().unwrap(),
                headers: Headers::default(),
            },
        )
        .await,
    );
    let mut none = Kept(Vec::new());
    let gone = client
        .get(&get("native", "one/object", None), &mut none, u64::MAX)
        .await
        .unwrap();
    assert_eq!(gone.status, Status::NoSuchKey);
    assert!(none.0.is_empty());
}

/// S3's operations of a cell of three, through the library and through the command line, as a
/// client `mantle init client` made, which no node pins.
#[test]
fn the_library_and_the_command_line_ask_s3s_operations_of_a_cell_of_three() {
    let mut cell = Cell::made_alone(three_founders);
    pins_nodes_alone(&cell);
    let dir = made_client(&cell, "client");
    let bound = cell.bound_quic();
    let account = account(&cell);

    let settings = settings(&dir, &bound);
    let sockets = mantle_client::sockets(bound.len()).unwrap();
    let library = std::thread::spawn(move || {
        client_runtime(sockets)
            .block_on(async move {
                let client = Client::open(settings).await.unwrap();
                through_the_library(client, account).await;
            })
            .unwrap();
    });
    cell.until(|_| {
        if library.is_finished() {
            Some(())
        } else {
            None
        }
    });
    library.join().unwrap();

    // The command line, a process an operation.
    let said = |out: Vec<u8>| String::from_utf8(out).unwrap();
    assert!(said(mantle(&dir, &["mb", "s3://cli"], None)).contains("made s3://cli"));
    let buckets = said(mantle(&dir, &["ls"], None));
    assert!(
        buckets.contains(" cli") && buckets.contains(" native"),
        "{buckets}"
    );
    let root = cell.out().parent().unwrap().to_path_buf();
    let file = root.join("file.bin");
    let object = bytes(4, (3 << 20) + 17);
    std::fs::write(&file, &object).unwrap();
    let file_text = file.display().to_string();
    mantle(&dir, &["cp", &file_text, "s3://cli/from/file"], None);
    let back = root.join("back.bin");
    let back_text = back.display().to_string();
    mantle(&dir, &["cp", "s3://cli/from/file", &back_text], None);
    assert!(std::fs::read(&back).unwrap() == object);
    // Standard input past a part: put in parts of 8 MiB; and back on standard output.
    let streamed = bytes(5, (9 << 20) + 3);
    mantle(&dir, &["cp", "-", "s3://cli/from/stdin"], Some(&streamed));
    let out = mantle(&dir, &["cp", "s3://cli/from/stdin", "-"], None);
    assert!(out == streamed, "standard output held other bytes");
    let head = said(mantle(&dir, &["head", "s3://cli/from/stdin"], None));
    assert!(head.contains(&format!("size {}", streamed.len())), "{head}");
    assert!(head.contains("checksum CRC64NVME "), "{head}");
    let keys = said(mantle(&dir, &["ls", "s3://cli/from/"], None));
    assert!(
        keys.contains(&format!("{} from/file", object.len()))
            && keys.contains(&format!("{} from/stdin", streamed.len())),
        "{keys}"
    );
    assert!(said(mantle(&dir, &["ls", "s3://cli"], None)).contains("PRE from/"));

    // The multipart steps: a part from a file and the last from standard input.
    let upload = said(mantle(
        &dir,
        &["multipart", "create", "s3://cli/steps"],
        None,
    ));
    let upload = upload.trim();
    let first = bytes(6, 5 << 20);
    let first_file = root.join("first.bin");
    std::fs::write(&first_file, &first).unwrap();
    let one = said(mantle(
        &dir,
        &[
            "multipart",
            "put",
            "s3://cli/steps",
            upload,
            "1",
            &first_file.display().to_string(),
        ],
        None,
    ));
    let last = bytes(7, 4096);
    let two = said(mantle(
        &dir,
        &[
            "multipart",
            "put",
            "s3://cli/steps",
            upload,
            "2",
            "-",
            "--length",
            &last.len().to_string(),
        ],
        Some(&last),
    ));
    let parts = said(mantle(
        &dir,
        &["multipart", "parts", "s3://cli/steps", upload],
        None,
    ));
    assert_eq!(parts.lines().count(), 2, "{parts}");
    let uploads = said(mantle(&dir, &["multipart", "uploads", "s3://cli"], None));
    assert!(uploads.contains(upload), "{uploads}");
    let etag = |line: &str| line.split_whitespace().nth(1).unwrap().to_owned();
    let completed = said(mantle(
        &dir,
        &[
            "multipart",
            "complete",
            "s3://cli/steps",
            upload,
            &format!("1={}", etag(&one)),
            &format!("2={}", etag(&two)),
        ],
        None,
    ));
    assert!(completed.contains("-2\""), "{completed}");
    let out = mantle(&dir, &["cp", "s3://cli/steps", "-"], None);
    assert!(
        out == [first, last].concat(),
        "the parts came back otherwise"
    );
    let aborted = said(mantle(
        &dir,
        &["multipart", "create", "s3://cli/aborted"],
        None,
    ));
    mantle(
        &dir,
        &["multipart", "abort", "s3://cli/aborted", aborted.trim()],
        None,
    );

    // Removed: an object gone, a bucket refused while it holds any, removed once empty.
    for key in ["from/file", "from/stdin"] {
        mantle(&dir, &["rm", &format!("s3://cli/{key}")], None);
    }
    let (found, _, err) = mantle_status(&dir, &["head", "s3://cli/from/file"], None);
    assert!(!found && String::from_utf8_lossy(&err).contains("NoSuchKey"));
    let (removed, _, err) = mantle_status(&dir, &["rb", "s3://cli"], None);
    assert!(
        !removed && String::from_utf8_lossy(&err).contains("BucketNotEmpty"),
        "{}",
        String::from_utf8_lossy(&err)
    );
    mantle(&dir, &["rm", "s3://cli/steps"], None);
    mantle(&dir, &["rb", "s3://cli"], None);
    assert!(!said(mantle(&dir, &["ls"], None)).contains(" cli"));
}

/// A client whose certificate another authority issued is refused at its handshake, and so is
/// one whose certificate the cell's authority issued a node, not a client; neither is named by any
/// node's configuration, and the cell's own client is served.
#[test]
fn a_certificate_no_client_of_the_cells_holds_is_refused_at_its_handshake() {
    let mut cell = Cell::made_alone(three_founders);
    pins_nodes_alone(&cell);
    let dir = made_client(&cell, "client");
    let bound = cell.bound_quic();
    let ours = settings(&dir, &bound);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let validity = Validity {
        not_before: now - 3_600,
        not_after: now + 86_400,
    };
    let issued = |authority: &KeyPair, name: &str, node: bool| {
        let own = KeyPair::generate().unwrap();
        let certificate = if node {
            mantle_cert::node(
                authority,
                name,
                own.public_key(),
                "client-77",
                (validity, Serial::draw().unwrap()),
            )
        } else {
            mantle_cert::client(
                authority,
                name,
                own.public_key(),
                ("client-77", &account(&cell)),
                (validity, Serial::draw().unwrap()),
            )
        }
        .unwrap();
        (certificate, own.pkcs8().to_vec())
    };
    // Another authority, its name the cell's own.
    let cell_name = std::fs::read_to_string(cell.out().join("authority").join("cell"))
        .unwrap()
        .lines()
        .find_map(|l| l.strip_prefix("authority = ").map(str::to_owned))
        .unwrap();
    let theirs = KeyPair::generate().unwrap();
    let foreign = issued(&theirs, &cell_name, false);
    // The cell's authority, a node's certificate.
    let authority = KeyPair::from_pkcs8(
        &std::fs::read(cell.out().join("authority").join("authority.key")).unwrap(),
    )
    .unwrap();
    let nodes = issued(&authority, &cell_name, true);
    let with = |(certificate, key): (Vec<u8>, Vec<u8>)| Settings {
        id: 77,
        bind: "127.0.0.1:0".parse().unwrap(),
        credentials: mantle_client::Credentials {
            chain: vec![mantle_client::CertificateDer::from(certificate)],
            key: mantle_client::PrivateKeyDer::Pkcs8(mantle_client::PrivatePkcs8KeyDer::from(key)),
            roots: ours.credentials.roots.clone(),
        },
        nodes: bound.clone(),
        transport_bytes: ours.transport_bytes,
    };
    let refused = [with(foreign), with(nodes)];
    let sockets = mantle_client::sockets(bound.len()).unwrap();
    let asked = std::thread::spawn(move || {
        client_runtime(sockets)
            .block_on(async move {
                let head = Op::HeadBucket {
                    bucket: "none".into(),
                };
                let mut answers = Vec::new();
                for settings in refused {
                    let mut client = Client::open(settings).await.unwrap();
                    answers.push(match client.call(&head, u64::MAX).await {
                        Err(ClientError::Refused(refusal)) => Some(refusal),
                        other => panic!("a stranger was served: {other:?}"),
                    });
                }
                let mut client = Client::open(ours).await.unwrap();
                let served = client.call(&head, u64::MAX).await.unwrap();
                (answers, served.status)
            })
            .unwrap()
    });
    cell.until(|_| if asked.is_finished() { Some(()) } else { None });
    let (answers, served) = asked.join().unwrap();
    assert_eq!(
        answers,
        [Some(mantle_client::Refusal::Identity); 2],
        "{answers:?}"
    );
    assert_eq!(served, Status::NoSuchBucket);
    pins_nodes_alone(&cell);
}
