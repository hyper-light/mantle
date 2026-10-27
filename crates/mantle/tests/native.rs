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
        // A command that ended before reading all of it closed the pipe: what it said tells why.
        if let Err(e) = stdin.write_all(input) {
            assert_eq!(e.kind(), std::io::ErrorKind::BrokenPipe, "{e}");
        }
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

/// A copy from standard input of `size` bytes into `s3://BUCKET/KEY`, a `mantle cp` process whose
/// input the test writes and whose lines on standard error it reads as they come.
struct Copying {
    child: std::process::Child,
    input: Option<std::process::ChildStdin>,
    lines: std::sync::mpsc::Receiver<String>,
    said: Vec<String>,
}

/// Where the copies of the resumable tests go.
const RESUMED: &str = "resumed";

impl Copying {
    fn start(dir: &Path, key: &str, size: usize) -> Self {
        let mut child = Command::new(NODE)
            .arg("--client")
            .arg(dir)
            .args(["cp", "-", &format!("s3://{RESUMED}/{key}")])
            .args(["--size", &size.to_string()])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let input = child.stdin.take();
        let err = child.stderr.take().unwrap();
        let (tell, lines) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(err).lines() {
                let Ok(line) = line else { return };
                if tell.send(line).is_err() {
                    return;
                }
            }
        });
        Self {
            child,
            input,
            lines,
            said: Vec::new(),
        }
    }

    /// Writes `bytes` to its input: returned once the copy read all but what the pipe holds. A
    /// copy that ended before reading them closed the pipe: what it said, at its finish, tells why.
    fn write(&mut self, bytes: &[u8]) {
        if let Err(e) = self.input.as_mut().unwrap().write_all(bytes) {
            assert_eq!(e.kind(), std::io::ErrorKind::BrokenPipe, "{e}");
        }
    }

    /// Takes its lines until one ends with `ending`, the cell's nodes heard meanwhile.
    fn until_said(&mut self, cell: &mut Cell, ending: &str) {
        cell.until(|_| {
            while let Ok(line) = self.lines.try_recv() {
                self.said.push(line);
            }
            if self.said.iter().any(|l| l.ends_with(ending)) {
                Some(())
            } else {
                None
            }
        });
    }

    /// Killed with SIGKILL: nothing of it runs past this.
    fn kill(mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }

    /// Its input closed, its end: whether it exited well, what it said on standard output and
    /// every line on standard error.
    fn finish(mut self, cell: &mut Cell) -> (bool, String, Vec<String>) {
        drop(self.input.take());
        let mut out = self.child.stdout.take().unwrap();
        let read = std::thread::spawn(move || {
            let mut said = String::new();
            std::io::Read::read_to_string(&mut out, &mut said).unwrap();
            said
        });
        let status = cell.until(|_| self.child.try_wait().unwrap());
        while let Ok(line) = self.lines.recv() {
            self.said.push(line);
        }
        (status.success(), read.join().unwrap(), self.said)
    }
}

/// The runs of the open upload of `key` as the cell holds them: their sizes, in order, by the
/// command line's own listings.
fn runs_held(dir: &Path, key: &str) -> Vec<u64> {
    let said = |out: Vec<u8>| String::from_utf8(out).unwrap();
    let uploads = said(mantle(
        dir,
        &["multipart", "uploads", &format!("s3://{RESUMED}")],
        None,
    ));
    // Each upload a line: its initiation, its ID and its key.
    let upload = uploads
        .lines()
        .map(|l| l.split_whitespace().collect::<Vec<_>>())
        .find(|words| words.get(2) == Some(&key))
        .map(|words| words[1].to_owned())
        .unwrap_or_else(|| panic!("no open upload of {key}: {uploads}"));
    let parts = said(mantle(
        dir,
        &[
            "multipart",
            "parts",
            &format!("s3://{RESUMED}/{key}"),
            &upload,
        ],
        None,
    ));
    // Each part a line: its number, its size and its ETag.
    parts
        .lines()
        .map(|l| l.split_whitespace().nth(1).unwrap().parse().unwrap())
        .collect()
}

/// The runs a copy said it committed, in order.
fn committed(said: &[String]) -> Vec<u16> {
    said.iter()
        .filter_map(|l| l.split(": run ").nth(1))
        .map(|l| l.split(' ').next().unwrap().parse().unwrap())
        .collect()
}

/// `key` holds `object`, read back whole, its ETag the MD5 of its bytes, as one PUT's is.
fn holds(dir: &Path, key: &str, object: &[u8]) {
    let got = mantle(dir, &["cp", &format!("s3://{RESUMED}/{key}"), "-"], None);
    assert!(got == object, "{key} came back otherwise");
    let head = String::from_utf8(mantle(
        dir,
        &["head", &format!("s3://{RESUMED}/{key}")],
        None,
    ))
    .unwrap();
    let md5 = mantle_s3::checksum::checksum(mantle_s3::checksum::Algorithm::Md5, object).unwrap();
    assert!(
        head.contains(&mantle_s3::checksum::etag(
            md5.bytes.as_slice().try_into().unwrap()
        )),
        "{head}"
    );
}

/// A copy resumed after a kill: it said it resumed at `offset` after `runs` runs, then committed
/// every run after them and none before, and its object holds `object` and its MD5 as its ETag.
fn resumed_whole(
    dir: &Path,
    key: &str,
    (offset, runs, total): (u64, u16, u16),
    said: &[String],
    object: &[u8],
) {
    let first = said
        .iter()
        .position(|l| l.contains("resuming upload "))
        .unwrap_or_else(|| panic!("no resume: {said:?}"));
    assert!(
        said[first].ends_with(&format!(" at {offset} bytes, {runs} runs")),
        "{said:?}"
    );
    assert_eq!(
        committed(&said[first..]),
        (runs + 1..=total).collect::<Vec<_>>(),
        "{said:?}"
    );
    holds(dir, key, object);
}

/// A native upload streamed from standard input in runs the node cuts (gateway.md §2.1): a copy
/// SIGKILLed once a run committed, and one SIGKILLed with a run half sent, each run again resumes
/// from the cell's durable offset, its committed runs not sent again, and its object is the bytes
/// whole, its ETag their MD5; a copy resumed from a source changed below that offset ends in
/// `BadDigest`, and nothing commits.
#[test]
fn a_copy_killed_at_a_run_boundary_or_mid_run_resumes_and_a_changed_source_is_refused() {
    let mut cell = Cell::made_alone(three_founders);
    let dir = made_client(&cell, "client");
    mantle(&dir, &["mb", &format!("s3://{RESUMED}")], None);
    // With its length stated, a copy's runs are S3's least part.
    let run = 5usize << 20;
    let size = 4 * run + (1 << 20);
    let total = 5u16;

    // A file of several runs, each read again from its offset: committed whole, run by run.
    let file = cell.out().parent().unwrap().join("runs.bin");
    let object = bytes(10, 3 * run + 77);
    std::fs::write(&file, &object).unwrap();
    let (done, out, err) = mantle_status(
        &dir,
        &[
            "cp",
            &file.display().to_string(),
            &format!("s3://{RESUMED}/file"),
        ],
        None,
    );
    let said: Vec<String> = String::from_utf8_lossy(&err)
        .lines()
        .map(str::to_owned)
        .collect();
    assert!(done, "{}{said:?}", String::from_utf8_lossy(&out));
    assert_eq!(committed(&said), [1, 2, 3, 4], "{said:?}");
    holds(&dir, "file", &object);

    // Killed once its second run committed, waiting on its input for the third.
    let object = bytes(11, size);
    let mut copying = Copying::start(&dir, "boundary", size);
    copying.write(&object[..2 * run]);
    copying.until_said(
        &mut cell,
        &format!("run 2 committed, {} bytes durable", 2 * run),
    );
    copying.kill();
    assert_eq!(runs_held(&dir, "boundary"), [run as u64; 2]);
    let mut again = Copying::start(&dir, "boundary", size);
    again.write(&object);
    let (done, out, said) = again.finish(&mut cell);
    assert!(done, "{out}{said:?}");
    resumed_whole(
        &dir,
        "boundary",
        ((2 * run) as u64, 2, total),
        &said,
        &object,
    );

    // Killed with half its third run read and sent: the cell holds two runs, the half lost.
    let object = bytes(12, size);
    let mut copying = Copying::start(&dir, "mid", size);
    copying.write(&object[..2 * run + run / 2]);
    copying.until_said(
        &mut cell,
        &format!("run 2 committed, {} bytes durable", 2 * run),
    );
    copying.kill();
    assert_eq!(runs_held(&dir, "mid"), [run as u64; 2]);
    let mut again = Copying::start(&dir, "mid", size);
    again.write(&object);
    let (done, out, said) = again.finish(&mut cell);
    assert!(done, "{out}{said:?}");
    resumed_whole(&dir, "mid", ((2 * run) as u64, 2, total), &said, &object);

    // Resumed from a source whose first run changed: its runs are not the object it now is.
    let object = bytes(13, size);
    let mut copying = Copying::start(&dir, "changed", size);
    copying.write(&object[..2 * run]);
    copying.until_said(
        &mut cell,
        &format!("run 2 committed, {} bytes durable", 2 * run),
    );
    copying.kill();
    let mut changed = object.clone();
    changed[run / 2] ^= 0xff;
    let mut again = Copying::start(&dir, "changed", size);
    again.write(&changed);
    let (done, out, said) = again.finish(&mut cell);
    assert!(!done, "{out}{said:?}");
    assert!(said.iter().any(|l| l.contains("BadDigest")), "{said:?}");
    let (found, _, err) = mantle_status(&dir, &["head", &format!("s3://{RESUMED}/changed")], None);
    assert!(
        !found && String::from_utf8_lossy(&err).contains("NoSuchKey"),
        "{}",
        String::from_utf8_lossy(&err)
    );
    let uploads = String::from_utf8(mantle(
        &dir,
        &["multipart", "uploads", &format!("s3://{RESUMED}")],
        None,
    ))
    .unwrap();
    assert!(!uploads.contains("changed"), "{uploads}");
}

/// A node SIGKILLed while a copy's third run streams through it: with one node of three gone the
/// cell writes no block, every chunk on a volume of its own and none acknowledged short of every
/// one (CLAUDE.md §6), so the copy, having found the upload's offset through another node, ends
/// `SlowDown`, its journal keeping the upload and the cell its two runs; once the node is back, its
/// volume enlisted and every range caught up on it, the copy run again resumes from the cell's
/// offset, no committed run sent again, its object the bytes whole.
#[test]
fn a_copy_through_a_node_killed_mid_run_resumes_from_the_cells_offset_once_it_is_back() {
    let mut cell = Cell::made_alone(three_founders);
    let dir = made_client(&cell, "client");
    let bound = cell.bound_quic();
    mantle(&dir, &["mb", &format!("s3://{RESUMED}")], None);
    let run = 5usize << 20;
    let size = 4 * run + (1 << 20);
    let object = bytes(14, size);
    let mut copying = Copying::start(&dir, "node", size);
    copying.write(&object[..2 * run + run / 2]);
    copying.until_said(
        &mut cell,
        &format!("run 2 committed, {} bytes durable", 2 * run),
    );
    // The client asks the nodes in the order its directory names them, the first first.
    let killed = bound[0].0;
    cell.kill(killed);
    copying.write(&object[2 * run + run / 2..]);
    let (done, out, said) = copying.finish(&mut cell);
    assert!(
        !done && said.iter().any(|l| l.ends_with("the upload: SlowDown")),
        "{out}{said:?}"
    );
    assert_eq!(runs_held(&dir, "node"), [run as u64; 2]);
    // Back on a port of its own (node.md §1.7): its peers and the client are told where, its
    // volume enlists again, and every range catches up on it, so the cell places blocks again.
    let groups: std::collections::BTreeSet<u128> =
        cell.status.keys().map(|(_, group)| *group).collect();
    cell.start(killed);
    cell.introduce();
    cell.enlisted(killed);
    for group in &groups {
        cell.converged(*group);
    }
    point_at(&dir, &cell.bound_quic());
    let mut again = Copying::start(&dir, "node", size);
    again.write(&object);
    let (done, out, said) = again.finish(&mut cell);
    assert!(done, "{out}{said:?}");
    resumed_whole(&dir, "node", ((2 * run) as u64, 2, 5), &said, &object);
}

/// Diagnostic (scratch): a copy's read-back get whose standard output is drained slowly, a piece
/// of `DIAG_PIECE` bytes every `DIAG_PAUSE_MS`: whether a node or the client cuts it.
#[test]
#[ignore]
fn diag_a_slowly_drained_get() {
    use std::io::Read;
    let piece: usize = std::env::var("DIAG_PIECE").map_or(4096, |v| v.parse().unwrap());
    let pause: u64 = std::env::var("DIAG_PAUSE_MS").map_or(10, |v| v.parse().unwrap());
    let mut cell = Cell::made_alone(three_founders);
    let dir = made_client(&cell, "client");
    mantle(&dir, &["mb", &format!("s3://{RESUMED}")], None);
    let run = 5usize << 20;
    let file = cell.out().parent().unwrap().join("runs.bin");
    let object = bytes(10, 4 * run + 77);
    std::fs::write(&file, &object).unwrap();
    mantle(
        &dir,
        &[
            "cp",
            &file.display().to_string(),
            &format!("s3://{RESUMED}/file"),
        ],
        None,
    );
    let mut child = Command::new(NODE)
        .arg("--client")
        .arg(&dir)
        .args(["cp", &format!("s3://{RESUMED}/file"), "-"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let errs = std::thread::spawn(move || {
        let mut said = Vec::new();
        stderr.read_to_end(&mut said).unwrap();
        said
    });
    let began = std::time::Instant::now();
    let mut got = Vec::new();
    let mut buf = vec![0u8; piece];
    loop {
        let n = stdout.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        got.extend_from_slice(&buf[..n]);
        std::thread::sleep(std::time::Duration::from_millis(pause));
    }
    let status = child.wait().unwrap();
    let said = errs.join().unwrap();
    eprintln!(
        "diag-slow piece={piece} pause_ms={pause} got={} of {} in {:?} status={status:?} said={}",
        got.len(),
        object.len(),
        began.elapsed(),
        String::from_utf8_lossy(&said)
    );
    assert!(status.success() && got == object);
}

/// Diagnostic (scratch): a read-back get at full speed while node `DIAG_VICTIM` (its place among
/// the client's config's nodes) is frozen by SIGSTOP for `DIAG_FROZEN_MS`: whether the get is cut.
#[cfg(unix)]
#[test]
#[ignore]
fn diag_a_get_beside_a_frozen_node() {
    use std::io::Read;
    let frozen: u64 = std::env::var("DIAG_FROZEN_MS").map_or(8000, |v| v.parse().unwrap());
    let place: usize = std::env::var("DIAG_VICTIM").map_or(1, |v| v.parse().unwrap());
    let mut cell = Cell::made_alone(three_founders);
    let dir = made_client(&cell, "client");
    mantle(&dir, &["mb", &format!("s3://{RESUMED}")], None);
    let run = 5usize << 20;
    let file = cell.out().parent().unwrap().join("runs.bin");
    let object = bytes(10, 4 * run + 77);
    std::fs::write(&file, &object).unwrap();
    mantle(
        &dir,
        &[
            "cp",
            &file.display().to_string(),
            &format!("s3://{RESUMED}/file"),
        ],
        None,
    );
    let config = std::fs::read_to_string(dir.join("config")).unwrap();
    let order: Vec<u64> = config
        .lines()
        .filter_map(|l| l.strip_prefix("node = "))
        .map(|l| l.split(' ').next().unwrap().parse().unwrap())
        .collect();
    let victim = order[place];
    let pid = cell.running.get(&victim).unwrap().child.id();
    eprintln!("diag-frozen order={order:?} victim={victim} pid={pid} frozen_ms={frozen}");
    assert!(Command::new("kill").args(["-STOP", &pid.to_string()]).status().unwrap().success());
    let began = std::time::Instant::now();
    let mut child = Command::new(NODE)
        .arg("--client")
        .arg(&dir)
        .args(["cp", &format!("s3://{RESUMED}/file"), "-"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let outs = std::thread::spawn(move || {
        let mut got = Vec::new();
        stdout.read_to_end(&mut got).unwrap();
        got
    });
    let errs = std::thread::spawn(move || {
        let mut said = Vec::new();
        stderr.read_to_end(&mut said).unwrap();
        said
    });
    std::thread::sleep(std::time::Duration::from_millis(frozen));
    assert!(Command::new("kill").args(["-CONT", &pid.to_string()]).status().unwrap().success());
    let status = child.wait().unwrap();
    let got = outs.join().unwrap();
    let said = errs.join().unwrap();
    eprintln!(
        "diag-frozen-end victim={victim} got={} of {} in {:?} status={status:?} said={}",
        got.len(),
        object.len(),
        began.elapsed(),
        String::from_utf8_lossy(&said)
    );
    assert!(status.success() && got == object);
}
