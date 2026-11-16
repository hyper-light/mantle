//! A cell mid-upgrade (docs/research/91; node.md §1.7): founders of two releases at once, as a
//! cell whose machines are upgraded at different times holds them, and clients of each. Init's
//! first founder runs this binary and the other two the previous wire profile's,
//! `MANTLE_PREVIOUS`, which `scripts/previous-binary.sh` builds from the commit
//! `scripts/previous-profile` names. Every operation profile one has by the previous binary's
//! command line through each founder, and by this one's through each founder of the previous
//! binary; profile two's by this one's, refused typed by the client through a founder that does
//! not speak it, answered through this one's founder, and `ServiceUnavailable` at both doors for a
//! read of versions while the Name range is led by a founder of the previous binary. Then those
//! founders are upgraded in turn, each an orderly stop and a start of this binary on its
//! directory; the root range's leader raises the cell's upgrade fence to profile two once every
//! founder speaks it, past which a founder announcing profile one does not start; and every
//! operation again through each founder, the previous command line's among it.
//!
//! A binary of its own, run by the `upgrade` feature with `MANTLE_PREVIOUS` set (CI's `upgrade`
//! job): it needs a second release's binary built.

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
#[allow(dead_code)]
#[path = "support/s3.rs"]
mod s3;

use std::process::{Command, Stdio};

use cell::*;

/// The previous wire profile's `mantle`: this suite's other release.
fn previous() -> PathBuf {
    let path = std::env::var_os("MANTLE_PREVIOUS")
        .expect("MANTLE_PREVIOUS: the previous wire profile's mantle (scripts/previous-binary.sh)");
    let path = PathBuf::from(path);
    assert!(path.is_file(), "MANTLE_PREVIOUS names no file: {path:?}");
    path
}

/// Bytes `n` of the suite's objects: each a function of its object and its place.
fn bytes(n: u64, len: usize) -> Vec<u8> {
    (0..len as u64)
        .map(|at| (at.wrapping_mul(131).wrapping_add(n.wrapping_mul(7)) % 251) as u8)
        .collect()
}

/// Points the client directory `dir` at `nodes` alone, each where it listens.
fn point_at(dir: &Path, nodes: &[(u64, SocketAddr)]) {
    let path = dir.join("config");
    let text = std::fs::read_to_string(&path).unwrap();
    let mut kept: String = text
        .lines()
        .filter(|l| !l.starts_with("node = "))
        .map(|l| format!("{l}\n"))
        .collect();
    for (id, quic) in nodes {
        kept.push_str(&format!("node = {id} {quic}\n"));
    }
    std::fs::write(&path, kept).unwrap();
}

/// A client `mantle init client` made in the cell's root.
fn made_client(cell: &Cell, name: &str) -> PathBuf {
    let dir = cell.out().parent().unwrap().join(name);
    let made = Command::new(NODE)
        .args(["init", "client"])
        .arg(&dir)
        .arg("--cell")
        .arg(cell.out())
        .output()
        .unwrap();
    assert!(made.status.success(), "{made:?}");
    dir
}

/// What `binary --client dir args` said: whether it exited well, its standard output and error.
fn run(binary: &Path, dir: &Path, args: &[&str], input: Option<&[u8]>) -> (bool, String, String) {
    let mut child = Command::new(binary)
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
        if let Err(e) = stdin.write_all(input) {
            assert_eq!(e.kind(), std::io::ErrorKind::BrokenPipe, "{e}");
        }
    }
    let output = child.wait_with_output().unwrap();
    (
        output.status.success(),
        String::from_utf8(output.stdout).unwrap(),
        String::from_utf8(output.stderr).unwrap(),
    )
}

/// What `binary --client dir args` said on standard output; refused unless it exited well.
fn ran(binary: &Path, dir: &Path, args: &[&str], input: Option<&[u8]>) -> String {
    let (well, out, err) = run(binary, dir, args, input);
    assert!(well, "{binary:?} {args:?}: {out}{err}");
    out
}

/// What `binary --client dir args` said on standard error; refused unless it failed.
fn refused(binary: &Path, dir: &Path, args: &[&str]) -> String {
    let (well, out, err) = run(binary, dir, args, None);
    assert!(!well, "{binary:?} {args:?} was answered: {out}");
    err
}

/// Every operation profile one has, by `binary`'s command line through the node `dir` names,
/// in bucket `bucket` under `root`: a bucket made and listed; an object put from a file and read
/// back to one, one from standard input just past a run read back on standard output; each headed
/// and listed; a multipart upload's part from a file, listed and completed and read back, and
/// another's from standard input, listed and aborted; every object removed, and the bucket. The
/// objects are as small as each path allows: a cell's deletes free their room only once the
/// collector's grace passes.
fn profile_one(binary: &Path, dir: &Path, root: &Path, bucket: &str) {
    let at = |key: &str| format!("s3://{bucket}/{key}");
    let got = |key: &str| {
        let out = Command::new(binary)
            .arg("--client")
            .arg(dir)
            .args(["cp", &at(key), "-"])
            .output()
            .unwrap();
        assert!(out.status.success(), "{bucket}/{key}: {out:?}");
        out.stdout
    };
    assert!(ran(binary, dir, &["mb", &format!("s3://{bucket}")], None).contains("made"));
    assert!(ran(binary, dir, &["ls"], None).contains(&format!(" {bucket}")));
    let file = root.join(format!("{bucket}.bin"));
    let small = bytes(1, 300_017);
    std::fs::write(&file, &small).unwrap();
    let file_text = file.display().to_string();
    ran(binary, dir, &["cp", &file_text, &at("file")], None);
    let back = root.join(format!("{bucket}.back"));
    let back_text = back.display().to_string();
    ran(binary, dir, &["cp", &at("file"), &back_text], None);
    assert!(std::fs::read(&back).unwrap() == small, "{bucket}/file");
    // Past one of standard input's runs, the AWS CLI's 8 MiB (gateway.md §6).
    let streamed = bytes(2, (8 << 20) + 5);
    ran(binary, dir, &["cp", "-", &at("stdin")], Some(&streamed));
    assert!(got("stdin") == streamed, "{bucket}/stdin");
    let head = ran(binary, dir, &["head", &at("stdin")], None);
    assert!(head.contains(&format!("size {}", streamed.len())), "{head}");
    let keys = ran(binary, dir, &["ls", &at("")], None);
    assert!(
        keys.contains(&format!("{} file", small.len()))
            && keys.contains(&format!("{} stdin", streamed.len())),
        "{keys}"
    );
    let tag = |said: &str| said.split_whitespace().nth(1).unwrap().to_owned();
    let upload = ran(binary, dir, &["multipart", "create", &at("parts")], None);
    let upload = upload.trim();
    let part = bytes(3, 70_001);
    let part_file = root.join(format!("{bucket}.part"));
    std::fs::write(&part_file, &part).unwrap();
    let one = ran(
        binary,
        dir,
        &[
            "multipart",
            "put",
            &at("parts"),
            upload,
            "1",
            &part_file.display().to_string(),
        ],
        None,
    );
    let listed = ran(
        binary,
        dir,
        &["multipart", "parts", &at("parts"), upload],
        None,
    );
    assert_eq!(listed.lines().count(), 1, "{listed}");
    let uploads = ran(
        binary,
        dir,
        &["multipart", "uploads", &format!("s3://{bucket}")],
        None,
    );
    assert!(uploads.contains(upload), "{uploads}");
    ran(
        binary,
        dir,
        &[
            "multipart",
            "complete",
            &at("parts"),
            upload,
            &format!("1={}", tag(&one)),
        ],
        None,
    );
    assert!(got("parts") == part, "{bucket}/parts");
    let aborted = ran(binary, dir, &["multipart", "create", &at("aborted")], None);
    let aborted = aborted.trim();
    let last = bytes(4, 1_234);
    ran(
        binary,
        dir,
        &[
            "multipart",
            "put",
            &at("aborted"),
            aborted,
            "1",
            "-",
            "--length",
            &last.len().to_string(),
        ],
        Some(&last),
    );
    let listed = ran(
        binary,
        dir,
        &["multipart", "parts", &at("aborted"), aborted],
        None,
    );
    assert_eq!(listed.lines().count(), 1, "{listed}");
    ran(
        binary,
        dir,
        &["multipart", "abort", &at("aborted"), aborted],
        None,
    );
    for key in ["file", "stdin", "parts"] {
        ran(binary, dir, &["rm", &at(key)], None);
    }
    ran(binary, dir, &["rb", &format!("s3://{bucket}")], None);
}

/// The group of the cell's Name range, as init named it in the founders' configurations
/// (`range = GROUP name`, the group in hexadecimal).
fn name_group(cell: &Cell) -> u128 {
    let config = cell.made.values().next().unwrap();
    let text = std::fs::read_to_string(config).unwrap();
    text.lines()
        .filter_map(|l| l.strip_prefix("range = "))
        .find_map(|l| {
            let (group, layer) = l.split_once(' ')?;
            (layer.trim() == "name").then(|| u128::from_str_radix(group, 16).unwrap())
        })
        .unwrap()
}

/// The Name range's leader runs the previous binary: a founder this binary runs that leads it
/// hands the lead on at an orderly stop to a voter whose connection speaks its profile where one
/// does, and none here does, so to one of the previous binary's; started again, it follows.
fn led_by_the_previous(cell: &mut Cell, group: u128) -> u64 {
    let leader = cell.leader(group);
    if cell.binaries.contains_key(&leader) {
        return leader;
    }
    cell.stop(leader);
    cell.start(leader);
    cell.introduce();
    let led = cell.leader(group);
    assert!(cell.binaries.contains_key(&led), "{led} leads");
    led
}

/// Founder `id` upgraded: an orderly stop, which hands every range it leads on, and this binary
/// started on its directory; the Name range caught up on it.
fn upgrade(cell: &mut Cell, id: u64, group: u128) {
    cell.stop(id);
    cell.binaries.remove(&id);
    cell.start(id);
    cell.introduce();
    cell.enlisted(id);
    cell.converged(group);
}

#[test]
fn a_cell_of_two_releases_serves_every_operation_through_its_upgrade() {
    let previous = previous();
    let this = PathBuf::from(NODE);
    // Init's first founder on this binary, its other two on the previous one's.
    let mut cell = Cell::made_alone_running(
        three_founders,
        &[(1, previous.clone()), (2, previous.clone())],
    );
    let older_founders: Vec<u64> = cell.binaries.keys().copied().collect();
    let this_founder = *cell
        .dirs
        .keys()
        .find(|id| !cell.binaries.contains_key(id))
        .unwrap();
    assert_eq!(older_founders.len(), 2);
    let root = cell.out().parent().unwrap().to_path_buf();
    let new = made_client(&cell, "new");
    let old = made_client(&cell, "old");
    let profile_two = format!("{:?}", mantle_protocol::wire::Profile::Two);
    let older = format!("an older wire profile than the operation's, {profile_two}");

    // Mixed: every profile-one operation by the previous command line through every founder, and
    // by this one's through each founder of the previous binary, which speaks it profile one.
    let mut n = 0;
    for (id, quic) in cell.bound_quic() {
        for (binary, dir) in [(&previous, &old), (&this, &new)] {
            if binary == &this && id == this_founder {
                continue;
            }
            point_at(dir, &[(id, quic)]);
            n += 1;
            profile_one(binary, dir, &root, &format!("one-{n}"));
        }
    }

    // Profile two by this command line. Through a founder of the previous binary, the client sends
    // nothing and says why.
    let bound = cell.bound_quic();
    let at = |id: u64| bound.iter().copied().find(|(n, _)| *n == id).unwrap();
    point_at(&new, &[at(older_founders[0])]);
    ran(&this, &new, &["mb", "s3://two"], None);
    let file = root.join("two.bin");
    // Past S3's least part, 5 MiB, so a range of it may be copied (research/89 §4.1).
    let object = bytes(5, (5 << 20) + 9);
    std::fs::write(&file, &object).unwrap();
    let file_text = file.display().to_string();
    let typed = ["--content-type", "text/csv", "--metadata", "k=v"];
    let err = refused(
        &this,
        &new,
        &[&["cp", file_text.as_str(), "s3://two/meta"][..], &typed[..]].concat(),
    );
    assert!(err.contains(&older), "{err}");
    let err = refused(&this, &new, &["ls", "--versions", "s3://two"]);
    assert!(err.contains(&older), "{err}");

    // Through the founder of this binary, the Name range led by one of the previous: what reads no
    // profile-two read is answered; a read of a version or of versions is `Unavailable`.
    let group = name_group(&cell);
    let leader = led_by_the_previous(&mut cell, group);
    let bound = cell.bound_quic();
    let at = |id: u64| bound.iter().copied().find(|(n, _)| *n == id).unwrap();
    point_at(&new, &[at(this_founder)]);
    ran(
        &this,
        &new,
        &[&["cp", file_text.as_str(), "s3://two/meta"][..], &typed[..]].concat(),
        None,
    );
    let head = ran(&this, &new, &["head", "s3://two/meta"], None);
    for line in ["content-type text/csv", "x-amz-meta-k v"] {
        assert!(head.lines().any(|l| l == line), "{line}: {head}");
    }
    let copied = ran(&this, &new, &["cp", "s3://two/meta", "s3://two/copy"], None);
    assert!(copied.contains("-> s3://two/copy"), "{copied}");
    let out = Command::new(&this)
        .arg("--client")
        .arg(&new)
        .args(["cp", "s3://two/copy", "-"])
        .output()
        .unwrap();
    assert!(
        out.status.success() && out.stdout == object,
        "the copy's bytes"
    );
    let err = refused(&this, &new, &["ls", "--versions", "s3://two"]);
    assert!(err.contains("Unavailable"), "{leader} leads: {err}");
    let err = refused(
        &this,
        &new,
        &["head", "s3://two/meta", "--version-id", "null"],
    );
    assert!(err.contains("Unavailable"), "{leader} leads: {err}");
    // Through the founder's S3 listener the same: S3's `503 ServiceUnavailable`.
    let listener = || {
        let (at, _) = cell.s3_bound[&this_founder];
        let (_, region, _) = &cell.s3[&this_founder];
        s3::Client::new(at, s3::Key::of(&cell.out()), region)
    };
    let named = listener().send("GET", "/two/meta?versionId=null", &[], b"");
    assert_eq!(
        (named.status, named.element("Code").as_deref()),
        (503, Some("ServiceUnavailable")),
        "{}",
        named.text()
    );
    let plain = listener().send("GET", "/two/meta", &[], b"");
    assert!(
        plain.status == 200 && plain.body == object,
        "{}",
        plain.status
    );
    let removed = ran(
        &this,
        &new,
        &["rm", "s3://two/meta", "s3://two/copy", "s3://two/never"],
        None,
    );
    assert_eq!(removed.lines().count(), 3, "{removed}");

    // Founders 2 and 3 upgraded in turn, each an orderly stop and this binary on its directory.
    for &id in &older_founders {
        upgrade(&mut cell, id, group);
    }
    assert!(cell.binaries.is_empty());

    // Every founder speaks profile two: the root range's leader raises the cell's fence to it, and
    // every founder says so. Past it, a founder announcing profile one, as a binary of the previous
    // profile is, does not start, and its directory keeps what it held for this binary again.
    cell.until(|c| {
        let raised = c.running.keys().all(|id| c.fences.get(id) == Some(&2));
        if raised { Some(()) } else { None }
    });
    let held = older_founders[0];
    cell.stop(held);
    cell.envs
        .insert(held, vec![("MANTLE_WIRE".into(), "1".into())]);
    let said = cell.start_refused(held);
    assert!(
        said.contains("upgrade fence is at wire profile 2"),
        "{said}"
    );
    cell.envs.remove(&held);
    cell.start(held);
    cell.introduce();
    cell.enlisted(held);
    cell.converged(group);

    // Every profile-one operation by the previous command line through every founder again, which
    // a cell of this binary serves on profile one, and every profile-two operation by this one's.
    for (id, quic) in cell.bound_quic() {
        point_at(&old, &[(id, quic)]);
        n += 1;
        profile_one(&previous, &old, &root, &format!("one-{n}"));
        point_at(&new, &[(id, quic)]);
        let bucket = format!("two-{id}");
        let at = |key: &str| format!("s3://{bucket}/{key}");
        ran(&this, &new, &["mb", &format!("s3://{bucket}")], None);
        ran(
            &this,
            &new,
            &[
                &["cp", file_text.as_str(), at("meta").as_str()][..],
                &typed[..],
            ]
            .concat(),
            None,
        );
        let head = ran(&this, &new, &["head", &at("meta")], None);
        assert!(head.lines().any(|l| l == "x-amz-meta-k v"), "{head}");
        ran(&this, &new, &["cp", &at("meta"), &at("copy")], None);
        let versions = ran(&this, &new, &["ls", "--versions", &at("")], None);
        assert_eq!(versions.lines().count(), 2, "{versions}");
        let head = ran(
            &this,
            &new,
            &["head", &at("meta"), "--version-id", "null"],
            None,
        );
        assert!(head.contains(&format!("size {}", object.len())), "{head}");
        let (s3_at, _) = cell.s3_bound[&id];
        let (_, region, _) = &cell.s3[&id];
        let named = s3::Client::new(s3_at, s3::Key::of(&cell.out()), region).send(
            "GET",
            &format!("/{bucket}/meta?versionId=null"),
            &[],
            b"",
        );
        assert!(
            named.status == 200 && named.body == object,
            "{}",
            named.status
        );
        let upload = ran(&this, &new, &["multipart", "create", &at("parts")], None);
        let part = ran(
            &this,
            &new,
            &[
                "multipart",
                "copy",
                &at("parts"),
                upload.trim(),
                "1",
                &at("meta"),
                "--range",
                "0-99",
            ],
            None,
        );
        let tag = part.split_whitespace().nth(1).unwrap().to_owned();
        ran(
            &this,
            &new,
            &[
                "multipart",
                "complete",
                &at("parts"),
                upload.trim(),
                &format!("1={tag}"),
            ],
            None,
        );
        let out = Command::new(&this)
            .arg("--client")
            .arg(&new)
            .args(["cp", &at("parts"), "-"])
            .output()
            .unwrap();
        assert!(
            out.status.success() && out.stdout == object[..100],
            "the part copy"
        );
        let removed = ran(
            &this,
            &new,
            &["rm", &at("meta"), &at("copy"), &at("parts")],
            None,
        );
        assert_eq!(removed.lines().count(), 3, "{removed}");
    }
}
