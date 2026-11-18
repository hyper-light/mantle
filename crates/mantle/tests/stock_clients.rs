//! Stock S3 clients against a running cell (docs/design/node.md §4): the AWS CLI v2 and boto3, as
//! the runner image has them, against a cell of three `mantle serve` processes made by
//! `mantle init`, with the access key init wrote to the operator's profile. Asked: `aws s3 mb`,
//! `cp`, `ls` and `rm`; `aws s3api`'s object, listing and multipart commands; `aws s3 cp` of a file
//! past the CLI's 8 MiB multipart threshold, from standard input and to standard output; and
//! boto3's object and listing calls and its managed transfers with the SDK's default checksums.
//! Every object read back is compared byte for byte with what was put. A tool the runner lacks
//! fails the suite: it never skips.
//!
//! A binary of its own (crates/mantle/tests/support/cell.rs's harness).

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
#[path = "../../range/tests/support/linear.rs"]
mod linear;
#[allow(dead_code)]
#[path = "../../node/tests/support/relay.rs"]
mod relay;

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use cell::*;
use mantle_protocol::bounds::STOCK_PATIENCE;

/// The CLI's multipart threshold, `8MB`, a megabyte being 1024² octets to it (the AWS CLI's S3
/// configuration reference; `awscli/customizations/s3/utils.py`): a file past it goes up in parts.
const THRESHOLD: usize = 8 << 20;

/// The Python the runner image has: `python` on Windows, `python3` elsewhere.
const PYTHON: &str = if cfg!(windows) { "python" } else { "python3" };

/// A cell of three, each node enlisted, and the operator's profile for the first node's endpoint:
/// init's credentials beside a config naming the endpoint, which init leaves out for a founder
/// whose port is chosen as it binds (node.md §1.5).
fn started(root: &Path) -> (Cell, Vec<(String, String)>) {
    let mut cell = Cell::made(three_founders);
    let ids: Vec<u64> = cell.dirs.keys().copied().collect();
    for &id in &ids {
        cell.enlisted(id);
    }
    let out = cell.dirs[&ids[0]].parent().unwrap().to_path_buf();
    let (at, _) = cell.s3_bound[&ids[0]];
    let (_, region, _) = &cell.s3[&ids[0]];
    // The profile as init writes it, its transfer client named: botocore's here, the CRT's in
    // `config-crt` beside it, which a run of the CRT's client names as its config.
    let profile = |client: &str| {
        format!(
            "[profile mantle]\nregion = {region}\nendpoint_url = http://{at}\ns3 =\n  addressing_style = path\n  preferred_transfer_client = {client}\n"
        )
    };
    let config = root.join("config");
    std::fs::write(&config, profile("classic")).unwrap();
    std::fs::write(root.join("config-crt"), profile("crt")).unwrap();
    let env = vec![
        ("AWS_CONFIG_FILE".into(), config.display().to_string()),
        (
            "AWS_SHARED_CREDENTIALS_FILE".into(),
            out.join("s3").join("credentials").display().to_string(),
        ),
        ("AWS_PROFILE".into(), "mantle".into()),
        // No instance metadata to look for, no pager to wait on.
        ("AWS_EC2_METADATA_DISABLED".into(), "true".into()),
        ("AWS_PAGER".into(), String::new()),
    ];
    (cell, env)
}

/// `program` with `args`, its environment `env`, `input` on its standard input: its output, which
/// must say it exited 0. A program the runner lacks fails here. Its input is written and its output
/// read each on a thread of its own, and its exit and their ends are waited on under `cell`'s
/// watch, its quiet period longer by a stock client's patience (`bounds::STOCK_PATIENCE`): what
/// the client does between its exchanges, its start and the bytes it builds, is its own work and
/// the OS's, which no law of the cell's bounds, and a stock client waits that long on the cell
/// between bytes before it gives up itself. A failure names the command at the test's own line, and
/// the process is killed rather than outliving the test.
#[track_caller]
fn run(
    cell: &mut Cell,
    program: &str,
    args: &[&str],
    env: &[(String, String)],
    input: Option<&[u8]>,
) -> Output {
    let mut command = Command::new(program);
    command
        .args(args)
        .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = Killed(
        command
            .spawn()
            .unwrap_or_else(|e| panic!("{program}: {e}: the runner must have it")),
    );
    let writing = input.map(|input| {
        let (mut stdin, input) = (child.0.stdin.take().unwrap(), input.to_vec());
        std::thread::spawn(move || stdin.write_all(&input))
    });
    let stdout = drained(child.0.stdout.take().unwrap());
    let stderr = drained(child.0.stderr.take().unwrap());
    let what = format!("{program} {args:?}");
    let status = cell.waiting_on(&what, STOCK_PATIENCE, Progress::Unmeasured, |_| {
        let ended = stdout.is_finished()
            && stderr.is_finished()
            && writing.as_ref().is_none_or(JoinHandle::is_finished);
        if ended {
            child.0.try_wait().unwrap()
        } else {
            None
        }
    });
    let output = Output {
        status,
        stdout: stdout.join().unwrap().unwrap(),
        stderr: stderr.join().unwrap().unwrap(),
    };
    assert!(
        output.status.success(),
        "{what}: {}\n{}{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if let Some(writing) = writing {
        (writing.join().unwrap()).unwrap_or_else(|e| panic!("{what}'s input: {e}"));
    }
    output
}

/// What `pipe` gives until its end, read on a thread of its own.
fn drained(mut pipe: impl std::io::Read + Send + 'static) -> JoinHandle<std::io::Result<Vec<u8>>> {
    std::thread::spawn(move || {
        let mut read = Vec::new();
        pipe.read_to_end(&mut read).map(|_| read)
    })
}

/// A stock client's process, killed and reaped once dropped, so one whose wait failed does not
/// outlive the test; one that exited is only reaped.
struct Killed(Child);

impl Drop for Killed {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[track_caller]
fn aws(cell: &mut Cell, args: &[&str], env: &[(String, String)]) -> Output {
    run(cell, "aws", args, env, None)
}

fn text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

/// Bytes for an object: `len` of them, each a function of `n` and its place.
fn bytes(n: u64, len: usize) -> Vec<u8> {
    object_bytes(n, len)
}

fn write(dir: &Path, name: &str, data: &[u8]) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, data).unwrap();
    path
}

/// The value of `query` (JMESPath, the CLI's `--query`) in what `args` answer, as text.
#[track_caller]
fn query(cell: &mut Cell, args: &[&str], query: &str, env: &[(String, String)]) -> String {
    let mut all = args.to_vec();
    all.extend(["--query", query, "--output", "text"]);
    text(&aws(cell, &all, env))
}

/// The CLI's version as `aws --version` says it: `aws-cli/2.38.4 Python/...`.
fn cli_version(said: &str) -> (u32, u32, u32) {
    let number = said
        .strip_prefix("aws-cli/")
        .and_then(|rest| rest.split_whitespace().next())
        .unwrap_or_else(|| panic!("not the AWS CLI's version: {said}"));
    let mut parts = number.split('.').map(|p| p.parse::<u32>().unwrap());
    (
        parts.next().unwrap(),
        parts.next().unwrap(),
        parts.next().unwrap(),
    )
}

/// A small object and a large one up and down by the CLI's CRT transfer client, each read back
/// byte-identical, then removed.
fn crt_transfers(cell: &mut Cell, dir: &Path, env: &[(String, String)]) {
    let mut crt = env.to_vec();
    crt.retain(|(k, _)| k != "AWS_CONFIG_FILE");
    crt.push((
        "AWS_CONFIG_FILE".into(),
        dir.join("config-crt").display().to_string(),
    ));
    for (client, data) in [
        ("crt-small", bytes(7, 333_331)),
        ("crt-large", bytes(8, THRESHOLD * 2 + 7)),
    ] {
        let file = write(dir, &format!("{client}.bin"), &data);
        let key = format!("s3://stock/{client}.bin");
        aws(cell, &["s3", "cp", file.to_str().unwrap(), &key], &crt);
        let back = dir.join(format!("back-{client}.bin"));
        aws(cell, &["s3", "cp", &key, back.to_str().unwrap()], &crt);
        assert!(
            std::fs::read(&back).unwrap() == data,
            "{client} by the CRT client"
        );
        aws(cell, &["s3", "rm", &key], &crt);
    }
}

#[test]
fn the_aws_cli_and_boto3_pass_against_a_cell_of_three() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path();
    let (mut cell, env) = started(dir);
    let version = text(&aws(&mut cell, &["--version"], &env));
    println!("{version}");

    // aws s3 by each of the CLI's transfer clients, botocore's and the CRT's, named rather than left
    // to the CLI's choice: a small object and a large one up and down, each read back
    // byte-identical. The CRT's client takes an http endpoint, and is `aws s3`'s default on every
    // host, from the CLI's 2.38.0 ("Disable TLS in CRT client when resolved endpoint scheme is
    // http"; "Enable the CRT S3 transfer client by default on all hosts", the CLI's changelog); an
    // older CLI's default is botocore's, which the commands below run.
    aws(&mut cell, &["s3", "mb", "s3://stock"], &env);
    if cli_version(&version) >= (2, 38, 0) {
        crt_transfers(&mut cell, dir, &env);
    }

    // aws s3: a bucket, a small object and a large one up, listed, down, compared, and removed.
    let small = bytes(1, 100_003);
    let large = bytes(2, THRESHOLD * 2 + 12_345);
    let small_file = write(dir, "small.bin", &small);
    let large_file = write(dir, "large.bin", &large);
    aws(
        &mut cell,
        &[
            "s3",
            "cp",
            small_file.to_str().unwrap(),
            "s3://stock/small.bin",
        ],
        &env,
    );
    aws(
        &mut cell,
        &[
            "s3",
            "cp",
            large_file.to_str().unwrap(),
            "s3://stock/large.bin",
        ],
        &env,
    );
    let listed = text(&aws(&mut cell, &["s3", "ls", "s3://stock/"], &env));
    assert!(
        listed.contains("small.bin") && listed.contains("large.bin"),
        "{listed}"
    );
    for (key, data) in [("small.bin", &small), ("large.bin", &large)] {
        let back = dir.join(format!("back-{key}"));
        aws(
            &mut cell,
            &[
                "s3",
                "cp",
                &format!("s3://stock/{key}"),
                back.to_str().unwrap(),
            ],
            &env,
        );
        assert!(std::fs::read(&back).unwrap() == *data, "{key} read back");
    }
    // The CLI's multipart upload committed with S3's ETag form: the parts' MD5s' MD5, and their count.
    let head = [
        "s3api",
        "head-object",
        "--bucket",
        "stock",
        "--key",
        "large.bin",
    ];
    let etag = query(&mut cell, &head, "ETag", &env);
    assert!(etag.trim_matches('"').ends_with("-3"), "{etag}");
    // From standard input and to standard output, past the threshold.
    let piped = bytes(3, THRESHOLD + 777);
    run(
        &mut cell,
        "aws",
        &["s3", "cp", "-", "s3://stock/piped.bin"],
        &env,
        Some(&piped),
    );
    let out = aws(&mut cell, &["s3", "cp", "s3://stock/piped.bin", "-"], &env);
    assert!(out.stdout == piped, "piped.bin read to standard output");
    aws(&mut cell, &["s3", "rm", "s3://stock/small.bin"], &env);
    let listed = text(&aws(&mut cell, &["s3", "ls", "s3://stock/"], &env));
    assert!(!listed.contains("small.bin"), "{listed}");

    // aws s3api: the object and listing commands.
    let one = bytes(4, 54_321);
    let one_file = write(dir, "one.bin", &one);
    let put = [
        "s3api",
        "put-object",
        "--bucket",
        "stock",
        "--key",
        "one.bin",
        "--body",
        one_file.to_str().unwrap(),
    ];
    let etag = query(&mut cell, &put, "ETag", &env);
    assert!(!etag.trim_matches('"').is_empty(), "{etag}");
    let got = dir.join("got.bin");
    aws(
        &mut cell,
        &[
            "s3api",
            "get-object",
            "--bucket",
            "stock",
            "--key",
            "one.bin",
            got.to_str().unwrap(),
        ],
        &env,
    );
    assert!(std::fs::read(&got).unwrap() == one, "one.bin by get-object");
    let ranged = dir.join("ranged.bin");
    aws(
        &mut cell,
        &[
            "s3api",
            "get-object",
            "--bucket",
            "stock",
            "--key",
            "one.bin",
            "--range",
            "bytes=10-4009",
            ranged.to_str().unwrap(),
        ],
        &env,
    );
    assert!(
        std::fs::read(&ranged).unwrap() == one[10..=4009],
        "one.bin's range"
    );
    let head = [
        "s3api",
        "head-object",
        "--bucket",
        "stock",
        "--key",
        "one.bin",
    ];
    assert_eq!(
        query(&mut cell, &head, "ContentLength", &env),
        one.len().to_string()
    );
    let list = ["s3api", "list-objects-v2", "--bucket", "stock"];
    let keys = query(&mut cell, &list, "Contents[].Key", &env);
    assert!(
        keys.contains("one.bin") && keys.contains("piped.bin"),
        "{keys}"
    );
    aws(
        &mut cell,
        &[
            "s3api",
            "delete-object",
            "--bucket",
            "stock",
            "--key",
            "one.bin",
        ],
        &env,
    );

    // aws s3api: a multipart upload by hand, listed, completed and read back; another aborted.
    let create = [
        "s3api",
        "create-multipart-upload",
        "--bucket",
        "stock",
        "--key",
        "parts.bin",
    ];
    let upload = query(&mut cell, &create, "UploadId", &env);
    let parts = [bytes(5, 5 << 20), bytes(6, 3_333)];
    let mut tags = Vec::new();
    for (number, data) in parts.iter().enumerate() {
        let file = write(dir, &format!("part-{number}"), data);
        let number = (number + 1).to_string();
        let part = [
            "s3api",
            "upload-part",
            "--bucket",
            "stock",
            "--key",
            "parts.bin",
            "--upload-id",
            &upload,
            "--part-number",
            &number,
            "--body",
            file.to_str().unwrap(),
        ];
        tags.push(
            query(&mut cell, &part, "ETag", &env)
                .trim_matches('"')
                .to_owned(),
        );
    }
    let list = [
        "s3api",
        "list-parts",
        "--bucket",
        "stock",
        "--key",
        "parts.bin",
        "--upload-id",
        &upload,
    ];
    let numbers = query(&mut cell, &list, "Parts[].PartNumber", &env);
    assert_eq!(
        numbers.split_whitespace().collect::<Vec<_>>(),
        ["1", "2"],
        "{numbers}"
    );
    let document = format!(
        "{{\"Parts\":[{{\"PartNumber\":1,\"ETag\":\"\\\"{}\\\"\"}},{{\"PartNumber\":2,\"ETag\":\"\\\"{}\\\"\"}}]}}",
        tags[0], tags[1]
    );
    let document_file = write(dir, "parts.json", document.as_bytes());
    aws(
        &mut cell,
        &[
            "s3api",
            "complete-multipart-upload",
            "--bucket",
            "stock",
            "--key",
            "parts.bin",
            "--upload-id",
            &upload,
            "--multipart-upload",
            &format!("file://{}", document_file.display()),
        ],
        &env,
    );
    let got = dir.join("parts-back.bin");
    aws(
        &mut cell,
        &[
            "s3api",
            "get-object",
            "--bucket",
            "stock",
            "--key",
            "parts.bin",
            got.to_str().unwrap(),
        ],
        &env,
    );
    assert!(
        std::fs::read(&got).unwrap() == parts.concat(),
        "parts.bin read back"
    );
    let create = [
        "s3api",
        "create-multipart-upload",
        "--bucket",
        "stock",
        "--key",
        "dropped.bin",
    ];
    let dropped = query(&mut cell, &create, "UploadId", &env);
    aws(
        &mut cell,
        &[
            "s3api",
            "abort-multipart-upload",
            "--bucket",
            "stock",
            "--key",
            "dropped.bin",
            "--upload-id",
            &dropped,
        ],
        &env,
    );
    let uploads = text(&aws(
        &mut cell,
        &["s3api", "list-multipart-uploads", "--bucket", "stock"],
        &env,
    ));
    assert!(!uploads.contains(&dropped), "{uploads}");

    // boto3: object calls, a listing, and the managed transfers, with the SDK's default checksums.
    let script = write(dir, "boto.py", BOTO.as_bytes());
    let said = run(
        &mut cell,
        PYTHON,
        &["-I", script.to_str().unwrap(), dir.to_str().unwrap()],
        &env,
        None,
    );
    println!("{}", text(&said));
    assert!(text(&said).ends_with("boto3 passed"), "{}", text(&said));
}

/// The boto3 half: each call against the profile's endpoint, every object compared with what was
/// put, a mismatch an exception and a nonzero exit.
const BOTO: &str = r#"
import io, os, sys
import boto3
from botocore.config import Config

root = sys.argv[1]
s3 = boto3.Session(profile_name="mantle").client("s3", config=Config(s3={"addressing_style": "path"}))
print("boto3", boto3.__version__)

def body(n, size):
    return bytes(((i * 0x2545F491 + n * 7) >> 8) & 0xFF for i in range(size))

small = body(11, 70_001)
s3.put_object(Bucket="stock", Key="boto/small.bin", Body=small)
got = s3.get_object(Bucket="stock", Key="boto/small.bin")["Body"].read()
assert got == small, "boto/small.bin read back"
head = s3.head_object(Bucket="stock", Key="boto/small.bin")
assert head["ContentLength"] == len(small), head
large = body(12, 9 * 1024 * 1024 + 5)
s3.upload_fileobj(io.BytesIO(large), "stock", "boto/large.bin")
back = io.BytesIO()
s3.download_fileobj("stock", "boto/large.bin", back)
assert back.getvalue() == large, "boto/large.bin read back"
keys = [o["Key"] for o in s3.list_objects_v2(Bucket="stock", Prefix="boto/")["Contents"]]
assert keys == ["boto/large.bin", "boto/small.bin"], keys
s3.delete_object(Bucket="stock", Key="boto/small.bin")
keys = [o["Key"] for o in s3.list_objects_v2(Bucket="stock", Prefix="boto/")["Contents"]]
assert keys == ["boto/large.bin"], keys
print("boto3 passed")
"#;

/// research/89 by the stock clients against a cell of three: the AWS CLI's `aws s3 cp` with
/// content headers and metadata, its server-side copies (one CopyObject below its 8 MiB threshold,
/// UploadPartCopy past it), `s3api` copy-object under REPLACE, get-object and delete-object of a
/// version, list-object-versions and delete-objects; boto3's put and head of every metadata field,
/// its copy_object under each directive and the source's conditions, its managed copy, its
/// versions and its delete_objects, quiet and not, every error code as S3's. Every object read
/// back byte-identical.
#[test]
fn the_aws_cli_and_boto3_keep_metadata_versions_copies_and_batch_deletes() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path();
    let (mut cell, env) = started(dir);
    aws(&mut cell, &["s3", "mb", "s3://kept"], &env);
    aws(&mut cell, &["s3", "mb", "s3://copies"], &env);

    // aws s3 cp with content headers and metadata, kept and returned.
    let small = bytes(40, 123_457);
    let small_file = write(dir, "small.blob", &small);
    aws(
        &mut cell,
        &[
            "s3",
            "cp",
            small_file.to_str().unwrap(),
            "s3://kept/blobs/small",
            "--content-type",
            "application/vnd.oci.image.layer.v1.tar",
            "--cache-control",
            "max-age=60",
            "--content-disposition",
            "attachment",
            "--content-language",
            "en",
            "--metadata",
            "digest=sha256:abc,kind=layer",
        ],
        &env,
    );
    let head = [
        "s3api",
        "head-object",
        "--bucket",
        "kept",
        "--key",
        "blobs/small",
    ];
    assert_eq!(
        query(&mut cell, &head, "ContentType", &env),
        "application/vnd.oci.image.layer.v1.tar"
    );
    assert_eq!(query(&mut cell, &head, "CacheControl", &env), "max-age=60");
    assert_eq!(
        query(&mut cell, &head, "ContentDisposition", &env),
        "attachment"
    );
    assert_eq!(query(&mut cell, &head, "ContentLanguage", &env), "en");
    assert_eq!(
        query(&mut cell, &head, "Metadata.digest", &env),
        "sha256:abc"
    );
    assert_eq!(query(&mut cell, &head, "Metadata.kind", &env), "layer");

    // A server-side copy across buckets below the CLI's multipart threshold (one CopyObject) and
    // one past it (UploadPartCopy), each read back whole with the source's metadata.
    let large = bytes(41, THRESHOLD * 2 + 4_321);
    let large_file = write(dir, "large.blob", &large);
    aws(
        &mut cell,
        &[
            "s3",
            "cp",
            large_file.to_str().unwrap(),
            "s3://kept/blobs/large",
            "--content-type",
            "application/octet-stream",
            "--metadata",
            "digest=sha256:def",
        ],
        &env,
    );
    for (key, data) in [("small", &small), ("large", &large)] {
        aws(
            &mut cell,
            &[
                "s3",
                "cp",
                &format!("s3://kept/blobs/{key}"),
                &format!("s3://copies/{key}"),
            ],
            &env,
        );
        let back = dir.join(format!("back-{key}"));
        aws(
            &mut cell,
            &[
                "s3",
                "cp",
                &format!("s3://copies/{key}"),
                back.to_str().unwrap(),
            ],
            &env,
        );
        assert!(
            std::fs::read(&back).unwrap() == *data,
            "copies/{key} read back"
        );
        let head = ["s3api", "head-object", "--bucket", "copies", "--key", key];
        assert!(
            query(&mut cell, &head, "Metadata.digest", &env).starts_with("sha256:"),
            "copies/{key} kept the source's metadata"
        );
    }
    // s3api copy-object under REPLACE.
    aws(
        &mut cell,
        &[
            "s3api",
            "copy-object",
            "--bucket",
            "copies",
            "--key",
            "replaced",
            "--copy-source",
            "kept/blobs/small",
            "--metadata-directive",
            "REPLACE",
            "--content-type",
            "text/plain",
            "--metadata",
            "fresh=yes",
        ],
        &env,
    );
    let head = [
        "s3api",
        "head-object",
        "--bucket",
        "copies",
        "--key",
        "replaced",
    ];
    assert_eq!(query(&mut cell, &head, "ContentType", &env), "text/plain");
    assert_eq!(query(&mut cell, &head, "Metadata.fresh", &env), "yes");
    assert_eq!(query(&mut cell, &head, "Metadata.digest", &env), "None");

    // Versions by s3api: put twice, each read by its ID, one removed by its ID, all listed.
    aws(
        &mut cell,
        &[
            "s3api",
            "put-bucket-versioning",
            "--bucket",
            "kept",
            "--versioning-configuration",
            "Status=Enabled",
        ],
        &env,
    );
    let first = bytes(42, 9_001);
    let second = bytes(43, 9_002);
    let mut ids = Vec::new();
    for (n, data) in [&first, &second].into_iter().enumerate() {
        let file = write(dir, &format!("v{n}"), data);
        let put = [
            "s3api",
            "put-object",
            "--bucket",
            "kept",
            "--key",
            "versioned",
            "--body",
            file.to_str().unwrap(),
        ];
        ids.push(query(&mut cell, &put, "VersionId", &env));
    }
    let got = dir.join("got-first");
    aws(
        &mut cell,
        &[
            "s3api",
            "get-object",
            "--bucket",
            "kept",
            "--key",
            "versioned",
            "--version-id",
            &ids[0],
            got.to_str().unwrap(),
        ],
        &env,
    );
    assert!(
        std::fs::read(&got).unwrap() == first,
        "the first version by its ID"
    );
    let listed = query(
        &mut cell,
        &[
            "s3api",
            "list-object-versions",
            "--bucket",
            "kept",
            "--prefix",
            "versioned",
        ],
        "Versions[].VersionId",
        &env,
    );
    assert_eq!(
        listed.split_whitespace().collect::<Vec<_>>(),
        [ids[1].as_str(), ids[0].as_str()],
        "{listed}"
    );
    aws(
        &mut cell,
        &[
            "s3api",
            "delete-object",
            "--bucket",
            "kept",
            "--key",
            "versioned",
            "--version-id",
            &ids[1],
        ],
        &env,
    );
    let back = dir.join("got-current");
    aws(
        &mut cell,
        &["s3", "cp", "s3://kept/versioned", back.to_str().unwrap()],
        &env,
    );
    assert!(
        std::fs::read(&back).unwrap() == first,
        "the older version is current again"
    );

    // delete-objects: present and missing keys, each deleted; then `aws s3 rm --recursive`,
    // which deletes in batches.
    for key in ["batch/1", "batch/2"] {
        aws(
            &mut cell,
            &[
                "s3",
                "cp",
                small_file.to_str().unwrap(),
                &format!("s3://copies/{key}"),
            ],
            &env,
        );
    }
    let delete = write(
        dir,
        "delete.json",
        br#"{"Objects":[{"Key":"batch/1"},{"Key":"batch/2"},{"Key":"batch/missing"}]}"#,
    );
    let deleted = query(
        &mut cell,
        &[
            "s3api",
            "delete-objects",
            "--bucket",
            "copies",
            "--delete",
            &format!("file://{}", delete.display()),
        ],
        "Deleted[].Key",
        &env,
    );
    assert_eq!(
        deleted.split_whitespace().collect::<Vec<_>>(),
        ["batch/1", "batch/2", "batch/missing"],
        "{deleted}"
    );
    aws(
        &mut cell,
        &["s3", "rm", "--recursive", "s3://copies/"],
        &env,
    );
    let left = text(&aws(&mut cell, &["s3", "ls", "s3://copies/"], &env));
    assert!(left.is_empty(), "{left}");

    // boto3's half.
    let script = write(dir, "boto89.py", BOTO_89.as_bytes());
    let said = run(
        &mut cell,
        PYTHON,
        &["-I", script.to_str().unwrap(), dir.to_str().unwrap()],
        &env,
        None,
    );
    println!("{}", text(&said));
    assert!(
        text(&said).ends_with("boto3 research/89 passed"),
        "{}",
        text(&said)
    );
}

/// boto3 against research/89: every metadata field round-tripped, copies under each directive and
/// condition and across buckets, the managed copy past its threshold, versions by ID, and
/// delete_objects, each error code as S3's.
const BOTO_89: &str = r#"
import io
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

s3 = boto3.Session(profile_name="mantle").client("s3", config=Config(s3={"addressing_style": "path"}))

def body(n, size):
    return bytes(((i * 0x2545F491 + n * 7) >> 8) & 0xFF for i in range(size))

def code(call, **kwargs):
    try:
        call(**kwargs)
    except ClientError as e:
        return e.response["Error"]["Code"], e.response["ResponseMetadata"]["HTTPStatusCode"]
    raise AssertionError(f"{call.__name__} {kwargs} succeeded")

s3.create_bucket(Bucket="boto89")
s3.create_bucket(Bucket="boto89b")

# Every metadata field round-tripped by put_object and head_object and get_object.
data = body(50, 77_777)
s3.put_object(
    Bucket="boto89", Key="m", Body=data,
    ContentType="application/json", ContentEncoding="identity",
    ContentDisposition='attachment; filename="m.json"', ContentLanguage="fr",
    CacheControl="no-cache", Expires="Wed, 01 Dec 2094 16:00:00 GMT",
    Metadata={"Owner": "kiln", "empty": "", "list": "a,b"},
)
for read in (s3.head_object(Bucket="boto89", Key="m"), s3.get_object(Bucket="boto89", Key="m")):
    assert read["ContentType"] == "application/json", read
    assert read["ContentEncoding"] == "identity", read
    assert read["ContentDisposition"] == 'attachment; filename="m.json"', read
    assert read["ContentLanguage"] == "fr", read
    assert read["CacheControl"] == "no-cache", read
    assert read["ResponseMetadata"]["HTTPHeaders"]["expires"] == "Wed, 01 Dec 2094 16:00:00 GMT", read
    assert read["Metadata"] == {"owner": "kiln", "empty": "", "list": "a,b"}, read
assert s3.get_object(Bucket="boto89", Key="m")["Body"].read() == data
overridden = s3.get_object(Bucket="boto89", Key="m", ResponseContentType="text/plain")
assert overridden["ContentType"] == "text/plain", overridden
assert code(s3.put_object, Bucket="boto89", Key="big-meta", Body=b"x", Metadata={"k": "v" * 2048}) == ("MetadataTooLarge", 400)
# A version's tag set is the empty one; a write naming tags is refused, not dropped (research/89 §3.8).
assert s3.get_object_tagging(Bucket="boto89", Key="m")["TagSet"] == []
assert code(s3.put_object, Bucket="boto89", Key="tagged", Body=b"x", Tagging="k=v") == ("NotImplemented", 501)

# CopyObject: COPY keeps the metadata, REPLACE takes the request's, across buckets; the source's
# conditions; a copy onto itself; a missing source.
etag = s3.head_object(Bucket="boto89", Key="m")["ETag"]
copied = s3.copy_object(Bucket="boto89b", Key="c", CopySource={"Bucket": "boto89", "Key": "m"})
assert copied["CopyObjectResult"]["ETag"] == etag, copied
assert s3.get_object(Bucket="boto89b", Key="c")["Body"].read() == data
assert s3.head_object(Bucket="boto89b", Key="c")["Metadata"]["owner"] == "kiln"
s3.copy_object(Bucket="boto89b", Key="r", CopySource="boto89/m", MetadataDirective="REPLACE",
               ContentType="image/gif", Metadata={"fresh": "1"})
replaced = s3.head_object(Bucket="boto89b", Key="r")
assert replaced["ContentType"] == "image/gif" and replaced["Metadata"] == {"fresh": "1"}, replaced
s3.copy_object(Bucket="boto89b", Key="ok", CopySource="boto89/m", CopySourceIfMatch=etag)
assert code(s3.copy_object, Bucket="boto89b", Key="no", CopySource="boto89/m", CopySourceIfMatch='"0123"') == ("PreconditionFailed", 412)
assert code(s3.copy_object, Bucket="boto89b", Key="no", CopySource="boto89/m", CopySourceIfNoneMatch=etag) == ("PreconditionFailed", 412)
assert code(s3.copy_object, Bucket="boto89", Key="m", CopySource="boto89/m") == ("InvalidRequest", 400)
s3.copy_object(Bucket="boto89", Key="m", CopySource="boto89/m", MetadataDirective="REPLACE", Metadata={"again": "yes"})
assert s3.head_object(Bucket="boto89", Key="m")["Metadata"] == {"again": "yes"}
assert code(s3.copy_object, Bucket="boto89b", Key="no", CopySource="boto89/none") == ("NoSuchKey", 404)
assert code(s3.copy_object, Bucket="boto89b", Key="no", CopySource="nobucket89/m") == ("NoSuchBucket", 404)

# The managed copy past its multipart threshold: UploadPartCopy of ranges.
large = body(51, 9 * 1024 * 1024 + 11)
s3.put_object(Bucket="boto89", Key="large", Body=large, Metadata={"size": "large"})
s3.copy({"Bucket": "boto89", "Key": "large"}, "boto89b", "large")
assert s3.get_object(Bucket="boto89b", Key="large")["Body"].read() == large
assert s3.head_object(Bucket="boto89b", Key="large")["Metadata"] == {"size": "large"}

# Versions: by ID, a delete marker named is 405, an ID no version has 404, one mantle never makes
# 400; a version removed by its ID.
s3.put_bucket_versioning(Bucket="boto89", VersioningConfiguration={"Status": "Enabled"})
v1 = s3.put_object(Bucket="boto89", Key="v", Body=b"one")["VersionId"]
v2 = s3.put_object(Bucket="boto89", Key="v", Body=b"two")["VersionId"]
assert s3.get_object(Bucket="boto89", Key="v", VersionId=v1)["Body"].read() == b"one"
assert s3.head_object(Bucket="boto89", Key="v", VersionId=v2)["VersionId"] == v2
marker = s3.delete_object(Bucket="boto89", Key="v")
assert marker["DeleteMarker"] is True, marker
assert code(s3.get_object, Bucket="boto89", Key="v", VersionId=marker["VersionId"]) == ("MethodNotAllowed", 405)
assert code(s3.get_object, Bucket="boto89", Key="v", VersionId="0000000000001") == ("NoSuchVersion", 404)
assert code(s3.get_object, Bucket="boto89", Key="v", VersionId="not-a-version") == ("InvalidArgument", 400)
listed = s3.list_object_versions(Bucket="boto89", Prefix="v")
assert [v["VersionId"] for v in listed["Versions"]] == [v2, v1], listed
assert [m["VersionId"] for m in listed["DeleteMarkers"]] == [marker["VersionId"]], listed
assert listed["DeleteMarkers"][0]["IsLatest"] is True, listed
removed = s3.delete_object(Bucket="boto89", Key="v", VersionId=marker["VersionId"])
assert removed["DeleteMarker"] is True and removed["VersionId"] == marker["VersionId"], removed
assert s3.get_object(Bucket="boto89", Key="v")["Body"].read() == b"two"
s3.copy_object(Bucket="boto89b", Key="old", CopySource={"Bucket": "boto89", "Key": "v", "VersionId": v1})
assert s3.get_object(Bucket="boto89b", Key="old")["Body"].read() == b"one"

# DeleteObjects, with the SDK's default checksum in Content-MD5's place: present, missing and a
# version, verbose and quiet.
for key in ("d1", "d2"):
    s3.put_object(Bucket="boto89b", Key=key, Body=b"x")
result = s3.delete_objects(Bucket="boto89b", Delete={"Objects": [{"Key": "d1"}, {"Key": "d2"}, {"Key": "missing"}]})
assert sorted(d["Key"] for d in result["Deleted"]) == ["d1", "d2", "missing"], result
assert "Errors" not in result, result
quiet = s3.delete_objects(Bucket="boto89", Delete={"Objects": [{"Key": "v", "VersionId": v1}], "Quiet": True})
assert "Deleted" not in quiet and "Errors" not in quiet, quiet
assert code(s3.get_object, Bucket="boto89", Key="v", VersionId=v1) == ("NoSuchVersion", 404)
failed = s3.delete_objects(Bucket="boto89", Delete={"Objects": [{"Key": "v", "VersionId": "bad"}], "Quiet": True})
assert [e["Code"] for e in failed["Errors"]] == ["InvalidArgument"], failed
print("boto3 research/89 passed")
"#;
