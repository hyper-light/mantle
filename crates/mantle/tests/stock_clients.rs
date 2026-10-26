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
/// must say it exited 0. A program the runner lacks fails here.
fn run(program: &str, args: &[&str], env: &[(String, String)], input: Option<&[u8]>) -> Output {
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
    let mut child = command
        .spawn()
        .unwrap_or_else(|e| panic!("{program}: {e}: the runner must have it"));
    if let Some(input) = input {
        child.stdin.take().unwrap().write_all(input).unwrap();
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{program} {args:?}: {}\n{}{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn aws(args: &[&str], env: &[(String, String)]) -> Output {
    run("aws", args, env, None)
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
fn query(args: &[&str], query: &str, env: &[(String, String)]) -> String {
    let mut all = args.to_vec();
    all.extend(["--query", query, "--output", "text"]);
    text(&aws(&all, env))
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
fn crt_transfers(dir: &Path, env: &[(String, String)]) {
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
        aws(&["s3", "cp", file.to_str().unwrap(), &key], &crt);
        let back = dir.join(format!("back-{client}.bin"));
        aws(&["s3", "cp", &key, back.to_str().unwrap()], &crt);
        assert!(
            std::fs::read(&back).unwrap() == data,
            "{client} by the CRT client"
        );
        aws(&["s3", "rm", &key], &crt);
    }
}

#[test]
fn the_aws_cli_and_boto3_pass_against_a_cell_of_three() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path();
    let (_cell, env) = started(dir);
    let version = text(&aws(&["--version"], &env));
    println!("{version}");

    // aws s3 by each of the CLI's transfer clients, botocore's and the CRT's, named rather than left
    // to the CLI's choice: a small object and a large one up and down, each read back
    // byte-identical. The CRT's client takes an http endpoint, and is `aws s3`'s default on every
    // host, from the CLI's 2.38.0 ("Disable TLS in CRT client when resolved endpoint scheme is
    // http"; "Enable the CRT S3 transfer client by default on all hosts", the CLI's changelog); an
    // older CLI's default is botocore's, which the commands below run.
    aws(&["s3", "mb", "s3://stock"], &env);
    if cli_version(&version) >= (2, 38, 0) {
        crt_transfers(dir, &env);
    }

    // aws s3: a bucket, a small object and a large one up, listed, down, compared, and removed.
    let small = bytes(1, 100_003);
    let large = bytes(2, THRESHOLD * 2 + 12_345);
    let small_file = write(dir, "small.bin", &small);
    let large_file = write(dir, "large.bin", &large);
    aws(
        &[
            "s3",
            "cp",
            small_file.to_str().unwrap(),
            "s3://stock/small.bin",
        ],
        &env,
    );
    aws(
        &[
            "s3",
            "cp",
            large_file.to_str().unwrap(),
            "s3://stock/large.bin",
        ],
        &env,
    );
    let listed = text(&aws(&["s3", "ls", "s3://stock/"], &env));
    assert!(
        listed.contains("small.bin") && listed.contains("large.bin"),
        "{listed}"
    );
    for (key, data) in [("small.bin", &small), ("large.bin", &large)] {
        let back = dir.join(format!("back-{key}"));
        aws(
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
    let etag = query(&head, "ETag", &env);
    assert!(etag.trim_matches('"').ends_with("-3"), "{etag}");
    // From standard input and to standard output, past the threshold.
    let piped = bytes(3, THRESHOLD + 777);
    run(
        "aws",
        &["s3", "cp", "-", "s3://stock/piped.bin"],
        &env,
        Some(&piped),
    );
    let out = aws(&["s3", "cp", "s3://stock/piped.bin", "-"], &env);
    assert!(out.stdout == piped, "piped.bin read to standard output");
    aws(&["s3", "rm", "s3://stock/small.bin"], &env);
    let listed = text(&aws(&["s3", "ls", "s3://stock/"], &env));
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
    let etag = query(&put, "ETag", &env);
    assert!(!etag.trim_matches('"').is_empty(), "{etag}");
    let got = dir.join("got.bin");
    aws(
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
    assert_eq!(query(&head, "ContentLength", &env), one.len().to_string());
    let list = ["s3api", "list-objects-v2", "--bucket", "stock"];
    let keys = query(&list, "Contents[].Key", &env);
    assert!(
        keys.contains("one.bin") && keys.contains("piped.bin"),
        "{keys}"
    );
    aws(
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
    let upload = query(&create, "UploadId", &env);
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
        tags.push(query(&part, "ETag", &env).trim_matches('"').to_owned());
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
    let numbers = query(&list, "Parts[].PartNumber", &env);
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
    let dropped = query(&create, "UploadId", &env);
    aws(
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
        &["s3api", "list-multipart-uploads", "--bucket", "stock"],
        &env,
    ));
    assert!(!uploads.contains(&dropped), "{uploads}");

    // boto3: object calls, a listing, and the managed transfers, with the SDK's default checksums.
    let script = write(dir, "boto.py", BOTO.as_bytes());
    let said = run(
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
