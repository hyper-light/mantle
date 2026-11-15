//! Principals with keys of their own against a running cell (docs/design/principals.md): a cell of
//! three `mantle serve` processes made by `mantle init`, its operator's client made by `mantle init
//! client`, which acts for the account's root. The operator makes principals with IAM identity
//! policies and keys for them with `mantle principal` and `mantle key`; the AWS CLI v2 and boto3,
//! signing with those keys, are let in exactly as each policy grants and refused `AccessDenied`
//! otherwise, through every node; a revoked key is refused `InvalidAccessKeyId` by every node from
//! the revoke's answer on, with no wait; and a key is rotated under a client that never stops
//! asking, none of its requests refused. A tool the runner lacks fails the suite: it never skips.
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

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};

use cell::*;

/// The Python the runner image has: `python` on Windows, `python3` elsewhere.
const PYTHON: &str = if cfg!(windows) { "python" } else { "python3" };

/// The home-directory policy IAM's own example writes (research/91 §3): a principal lists and
/// works under `home/` and its own name alone, `${aws:username}` standing for that name.
const HOME: &str = r#"{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "ListOwnHome", "Effect": "Allow", "Action": "s3:ListBucket",
     "Resource": "arn:aws:s3:::artifacts",
     "Condition": {"StringLike": {"s3:prefix": ["home/${aws:username}/*"]}}},
    {"Sid": "OwnHome", "Effect": "Allow",
     "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload",
                "s3:ListMultipartUploadParts"],
     "Resource": "arn:aws:s3:::artifacts/home/${aws:username}/*"}
  ]
}"#;

/// A reader of one bucket that lists the account's buckets.
const READER: &str = r#"{"Version": "2012-10-17", "Statement": [
  {"Effect": "Allow", "Action": "s3:ListAllMyBuckets", "Resource": "*"},
  {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetObject"],
   "Resource": ["arn:aws:s3:::artifacts", "arn:aws:s3:::artifacts/*"]}
]}"#;

/// Everything but one prefix, which a deny takes back whatever allows it (research/91 §3).
const FENCED: &str = r#"{"Version": "2012-10-17", "Statement": [
  {"Effect": "Allow", "Action": "s3:*", "Resource": "*"},
  {"Effect": "Deny", "Action": "s3:*", "Resource": "arn:aws:s3:::artifacts/home/runner-8/*"}
]}"#;

/// A service's own prefix, which the rotation's client works under.
const SERVICE: &str = r#"{"Version": "2012-10-17", "Statement": [
  {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"],
   "Resource": "arn:aws:s3:::artifacts/svc/*"}
]}"#;

/// `program` with `args`, its environment `env`: its output, whatever it exited with. A program the
/// runner lacks fails here.
fn run(program: &str, args: &[&str], env: &[(String, String)]) -> Output {
    Command::new(program)
        .args(args)
        .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .stdin(Stdio::null())
        .output()
        .unwrap_or_else(|e| panic!("{program}: {e}: the runner must have it"))
}

fn said(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// What `program` said, which must be that it exited 0.
fn ok(program: &str, args: &[&str], env: &[(String, String)]) -> String {
    let output = run(program, args, env);
    assert!(
        output.status.success(),
        "{program} {args:?}: {}\n{}",
        output.status,
        said(&output)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

/// `program` refused with S3's or IAM's `code`, as the AWS CLI and mantle each name it.
fn refused(program: &str, args: &[&str], env: &[(String, String)], code: &str) {
    let output = run(program, args, env);
    let said = said(&output);
    assert!(
        !output.status.success(),
        "{program} {args:?} was let: {said}"
    );
    assert!(
        said.contains(code),
        "{program} {args:?}: not {code}: {said}"
    );
}

/// The operator's `mantle` as the client of `client`.
fn mantle(client: &Path, args: &[&str]) -> String {
    let mut all = vec!["--client", client.to_str().unwrap()];
    all.extend_from_slice(args);
    ok(NODE, &all, &[])
}

fn mantle_refused(client: &Path, args: &[&str], code: &str) {
    let mut all = vec!["--client", client.to_str().unwrap()];
    all.extend_from_slice(args);
    refused(NODE, &all, &[], code);
}

/// The client's directory's config, each `node` line naming where its node bound.
fn point_at(dir: &Path, bound: &[(u64, std::net::SocketAddr)]) {
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

/// The operator's client, made by `mantle init client` with the cell's authority: the account's
/// root on the native protocol (node.md §3.6).
fn operator(cell: &Cell) -> PathBuf {
    let dir = cell.out().parent().unwrap().join("operator");
    let made = run(
        NODE,
        &[
            "init",
            "client",
            dir.to_str().unwrap(),
            "--cell",
            cell.out().to_str().unwrap(),
        ],
        &[],
    );
    assert!(made.status.success(), "mantle init client: {}", said(&made));
    point_at(&dir, &cell.bound_quic());
    dir
}

/// A cell of three, each node enlisted; its endpoints' URLs, a node's each; and the AWS CLI's and
/// boto3's environment, the profile `mantle` naming the cell's region and path-style addressing,
/// with credentials from `credentials`.
struct Started {
    cell: Cell,
    endpoints: Vec<String>,
    config: PathBuf,
}

impl Started {
    fn new(root: &Path) -> Self {
        let mut cell = Cell::made(three_founders);
        let ids: Vec<u64> = cell.dirs.keys().copied().collect();
        for &id in &ids {
            cell.enlisted(id);
        }
        let endpoints: Vec<String> = ids
            .iter()
            .map(|id| format!("http://{}", cell.s3_bound[id].0))
            .collect();
        let (_, region, _) = &cell.s3[&ids[0]];
        let config = root.join("config");
        std::fs::write(
            &config,
            format!(
                "[profile mantle]\nregion = {region}\ns3 =\n  addressing_style = path\n  preferred_transfer_client = classic\n"
            ),
        )
        .unwrap();
        Self {
            cell,
            endpoints,
            config,
        }
    }

    /// The stock clients' environment signing with the key in `credentials`.
    fn env(&self, credentials: &Path) -> Vec<(String, String)> {
        vec![
            ("AWS_CONFIG_FILE".into(), self.config.display().to_string()),
            (
                "AWS_SHARED_CREDENTIALS_FILE".into(),
                credentials.display().to_string(),
            ),
            ("AWS_PROFILE".into(), "mantle".into()),
            // No instance metadata to look for, no pager to wait on.
            ("AWS_EC2_METADATA_DISABLED".into(), "true".into()),
            ("AWS_PAGER".into(), String::new()),
        ]
    }

    /// The account's root's key, as `mantle init` wrote it to the operator's profile.
    fn root(&self) -> PathBuf {
        self.cell.out().join("s3").join("credentials")
    }
}

/// `aws s3api ARGS` at `endpoint`.
fn api(endpoint: &str, args: &[&str]) -> Vec<String> {
    let mut all: Vec<String> = vec!["s3api".into(), "--endpoint-url".into(), endpoint.into()];
    all.extend(args.iter().map(|a| (*a).to_owned()));
    all
}

fn aws(env: &[(String, String)], endpoint: &str, args: &[&str]) -> String {
    let all = api(endpoint, args);
    let all: Vec<&str> = all.iter().map(String::as_str).collect();
    ok("aws", &all, env)
}

fn aws_refused(env: &[(String, String)], endpoint: &str, args: &[&str], code: &str) {
    let all = api(endpoint, args);
    let all: Vec<&str> = all.iter().map(String::as_str).collect();
    refused("aws", &all, env, &format!("({code})"));
}

/// The access key id a credentials file holds.
fn key_id(credentials: &Path) -> String {
    std::fs::read_to_string(credentials)
        .unwrap()
        .lines()
        .find_map(|l| l.strip_prefix("aws_access_key_id = "))
        .unwrap()
        .to_owned()
}

/// An argument list as `aws` takes it.
fn strs(args: &[String]) -> Vec<&str> {
    args.iter().map(String::as_str).collect()
}

fn write(dir: &Path, name: &str, data: &[u8]) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, data).unwrap();
    path
}

/// Principals are let in exactly as their identity policies grant, through every node, by the AWS
/// CLI and boto3; the operator's commands refuse what IAM refuses; a revoked key is refused by every
/// node at once; and a key rotated under a client asking all along refuses none of its requests.
#[test]
fn principals_are_let_in_as_granted_revoked_at_once_and_rotated_with_no_request_refused() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path();
    let started = Started::new(dir);
    let operator = operator(&started.cell);
    let first = started.endpoints[0].clone();
    let root_env = started.env(&started.root());
    println!("{}", ok("aws", &["--version"], &root_env));

    // The account's root makes the buckets and an object under each home.
    aws(
        &root_env,
        &first,
        &["create-bucket", "--bucket", "artifacts"],
    );
    aws(&root_env, &first, &["create-bucket", "--bucket", "other"]);
    let eight = object_bytes(8, 40_001);
    let eight_file = write(dir, "eight.bin", &eight);
    for key in ["home/runner-7/a.bin", "home/runner-8/a.bin"] {
        aws(
            &root_env,
            &first,
            &[
                "put-object",
                "--bucket",
                "artifacts",
                "--key",
                key,
                "--body",
                eight_file.to_str().unwrap(),
            ],
        );
    }
    aws(
        &root_env,
        &first,
        &[
            "put-object",
            "--bucket",
            "other",
            "--key",
            "x.bin",
            "--body",
            eight_file.to_str().unwrap(),
        ],
    );

    // The operator's principals, each with its policy, and a key each.
    let policies = dir.join("policies");
    std::fs::create_dir(&policies).unwrap();
    let credentials = dir.join("credentials");
    std::fs::create_dir(&credentials).unwrap();
    for (name, policy) in [
        ("runner-7", HOME),
        ("runner-8", HOME),
        ("reader", READER),
        ("fenced", FENCED),
        ("svc", SERVICE),
    ] {
        let file = write(&policies, &format!("{name}.json"), policy.as_bytes());
        let made = mantle(
            &operator,
            &[
                "principal",
                "create",
                name,
                "--policy",
                file.to_str().unwrap(),
            ],
        );
        assert_eq!(made, format!("made principal {name}"));
        let out = credentials.join(name);
        let made = mantle(
            &operator,
            &["key", "create", name, "--out", out.to_str().unwrap()],
        );
        assert!(made.starts_with("made key "), "{made}");
        assert!(made.contains(&format!(" for {name} in ")), "{made}");
    }
    let creds = |name: &str| credentials.join(name);

    // What IAM refuses of an operator is refused: a name taken without regard to case, a policy
    // naming a principal, a third key, a delete while keys are held, a key over a file that exists.
    let named = write(
        &policies,
        "named.json",
        br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"*"}]}"#,
    );
    mantle_refused(
        &operator,
        &[
            "principal",
            "create",
            "intruder",
            "--policy",
            named.to_str().unwrap(),
        ],
        "should not specify a principal",
    );
    let home = policies.join("runner-7.json");
    mantle_refused(
        &operator,
        &[
            "principal",
            "create",
            "RUNNER-7",
            "--policy",
            home.to_str().unwrap(),
        ],
        "EntityAlreadyExists",
    );
    let spare = dir.join("spare");
    mantle(
        &operator,
        &["key", "create", "reader", "--out", spare.to_str().unwrap()],
    );
    let third = dir.join("third");
    mantle_refused(
        &operator,
        &["key", "create", "reader", "--out", third.to_str().unwrap()],
        "LimitExceeded",
    );
    assert!(!third.exists(), "no secret written for a key not made");
    mantle_refused(
        &operator,
        &["principal", "delete", "reader"],
        "DeleteConflict",
    );
    mantle_refused(
        &operator,
        &[
            "key",
            "create",
            "svc",
            "--out",
            creds("svc").to_str().unwrap(),
        ],
        "exists",
    );
    mantle(&operator, &["key", "revoke", &key_id(&spare)]);

    // The operator lists principals and keys, never a secret, and reads a policy back.
    let listed = mantle(&operator, &["principal", "list"]);
    let names: Vec<&str> = listed
        .lines()
        .map(|l| l.split_whitespace().nth(1).unwrap())
        .collect();
    assert_eq!(
        names,
        ["fenced", "reader", "runner-7", "runner-8", "svc"],
        "{listed}"
    );
    let keys = mantle(&operator, &["key", "list"]);
    assert_eq!(keys.lines().count(), 5, "{keys}");
    for name in ["runner-7", "runner-8", "reader", "fenced", "svc"] {
        let id = key_id(&creds(name));
        assert!(
            keys.lines().any(|l| l.contains(&id) && l.ends_with(name)),
            "{keys}"
        );
        let secret = std::fs::read_to_string(creds(name)).unwrap();
        let secret = secret
            .lines()
            .find_map(|l| l.strip_prefix("aws_secret_access_key = "))
            .unwrap();
        assert!(!keys.contains(secret) && !listed.contains(secret));
    }
    let shown = mantle(&operator, &["principal", "show", "Runner-7"]);
    assert!(shown.starts_with("principal runner-7\n"), "{shown}");
    assert!(shown.contains("home/${aws:username}/*"), "{shown}");
    #[cfg(unix)]
    for name in ["runner-7", "svc"] {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(creds(name)).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0, "{name}'s credentials readable by others");
    }

    // The AWS CLI under each principal, through every node: what its policy grants, and
    // `AccessDenied` for the rest.
    let back = dir.join("back.bin");
    let back_path = back.to_str().unwrap();
    for endpoint in &started.endpoints {
        let seven = started.env(&creds("runner-7"));
        aws(
            &seven,
            endpoint,
            &[
                "put-object",
                "--bucket",
                "artifacts",
                "--key",
                "home/runner-7/cli.bin",
                "--body",
                eight_file.to_str().unwrap(),
            ],
        );
        aws(
            &seven,
            endpoint,
            &[
                "get-object",
                "--bucket",
                "artifacts",
                "--key",
                "home/runner-7/a.bin",
                back_path,
            ],
        );
        assert!(std::fs::read(&back).unwrap() == eight);
        let listed = aws(
            &seven,
            endpoint,
            &[
                "list-objects-v2",
                "--bucket",
                "artifacts",
                "--prefix",
                "home/runner-7/",
                "--query",
                "Contents[].Key",
                "--output",
                "text",
            ],
        );
        assert!(listed.contains("home/runner-7/a.bin") && listed.contains("home/runner-7/cli.bin"));
        for denied in [
            vec![
                "list-objects-v2",
                "--bucket",
                "artifacts",
                "--prefix",
                "home/runner-8/",
            ],
            vec!["list-objects-v2", "--bucket", "artifacts"],
            vec![
                "get-object",
                "--bucket",
                "artifacts",
                "--key",
                "home/runner-8/a.bin",
                back_path,
            ],
            vec![
                "put-object",
                "--bucket",
                "artifacts",
                "--key",
                "home/runner-8/cli.bin",
                "--body",
                eight_file.to_str().unwrap(),
            ],
            vec![
                "get-object",
                "--bucket",
                "other",
                "--key",
                "x.bin",
                back_path,
            ],
            vec!["list-buckets"],
            vec!["create-bucket", "--bucket", "seven"],
            vec!["delete-bucket", "--bucket", "other"],
        ] {
            aws_refused(&seven, endpoint, &denied, "AccessDenied");
        }

        let reader = started.env(&creds("reader"));
        let buckets = aws(
            &reader,
            endpoint,
            &[
                "list-buckets",
                "--query",
                "Buckets[].Name",
                "--output",
                "text",
            ],
        );
        assert_eq!(
            buckets.split_whitespace().collect::<Vec<_>>(),
            ["artifacts", "other"]
        );
        aws(
            &reader,
            endpoint,
            &[
                "get-object",
                "--bucket",
                "artifacts",
                "--key",
                "home/runner-8/a.bin",
                back_path,
            ],
        );
        assert!(std::fs::read(&back).unwrap() == eight);
        for denied in [
            vec![
                "put-object",
                "--bucket",
                "artifacts",
                "--key",
                "reader.bin",
                "--body",
                eight_file.to_str().unwrap(),
            ],
            vec![
                "delete-object",
                "--bucket",
                "artifacts",
                "--key",
                "home/runner-7/a.bin",
            ],
            vec![
                "get-object",
                "--bucket",
                "other",
                "--key",
                "x.bin",
                back_path,
            ],
            vec!["list-objects-v2", "--bucket", "other"],
        ] {
            aws_refused(&reader, endpoint, &denied, "AccessDenied");
        }

        let fenced = started.env(&creds("fenced"));
        aws(
            &fenced,
            endpoint,
            &[
                "get-object",
                "--bucket",
                "artifacts",
                "--key",
                "home/runner-7/a.bin",
                back_path,
            ],
        );
        aws(
            &fenced,
            endpoint,
            &[
                "put-object",
                "--bucket",
                "other",
                "--key",
                "fenced.bin",
                "--body",
                eight_file.to_str().unwrap(),
            ],
        );
        for denied in [
            vec![
                "get-object",
                "--bucket",
                "artifacts",
                "--key",
                "home/runner-8/a.bin",
                back_path,
            ],
            vec![
                "put-object",
                "--bucket",
                "artifacts",
                "--key",
                "home/runner-8/fenced.bin",
                "--body",
                eight_file.to_str().unwrap(),
            ],
            vec![
                "delete-object",
                "--bucket",
                "artifacts",
                "--key",
                "home/runner-8/a.bin",
            ],
        ] {
            aws_refused(&fenced, endpoint, &denied, "AccessDenied");
        }
    }
    // What the principals made is the account's: its root reads it, and lists it.
    aws(
        &root_env,
        &first,
        &[
            "get-object",
            "--bucket",
            "other",
            "--key",
            "fenced.bin",
            back_path,
        ],
    );
    assert!(std::fs::read(&back).unwrap() == eight);

    // A copy is judged on both its objects, as S3 judges one: `s3:GetObject` on its source and
    // `s3:PutObject` on its destination (research/91 §6). runner-7 copies within its home, and a
    // copy of runner-8's object into its home is refused, by CopyObject and by UploadPartCopy, so a
    // copy reads nothing its policy does not let it read.
    let seven = started.env(&creds("runner-7"));
    let (second, third) = (&started.endpoints[1], &started.endpoints[2]);
    let copy = |key: &str, from: &str| {
        vec![
            "copy-object".to_owned(),
            "--bucket".into(),
            "artifacts".into(),
            "--key".into(),
            key.into(),
            "--copy-source".into(),
            format!("artifacts/{from}"),
        ]
    };
    aws(
        &seven,
        &first,
        &strs(&copy("home/runner-7/copy.bin", "home/runner-7/a.bin")),
    );
    aws(
        &seven,
        &first,
        &[
            "get-object",
            "--bucket",
            "artifacts",
            "--key",
            "home/runner-7/copy.bin",
            back_path,
        ],
    );
    assert!(std::fs::read(&back).unwrap() == eight);
    aws_refused(
        &seven,
        &first,
        &strs(&copy("home/runner-7/stolen.bin", "home/runner-8/a.bin")),
        "AccessDenied",
    );
    aws_refused(
        &seven,
        &first,
        &[
            "head-object",
            "--bucket",
            "artifacts",
            "--key",
            "home/runner-7/stolen.bin",
        ],
        "404",
    );
    let upload = aws(
        &seven,
        second,
        &[
            "create-multipart-upload",
            "--bucket",
            "artifacts",
            "--key",
            "home/runner-7/parts.bin",
            "--query",
            "UploadId",
            "--output",
            "text",
        ],
    );
    let part_copy = |from: &str| {
        vec![
            "upload-part-copy".to_owned(),
            "--bucket".into(),
            "artifacts".into(),
            "--key".into(),
            "home/runner-7/parts.bin".into(),
            "--upload-id".into(),
            upload.trim().into(),
            "--part-number".into(),
            "1".into(),
            "--copy-source".into(),
            format!("artifacts/{from}"),
        ]
    };
    aws_refused(
        &seven,
        second,
        &strs(&part_copy("home/runner-8/a.bin")),
        "AccessDenied",
    );
    aws(&seven, second, &strs(&part_copy("home/runner-7/a.bin")));
    aws(
        &seven,
        second,
        &[
            "abort-multipart-upload",
            "--bucket",
            "artifacts",
            "--key",
            "home/runner-7/parts.bin",
            "--upload-id",
            upload.trim(),
        ],
    );

    // A batch delete judges each of its objects as a delete of its own, answers the one its
    // policy refuses `AccessDenied` in its place, and deletes the rest, verbose or quiet
    // (research/91 §6).
    let deletes = |objects: &str| {
        vec![
            "delete-objects".to_owned(),
            "--bucket".into(),
            "artifacts".into(),
            "--delete".into(),
            objects.into(),
            "--query".into(),
            "[Deleted[].Key, Errors[].[Key, Code]]".into(),
            "--output".into(),
            "json".into(),
        ]
    };
    let compact = |json: String| json.split_whitespace().collect::<String>();
    let answered = aws(
        &seven,
        third,
        &strs(&deletes(
            r#"{"Objects": [{"Key": "home/runner-7/copy.bin"}, {"Key": "home/runner-8/a.bin"}]}"#,
        )),
    );
    assert_eq!(
        compact(answered),
        r#"[["home/runner-7/copy.bin"],[["home/runner-8/a.bin","AccessDenied"]]]"#
    );
    let answered = aws(
        &seven,
        third,
        &strs(&deletes(
            r#"{"Objects": [{"Key": "home/runner-8/a.bin"}], "Quiet": true}"#,
        )),
    );
    assert_eq!(
        compact(answered),
        r#"[null,[["home/runner-8/a.bin","AccessDenied"]]]"#
    );
    aws_refused(
        &seven,
        third,
        &[
            "head-object",
            "--bucket",
            "artifacts",
            "--key",
            "home/runner-7/copy.bin",
        ],
        "404",
    );
    aws(
        &started.env(&creds("runner-8")),
        third,
        &[
            "get-object",
            "--bucket",
            "artifacts",
            "--key",
            "home/runner-8/a.bin",
            back_path,
        ],
    );
    assert!(std::fs::read(&back).unwrap() == eight);

    // A policy replaced is judged by at every node from the answer on, as a revocation is: the
    // reader loses one home, and keeps the rest of the bucket.
    let narrowed = write(
        &policies,
        "reader-narrowed.json",
        br#"{"Version": "2012-10-17", "Statement": [
          {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetObject"],
           "Resource": ["arn:aws:s3:::artifacts", "arn:aws:s3:::artifacts/*"]},
          {"Effect": "Deny", "Action": "s3:GetObject",
           "Resource": "arn:aws:s3:::artifacts/home/runner-7/*"}]}"#,
    );
    assert_eq!(
        mantle(
            &operator,
            &["principal", "policy", "reader", narrowed.to_str().unwrap()]
        ),
        "set the policy of reader"
    );
    let reader = started.env(&creds("reader"));
    for endpoint in &started.endpoints {
        aws_refused(
            &reader,
            endpoint,
            &[
                "get-object",
                "--bucket",
                "artifacts",
                "--key",
                "home/runner-7/a.bin",
                back_path,
            ],
            "AccessDenied",
        );
        aws_refused(&reader, endpoint, &["list-buckets"], "AccessDenied");
        aws(
            &reader,
            endpoint,
            &[
                "get-object",
                "--bucket",
                "artifacts",
                "--key",
                "home/runner-8/a.bin",
                back_path,
            ],
        );
    }
    let shown = mantle(&operator, &["principal", "show", "reader"]);
    assert!(shown.contains(r#""Effect":"Deny""#), "{shown}");
    mantle(
        &operator,
        &[
            "principal",
            "policy",
            "reader",
            policies.join("reader.json").to_str().unwrap(),
        ],
    );

    // A principal's bucket, which its policy lets it make, is the account's.
    let fenced = started.env(&creds("fenced"));
    aws(
        &fenced,
        &first,
        &["create-bucket", "--bucket", "made-by-fenced"],
    );
    let owned = aws(
        &root_env,
        &first,
        &[
            "list-buckets",
            "--query",
            "Buckets[].Name",
            "--output",
            "text",
        ],
    );
    assert!(owned.contains("made-by-fenced"), "{owned}");

    // boto3 under the principals, through every node: object calls, listings and a managed
    // multipart transfer within what each policy grants, `AccessDenied` past it.
    let script = write(dir, "boto.py", BOTO.as_bytes());
    let said = ok(
        PYTHON,
        &[
            "-I",
            script.to_str().unwrap(),
            &started.endpoints.join(","),
            credentials.to_str().unwrap(),
        ],
        &started.env(&creds("runner-7")),
    );
    assert!(said.ends_with("boto3 passed"), "{said}");

    // A revoked key is refused by every node from the revoke's answer on, with nothing waited
    // for; the principal's other keys, and every other principal's, are untouched.
    let eight_env = started.env(&creds("runner-8"));
    let get = [
        "get-object",
        "--bucket",
        "artifacts",
        "--key",
        "home/runner-8/a.bin",
        back_path,
    ];
    for endpoint in &started.endpoints {
        aws(&eight_env, endpoint, &get);
    }
    let revoked = key_id(&creds("runner-8"));
    assert_eq!(
        mantle(&operator, &["key", "revoke", &revoked]),
        format!("revoked {revoked}")
    );
    for endpoint in &started.endpoints {
        aws_refused(&eight_env, endpoint, &get, "InvalidAccessKeyId");
        aws(
            &started.env(&creds("runner-7")),
            endpoint,
            &[
                "get-object",
                "--bucket",
                "artifacts",
                "--key",
                "home/runner-7/a.bin",
                back_path,
            ],
        );
    }
    mantle_refused(&operator, &["key", "revoke", &revoked], "NoSuchEntity");
    // A key in another form than the cell draws is no principal's: refused as unknown by every
    // node, no range asked; and so is one in the cell's form the cell never made.
    for (name, id) in [("stranger", "AKIA1"), ("unmade", "ABCDEFGHIJKLMNOPQRST")] {
        let file = write(
            dir,
            name,
            format!(
                "[mantle]\naws_access_key_id = {id}\naws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n"
            )
            .as_bytes(),
        );
        for endpoint in &started.endpoints {
            aws_refused(&started.env(&file), endpoint, &get, "InvalidAccessKeyId");
        }
    }
    // A principal whose keys are revoked is deleted, and its name is free again.
    mantle(&operator, &["principal", "delete", "runner-8"]);
    mantle_refused(
        &operator,
        &["principal", "show", "runner-8"],
        "NoSuchEntity",
    );

    // A key rotated under a client that never stops asking: the client reads its credentials
    // file afresh for each request, as a runner started again does, and none of its requests is
    // refused while the second key is made, written over the file, and the first revoked.
    let svc = creds("svc");
    let first_key = key_id(&svc);
    let kept = dir.join("svc-first");
    std::fs::copy(&svc, &kept).unwrap();
    let stop = dir.join("stop");
    let rotation = write(dir, "rotation.py", ROTATION.as_bytes());
    let mut asking = Asking::new(
        Command::new(PYTHON)
            .args([
                "-I",
                rotation.to_str().unwrap(),
                &started.endpoints.join(","),
                svc.to_str().unwrap(),
                stop.to_str().unwrap(),
            ])
            .envs(started.env(&svc))
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    // Every node answers the first key before the rotation begins.
    asking.until(&first_key, started.endpoints.len());
    let rotated = mantle(
        &operator,
        &["key", "rotate", "svc", "--out", svc.to_str().unwrap()],
    );
    let second_key = key_id(&svc);
    assert_ne!(first_key, second_key);
    assert!(
        rotated.starts_with(&format!("rotated svc from {first_key} to {second_key} in ")),
        "{rotated}"
    );
    // The first key stays valid until it is revoked: both sign, and the client moves to the
    // second, through every node.
    aws(
        &started.env(&kept),
        &first,
        &[
            "get-object",
            "--bucket",
            "artifacts",
            "--key",
            "svc/0",
            back_path,
        ],
    );
    asking.until(&second_key, started.endpoints.len());
    mantle(&operator, &["key", "revoke", &first_key]);
    asking.until(&second_key, started.endpoints.len());
    std::fs::write(&stop, b"").unwrap();
    let (asked, status) = asking.end();
    assert!(status.success(), "the client was refused: {asked:?}");
    assert!(asked.iter().all(|l| l.starts_with("ok ")), "{asked:?}");
    for endpoint in &started.endpoints {
        aws_refused(
            &started.env(&kept),
            endpoint,
            &[
                "get-object",
                "--bucket",
                "artifacts",
                "--key",
                "svc/0",
                back_path,
            ],
            "InvalidAccessKeyId",
        );
    }
    // A rotation replaces the one key a principal holds: with two held it is refused, and with
    // none there is nothing to rotate.
    let svc_out = svc.to_str().unwrap();
    mantle(&operator, &["key", "rotate", "svc", "--out", svc_out]);
    let third_key = key_id(&svc);
    mantle_refused(
        &operator,
        &["key", "rotate", "svc", "--out", svc_out],
        "LimitExceeded",
    );
    mantle(&operator, &["key", "revoke", &second_key]);
    mantle(&operator, &["key", "revoke", &third_key]);
    mantle_refused(
        &operator,
        &["key", "rotate", "svc", "--out", svc_out],
        "NothingToRotate",
    );
}

/// The rotation's client: its lines as it says them, one a request.
struct Asking {
    child: Child,
    lines: std::io::Lines<BufReader<std::process::ChildStdout>>,
    said: Vec<String>,
}

impl Asking {
    fn new(mut child: Child) -> Self {
        let lines = BufReader::new(child.stdout.take().unwrap()).lines();
        Self {
            child,
            lines,
            said: Vec::new(),
        }
    }

    /// Reads the client's lines until it says `count` more requests signed with `key` were let,
    /// failing at a line that says one was refused.
    fn until(&mut self, key: &str, count: usize) {
        let mut seen = 0;
        while seen < count {
            let line = self.lines.next().expect("the client ended").unwrap();
            assert!(line.starts_with("ok "), "the client was refused: {line}");
            if line.split_whitespace().nth(1) == Some(key) {
                seen += 1;
            }
            self.said.push(line);
        }
    }

    /// The client stopped, once told to: every line it said, and how it exited.
    fn end(mut self) -> (Vec<String>, std::process::ExitStatus) {
        for line in self.lines.by_ref() {
            self.said.push(line.unwrap());
        }
        let status = self.child.wait().unwrap();
        (std::mem::take(&mut self.said), status)
    }
}

impl Drop for Asking {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// boto3 under the principals: each call against each node, every object compared with what was
/// put, a call let that its policy refuses, or refused that it grants, an exception and a nonzero
/// exit.
const BOTO: &str = r#"
import io, os, sys
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

endpoints = sys.argv[1].split(",")
credentials = sys.argv[2]
print("boto3", boto3.__version__)

def client(name, endpoint):
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = os.path.join(credentials, name)
    session = boto3.Session(profile_name="mantle")
    return session.client("s3", endpoint_url=endpoint, config=Config(s3={"addressing_style": "path"}))

def body(n, size):
    return bytes(((i * 0x2545F491 + n * 7) >> 8) & 0xFF for i in range(size))

def refused(code, call, **kwargs):
    try:
        call(**kwargs)
    except ClientError as e:
        got = e.response["Error"]["Code"]
        assert got == code, (call.__name__, kwargs, got, e.response["Error"])
        return
    raise AssertionError(f"{call.__name__} {kwargs} was let, not refused {code}")

for n, endpoint in enumerate(endpoints):
    seven = client("runner-7", endpoint)
    small = body(n, 70_001)
    key = f"home/runner-7/boto-{n}.bin"
    seven.put_object(Bucket="artifacts", Key=key, Body=small)
    assert seven.get_object(Bucket="artifacts", Key=key)["Body"].read() == small, key
    listed = [o["Key"] for o in seven.list_objects_v2(Bucket="artifacts", Prefix="home/runner-7/")["Contents"]]
    assert key in listed, listed
    large = body(n + 10, 9 * 1024 * 1024 + 5)
    big = f"home/runner-7/large-{n}.bin"
    seven.upload_fileobj(io.BytesIO(large), "artifacts", big)
    back = io.BytesIO()
    seven.download_fileobj("artifacts", big, back)
    assert back.getvalue() == large, big
    seven.delete_object(Bucket="artifacts", Key=big)
    refused("AccessDenied", seven.list_objects_v2, Bucket="artifacts", Prefix="home/runner-8/")
    refused("AccessDenied", seven.list_objects_v2, Bucket="artifacts")
    refused("AccessDenied", seven.get_object, Bucket="artifacts", Key="home/runner-8/a.bin")
    refused("AccessDenied", seven.put_object, Bucket="artifacts", Key="home/runner-8/boto.bin", Body=b"x")
    refused("AccessDenied", seven.put_object, Bucket="other", Key="boto.bin", Body=b"x")
    refused("AccessDenied", seven.list_buckets)
    refused("AccessDenied", seven.create_multipart_upload, Bucket="artifacts", Key="home/runner-8/parts.bin")

    reader = client("reader", endpoint)
    names = [b["Name"] for b in reader.list_buckets()["Buckets"]]
    assert "artifacts" in names and "other" in names, names
    assert reader.get_object(Bucket="artifacts", Key=key)["Body"].read() == small, key
    refused("AccessDenied", reader.put_object, Bucket="artifacts", Key="reader.bin", Body=b"x")
    refused("AccessDenied", reader.delete_object, Bucket="artifacts", Key=key)
    refused("AccessDenied", reader.get_object, Bucket="other", Key="x.bin")

    fenced = client("fenced", endpoint)
    fenced.put_object(Bucket="other", Key=f"boto-{n}.bin", Body=small)
    refused("AccessDenied", fenced.get_object, Bucket="artifacts", Key="home/runner-8/a.bin")
print("boto3 passed")
"#;

/// The rotation's client: a request a turn, through the nodes in turn, each with the key its
/// credentials file holds when the request is made, read afresh as a runner started again reads
/// it; a line each, `ok KEY`, until the stop file exists. A refusal says why and exits 1.
const ROTATION: &str = r#"
import configparser, os, sys
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

endpoints = sys.argv[1].split(",")
credentials = sys.argv[2]
stop = sys.argv[3]
n = 0
while not os.path.exists(stop):
    endpoint = endpoints[n % len(endpoints)]
    parsed = configparser.ConfigParser()
    parsed.read(credentials)
    key = parsed["mantle"]["aws_access_key_id"]
    secret = parsed["mantle"]["aws_secret_access_key"]
    # The key printed is the key signed with: one read of the file a request.
    session = boto3.Session(profile_name="mantle")
    s3 = session.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        config=Config(s3={"addressing_style": "path"}),
    )
    body = f"request {n}".encode()
    try:
        s3.put_object(Bucket="artifacts", Key=f"svc/{n % 3}", Body=body)
        got = s3.get_object(Bucket="artifacts", Key=f"svc/{n % 3}")["Body"].read()
        assert got == body, (got, body)
    except ClientError as e:
        print("refused", key, endpoint, e.response["Error"], flush=True)
        sys.exit(1)
    print("ok", key, endpoint, flush=True)
    n += 1
"#;
