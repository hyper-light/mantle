use super::*;

/// Own the fixture even when an assertion unwinds; it never survives its test.
struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// AWS CLI 2.38.5 silently skips a PreconditionFailed upload: exit 0, no stderr, Abort instead
/// of Complete. The harness must not call that an acknowledged object. This uses the real CLI,
/// a real multipart request and the stock profile, with only the HTTP refusal injected.
#[test]
fn a_silently_skipped_cli_upload_is_not_acknowledged() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path();
    let fixture = dir.join("skip.py");
    std::fs::write(&fixture, include_str!("skip.py")).unwrap();
    let counts = dir.join("counts.json");
    let mut server = Server(
        Command::new(PYTHON)
            .arg(&fixture)
            .arg(&counts)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut ready = String::new();
    BufReader::new(server.0.stdout.take().unwrap())
        .read_line(&mut ready)
        .unwrap();
    let port: u16 = ready.trim().parse().unwrap();
    std::fs::create_dir(dir.join("s3")).unwrap();
    std::fs::write(
        dir.join("s3/credentials"),
        "[mantle]\naws_access_key_id = test\naws_secret_access_key = test\n",
    )
    .unwrap();
    let endpoint = format!("127.0.0.1:{port}");
    let mut cli = Cli::new(dir, dir, (&endpoint, "us-east-1")).unwrap();
    let generator = super::super::generate::Generator::new(5).unwrap();
    let object = generator.object("l0").unwrap();
    let result = cli.put(("transfer", "l0"), &object, 9 << 20);
    // The server shuts down on this pipe's EOF: no sleep or guess about its work.
    drop(server.0.stdin.take());
    assert!(server.0.wait().unwrap().success());
    let counts = std::fs::read_to_string(counts).unwrap();
    assert!(counts.contains("\"aborted\": 1"), "{counts}");
    assert!(counts.contains("\"completed\": 0"), "{counts}");
    match result {
        Err(StockError::Said(said)) => assert!(
            said.contains("exited successfully") && said.contains("uploaded object"),
            "{said}"
        ),
        // CLI versions that report the refusal directly already keep it out of the oracle.
        Ok(Came::Refused(_)) => {}
        other => panic!(
            "a CLI skip must fail the benchmark, not enter its acknowledgment oracle: {other:?}"
        ),
    }
}
