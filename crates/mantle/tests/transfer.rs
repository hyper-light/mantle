//! `mantle bench transfer` run whole (plan step 9): cells of three `mantle init` makes and `mantle
//! serve` runs, objects put through the native door and through the S3 listener by boto3 and the
//! AWS CLI, each read back through both doors and another node, every byte checked against what
//! the run's seed and the object's name make, and every node's chunk bytes accounted. boto3 and the
//! AWS CLI are the host's, as the stock-client tests take them.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::process::Command;

const MANTLE: &str = env!("CARGO_BIN_EXE_mantle");

/// `mantle bench transfer` into `run` with `args`: what it said, which must say it exited well.
fn transfer(run: &std::path::Path, args: &[&str]) -> String {
    let ran = Command::new(MANTLE)
        .args(["bench", "transfer"])
        .arg(run)
        .args(args)
        .output()
        .unwrap();
    let said = String::from_utf8_lossy(&ran.stdout).into_owned();
    assert!(
        ran.status.success(),
        "{said}{}",
        String::from_utf8_lossy(&ran.stderr)
    );
    said
}

/// The words of the row whose case begins `case`.
fn row<'a>(said: &'a str, case: &str) -> Vec<&'a str> {
    said.lines()
        .find(|l| l.trim_start().starts_with(case))
        .unwrap_or_else(|| panic!("no row {case}: {said}"))
        .split_whitespace()
        .collect()
}

/// The line that begins `case`.
fn line<'a>(said: &'a str, case: &str) -> &'a str {
    said.lines()
        .find(|l| l.trim_start().starts_with(case))
        .unwrap_or_else(|| panic!("no line {case}: {said}"))
}

/// The plan's smoke: the 1,000-object small set, its sizes the layout's boundaries and then drawn
/// below S3's small-object line, half put through each door; and a large case of four 20 MiB
/// objects, a few runs and parts each, one put natively in runs, one by the AWS CLI from its
/// standard input, one natively in explicit parts and one by boto3 in parts. Every object reads
/// back whole through both doors, the large ones listed by both; every block's chunks sit on
/// distinct nodes, as the Block range and the volume table say; every node holds exactly the chunk
/// bytes it received over QUIC and put itself, none refused; with each founder SIGKILLed in turn
/// every object reads back through the others, and through it once it is started again; and each
/// control is caught: a byte inverted on its way to the check, a chunk moved onto another's node, a
/// key never put. A case's directory is its own: the same run again is refused, not mixed into the
/// last.
#[test]
fn the_small_set_and_large_objects_in_every_way_read_back_exactly() {
    let root = tempfile::tempdir().unwrap();
    let run = root.path().join("run");
    let said = transfer(
        &run,
        &[
            "--small", "1000", "--large", "20M", "--count", "4", "--seed", "5",
        ],
    );
    for door in ["native", "http"] {
        let row = row(&said, &format!("small 1000 {door}"));
        // case (3 words), objects, bytes, six latencies (2 words each), refused, wrong, missing.
        assert_eq!(row[3], "500", "{said}");
        assert_eq!(&row[row.len() - 3..], ["0", "0", "0"], "{said}");
    }
    for way in ["native", "cli", "native-parts", "boto-parts"] {
        let row = row(&said, &format!("large 20971520 {way} "));
        // case (3 words), objects, bytes, three rates (2 words each), full, refused, wrong,
        // missing.
        assert_eq!(row[3..5], ["1", "20971520"], "{said}");
        assert_eq!(&row[row.len() - 4..], ["0", "0", "0", "0"], "{said}");
    }
    assert!(
        line(&said, "large 20971520 out of space").ends_with(
            "native none, http none; refused objects unreadable and unlisted, acknowledged ones \
             listed: yes"
        ),
        "{said}"
    );
    for case in ["small 1000", "large 20971520"] {
        let chunks = line(&said, &format!("{case} chunks"));
        assert!(chunks.ends_with("put in-process: yes"), "{said}");
        assert_eq!(chunks.matches("(+0 kept by none)").count(), 3, "{said}");
        assert!(
            line(&said, &format!("{case} placement")).ends_with("on distinct nodes: yes"),
            "{said}"
        );
        assert!(
            line(&said, &format!("{case} kills")).ends_with("0 wrong, 0 missing"),
            "{said}"
        );
        // The controls, each a fault the checks must catch: a byte inverted on its way to the
        // check, a chunk moved onto another's node, a key never put.
        let controls = line(&said, &format!("{case} controls"));
        assert_eq!(controls.matches("caught").count(), 3, "{said}");
        assert!(!controls.contains("NOT"), "{said}");
    }
    let again = Command::new(MANTLE)
        .args(["bench", "transfer"])
        .arg(&run)
        .args(["--small", "1000"])
        .output()
        .unwrap();
    assert!(!again.status.success());
}

/// Large objects put in turn through the four ways into a cell whose 96 MiB volumes hold two of
/// them: the next is refused as out of space through the native door and through the S3 listener,
/// typed on each, and nothing of either refused object is readable through either door or listed
/// by either, while every acknowledged object reads back whole and is listed by both, its blocks
/// on distinct nodes, and through the others with each founder killed in turn. Every node holds
/// what it received and put but for what it kept of none, its volume's refusals.
#[test]
fn a_cell_filled_with_large_objects_refuses_the_next_through_both_doors_and_keeps_none_of_it() {
    let root = tempfile::tempdir().unwrap();
    let said = transfer(
        &root.path().join("run"),
        &["--large", "20M", "--volume", "96M", "--seed", "7"],
    );
    let full = line(&said, "large 20971520 out of space");
    assert!(!full.contains("none"), "{said}");
    assert!(full.ends_with("acknowledged ones listed: yes"), "{said}");
    let acknowledged: u64 = ["native", "cli", "native-parts", "boto-parts"]
        .iter()
        .map(|way| {
            let row = row(&said, &format!("large 20971520 {way} "));
            assert_eq!(&row[row.len() - 3..], ["0", "0", "0"], "{said}");
            row[3].parse::<u64>().unwrap()
        })
        .sum();
    assert!(acknowledged > 0, "{said}");
    assert!(
        line(&said, "large 20971520 chunks").ends_with("put in-process: yes"),
        "{said}"
    );
    assert!(
        line(&said, "large 20971520 placement").ends_with("on distinct nodes: yes"),
        "{said}"
    );
    assert!(
        line(&said, "large 20971520 kills").ends_with("0 wrong, 0 missing"),
        "{said}"
    );
}
