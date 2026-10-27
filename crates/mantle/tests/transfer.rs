//! `mantle bench transfer` run whole (plan step 9): a cell of three `mantle init` makes and
//! `mantle serve` runs, objects put through the native door and each read back through another
//! node, every byte checked against what the run's seed and the object's name make.

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

/// A small set of 20, its sizes the layout's boundaries and then drawn below S3's small-object
/// line, each put and read back whole: the case's row counts all 20 and none refused, wrong or
/// missing, and the run exits well.
#[test]
fn a_small_set_puts_and_reads_back_every_object_exactly() {
    let root = tempfile::tempdir().unwrap();
    let run = root.path().join("run");
    let ran = Command::new(MANTLE)
        .args(["bench", "transfer"])
        .arg(&run)
        .args(["--small", "20", "--seed", "5"])
        .output()
        .unwrap();
    let said = String::from_utf8_lossy(&ran.stdout);
    assert!(
        ran.status.success(),
        "{said}{}",
        String::from_utf8_lossy(&ran.stderr)
    );
    let row: Vec<&str> = said
        .lines()
        .find(|l| l.trim_start().starts_with("small 20"))
        .unwrap_or_else(|| panic!("{said}"))
        .split_whitespace()
        .collect();
    // case (2 words), objects, bytes, four latencies (2 words each), refused, wrong, missing.
    assert_eq!(row[2], "20", "{said}");
    assert_eq!(&row[row.len() - 3..], ["0", "0", "0"], "{said}");
    // A case's directory is its own: the same run again is refused, not mixed into the last.
    let again = Command::new(MANTLE)
        .args(["bench", "transfer"])
        .arg(&run)
        .args(["--small", "20"])
        .output()
        .unwrap();
    assert!(!again.status.success());
}
