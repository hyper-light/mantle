//! Scratch (diag/roll-stop-bound, never merged): timed lines of the orderly stop on standard
//! error, each `DIAG-STOP <wall ms> node=<id> <text>`, written in one write so the lines of the
//! processes sharing the test's standard error do not interleave.

use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

/// Wall-clock milliseconds since the Unix epoch.
pub fn wall_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis())
}

/// Says `text` for node `node`, stamped with the wall clock.
pub fn say(node: u64, text: &str) {
    let line = format!("DIAG-STOP {} node={node} {text}\n", wall_ms());
    let _ = std::io::stderr().write_all(line.as_bytes());
}
