//! Upstream rustls's SSLKEYLOGFILE tests (test code: the no-panic wall is off).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::todo,
    clippy::unimplemented,
    clippy::indexing_slicing,
    clippy::string_slice,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::disallowed_macros,
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::cognitive_complexity,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn,
    clippy::missing_panics_doc,
    clippy::dbg_macro,
    unreachable_pub
)]

use std::env;
use std::path::PathBuf;
use std::sync::Mutex;

#[macro_use]
mod macros;

#[path = "."]
mod tests_with_aws_lc_rs {
    use super::{key_log, serialized};

    provider_aws_lc_rs!();

    #[path = "../key_log_file_env.rs"]
    mod tests;
}

/// Where the tests' key log goes: a file in this test's own target directory, under its process
/// id. Upstream wrote `./sslkeylogfile.txt` into the crate's directory and left it there.
fn key_log() -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("sslkeylogfile-{}.txt", std::process::id()))
}

/// The key log, removed when the test lets it go, whatever the test did with it.
struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Approximates `#[serial]` from the `serial_test` crate.
///
/// No attempt is made to recover from a poisoned mutex, which will
/// happen when `f` panics. In other words, all the tests that use
/// `serialized` will start failing after one test panics.
#[allow(dead_code)]
fn serialized(f: impl FnOnce()) {
    // Ensure every test is run serialized
    static MUTEX: Mutex<()> = const { Mutex::new(()) };

    let _guard = MUTEX.lock().unwrap();
    let _scratch = Scratch(key_log());

    // XXX: NOT thread safe.
    env::set_var("SSLKEYLOGFILE", key_log());

    f()
}
