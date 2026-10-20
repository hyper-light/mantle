//! Runs the BoringSSL bogo suite, ignored by default (test code: the no-panic wall is off).
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

// Runs the bogo test suite, in the form of a rust test.
// Note that bogo requires a golang environment to build
// and run.

#[test]
#[ignore]
fn run_bogo_tests_ring() {
    run_bogo_tests("ring");
}

#[test]
#[ignore]
fn run_bogo_tests_aws_lc_rs() {
    run_bogo_tests("aws-lc-rs");
}

#[test]
#[ignore]
fn run_bogo_tests_aws_lc_rs_fips() {
    run_bogo_tests("aws-lc-rs-fips");
}

fn run_bogo_tests(provider: &str) {
    use std::process::Command;

    let rc = Command::new("./runme")
        .current_dir("../bogo")
        .env("BOGO_SHIM_PROVIDER", provider)
        .spawn()
        .expect("cannot run bogo/runme")
        .wait()
        .expect("cannot wait for bogo");

    assert!(rc.success(), "bogo ({provider}) exited non-zero");
}
