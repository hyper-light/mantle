//! The process's open descriptors as the OS counts them (`hyper_measure::descriptors`), alone in
//! its test binary so no other test opens or closes one meanwhile.

#![allow(
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::arithmetic_side_effects
)]

/// Do: count, open this test's own executable twice, count, close both, count. Expect: two more
/// while open, and as many as before once closed.
#[test]
fn two_files_opened_count_two_and_closed_count_none() {
    let exe = std::env::current_exe().unwrap();
    let before = hyper_measure::descriptors::open().unwrap();
    let one = std::fs::File::open(&exe).unwrap();
    let two = std::fs::File::open(&exe).unwrap();
    let during = hyper_measure::descriptors::open().unwrap();
    drop((one, two));
    let after = hyper_measure::descriptors::open().unwrap();
    assert_eq!((during - before, after), (2, before));
}
