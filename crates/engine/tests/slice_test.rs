//! RocksDB's util/slice_test.cc, the grab bag of small utility tests, for the utilities P1
//! converts: `SliceTest.StringView` and `ToBaseCharsStringTest.Tests`.
//!
//! Its other 11 tests belong to later phases or have no Rust counterpart
//! (docs/design/engine.md §6): `PinnableSliceTest` ×3 (the read path, P8), `SmallEnumSetTest`
//! ×3 (`FileTypeSet`, P6), `BitFieldsTest` (HyperClockCache, P9), and `StatusTest.Update`,
//! `UnownedPtrTest.Tests` and `SemaphoreTest` ×2, whose subjects the port replaces with typed
//! errors, references and `std::sync`.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use mantle_engine::util::string_util::to_base_chars_string;

/// A `Slice` is a `&[u8]`: one taken from an owned string and one from a borrowed view of it
/// hold the same bytes.
#[test]
fn slice_string_view() {
    let s = String::from("foo");
    let sv: &str = &s;
    assert_eq!(s.as_bytes(), sv.as_bytes());
    let moved: &str = sv;
    assert_eq!(s.as_bytes(), moved.as_bytes());
}

#[test]
fn to_base_chars_string_tests() {
    // Base 16
    assert_eq!(to_base_chars_string(16, 5, 0, true), "00000");
    assert_eq!(to_base_chars_string(16, 5, 42, true), "0002A");
    assert_eq!(to_base_chars_string(16, 5, 42, false), "0002a");
    assert_eq!(to_base_chars_string(16, 2, 255, false), "ff");
    // Base 32
    assert_eq!(to_base_chars_string(32, 2, 255, false), "7v");
}
