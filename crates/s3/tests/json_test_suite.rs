//! The JSON reader against JSONTestSuite's parsing cases (docs/research/17 §1.3), vendored
//! with its MIT license from https://github.com/nst/JSONTestSuite at
//! 1ef36fa01286573e846ac449e8683f8833c5b26a.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros
)]

use std::path::Path;

use mantle_s3::json::{self, JsonError};

fn cases() -> Vec<(String, Vec<u8>)> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/JSONTestSuite/test_parsing");
    let mut cases: Vec<(String, Vec<u8>)> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let name = entry.file_name().into_string().unwrap();
            (name, std::fs::read(entry.path()).unwrap())
        })
        .collect();
    cases.sort();
    cases
}

/// RFC 8259 lets a name repeat, and the suite's two cases of it are `y_`; I-JSON
/// (RFC 7493 §2.3) and IAM's policy grammar forbid it, and the reader refuses them.
const REPEATED: [&str; 2] = [
    "y_object_duplicated_key.json",
    "y_object_duplicated_key_and_value.json",
];

/// How the reader answers a case RFC 8259 leaves to the implementation.
#[derive(Debug, PartialEq)]
enum Chosen {
    /// A number past a double's range or precision, kept as written.
    Kept,
    /// Text that is not UTF-8.
    NotUtf8,
    /// An unpaired or misordered surrogate escape (RFC 7493 §2.1).
    Surrogate,
    /// Nesting past `MAX_DEPTH`.
    Deep,
    /// A byte order mark, which begins no value.
    Mark,
}

const CHOSEN: [(&str, Chosen); 35] = [
    ("i_number_double_huge_neg_exp.json", Chosen::Kept),
    ("i_number_huge_exp.json", Chosen::Kept),
    ("i_number_neg_int_huge_exp.json", Chosen::Kept),
    ("i_number_pos_double_huge_exp.json", Chosen::Kept),
    ("i_number_real_neg_overflow.json", Chosen::Kept),
    ("i_number_real_pos_overflow.json", Chosen::Kept),
    ("i_number_real_underflow.json", Chosen::Kept),
    ("i_number_too_big_neg_int.json", Chosen::Kept),
    ("i_number_too_big_pos_int.json", Chosen::Kept),
    ("i_number_very_big_negative_int.json", Chosen::Kept),
    ("i_object_key_lone_2nd_surrogate.json", Chosen::Surrogate),
    (
        "i_string_1st_surrogate_but_2nd_missing.json",
        Chosen::Surrogate,
    ),
    (
        "i_string_1st_valid_surrogate_2nd_invalid.json",
        Chosen::Surrogate,
    ),
    ("i_string_UTF-16LE_with_BOM.json", Chosen::NotUtf8),
    ("i_string_UTF-8_invalid_sequence.json", Chosen::NotUtf8),
    ("i_string_UTF8_surrogate_U+D800.json", Chosen::NotUtf8),
    (
        "i_string_incomplete_surrogate_and_escape_valid.json",
        Chosen::Surrogate,
    ),
    ("i_string_incomplete_surrogate_pair.json", Chosen::Surrogate),
    (
        "i_string_incomplete_surrogates_escape_valid.json",
        Chosen::Surrogate,
    ),
    ("i_string_invalid_lonely_surrogate.json", Chosen::Surrogate),
    ("i_string_invalid_surrogate.json", Chosen::Surrogate),
    ("i_string_invalid_utf-8.json", Chosen::NotUtf8),
    (
        "i_string_inverted_surrogates_U+1D11E.json",
        Chosen::Surrogate,
    ),
    ("i_string_iso_latin_1.json", Chosen::NotUtf8),
    ("i_string_lone_second_surrogate.json", Chosen::Surrogate),
    ("i_string_lone_utf8_continuation_byte.json", Chosen::NotUtf8),
    ("i_string_not_in_unicode_range.json", Chosen::NotUtf8),
    ("i_string_overlong_sequence_2_bytes.json", Chosen::NotUtf8),
    ("i_string_overlong_sequence_6_bytes.json", Chosen::NotUtf8),
    (
        "i_string_overlong_sequence_6_bytes_null.json",
        Chosen::NotUtf8,
    ),
    ("i_string_truncated-utf-8.json", Chosen::NotUtf8),
    ("i_string_utf16BE_no_BOM.json", Chosen::NotUtf8),
    ("i_string_utf16LE_no_BOM.json", Chosen::NotUtf8),
    ("i_structure_500_nested_arrays.json", Chosen::Deep),
    ("i_structure_UTF-8_BOM_empty_object.json", Chosen::Mark),
];

fn chosen(read: &Result<json::Value, JsonError>) -> Option<Chosen> {
    match read {
        Ok(_) => Some(Chosen::Kept),
        Err(JsonError::Utf8) => Some(Chosen::NotUtf8),
        Err(JsonError::Surrogate) => Some(Chosen::Surrogate),
        Err(JsonError::Depth) => Some(Chosen::Deep),
        Err(JsonError::Syntax(_)) => Some(Chosen::Mark),
        Err(JsonError::Duplicate) => None,
    }
}

/// Every `y_` case is read and every `n_` case refused, but for the repeated names; every
/// `i_` case is answered as chosen.
#[test]
fn every_case_is_read_as_the_suite_expects() {
    let cases = cases();
    assert_eq!(cases.len(), 318);
    let mut wrong = Vec::new();
    for (name, text) in &cases {
        let read = json::parse(text);
        let right = match name.get(..2) {
            _ if REPEATED.contains(&name.as_str()) => read == Err(JsonError::Duplicate),
            Some("y_") => read.is_ok(),
            Some("n_") => read.is_err(),
            _ => CHOSEN
                .iter()
                .any(|(case, outcome)| case == name && chosen(&read).as_ref() == Some(outcome)),
        };
        if !right {
            wrong.push(format!("{name}: {read:?}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}
